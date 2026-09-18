# Redshift

## Comandos utilitários para diagnóstico

| Comando | O que mostra |
| --- | --- |
| `SELECT current_user;` | Usuário do banco da sessão. |
| `SELECT user_name, superuser, createdb FROM svv_user_info WHERE user_name = current_user;` | Se o usuário da sessão é superusuário e se pode criar bancos. |
| `SELECT database_name, database_type, database_isolation_level FROM svv_redshift_databases;` | Bancos acessíveis, locais ou compartilhados, e o nível de isolamento de cada um. |
| `SELECT database_name, schema_name, schema_type FROM svv_all_schemas;` | Esquemas locais, externos e compartilhados; usuários comuns só veem os próprios dados. |
| `SHOW GRANTS FOR <usuario> FROM DATABASE <banco>;` | Permissões do usuário no banco compartilhado. |
| `SELECT default_iam_role();` | Papel IAM padrão, usado por `IAM_ROLE default` no `COPY` e no `UNLOAD`. |
| `SHOW data_catalog_auto_mount;` | Se o `awsdatacatalog` está montado no workgroup. |

## Carga de DataFrames com COPY

A documentação do Redshift recomenda `COPY` para cargas e `INSERT` com várias linhas apenas quando
`COPY` não é possível. O `INSERT` grande da biblioteca atual vira este fluxo:

1. Converter o DataFrame numa tabela Arrow com o esquema do modelo (cast seguro).
2. Gravar Parquet em `<caminho S3 do projeto>/staging/<id_execucao>/<tabela>/`, fora dos locais das
   tabelas Iceberg.
3. `COPY execucao_<id>.<tabela> FROM '<manifesto>' IAM_ROLE '<arn>' FORMAT AS PARQUET MANIFEST`, ou com
   `CREDENTIALS` enquanto o namespace não tiver papel associado.
4. Comparar `pg_last_copy_count()` com o número de linhas enviadas.

A biblioteca apaga o staging da execução ao terminar, e a rotina de limpeza remove o staging de execuções
que falharam. Uma regra de ciclo de vida no bucket do domínio dependeria do administrador.

Implementações existentes servem de referência ou dependência:

- `awswrangler.redshift.copy` (versão 3.17.1, 2026-08-03) executa o mesmo fluxo.
- O driver ADBC para Redshift (versão 1.7.0, 2026-09-09) faz ingestão em bloco e leitura em Arrow. Ele
  merece um benchmark contra o fluxo acima.

## Regras do COPY para Parquet

| Regra da documentação | Consequência para a biblioteca |
| --- | --- |
| Colunas são associadas por posição, e a quantidade precisa coincidir com a tabela. | A ordem das colunas no Parquet é a ordem do modelo. Os dois derivam do mesmo `Table`. |
| Só existem as colunas gravadas no arquivo. | Colunas de partição ficam dentro do arquivo. |
| Parâmetros aceitos: `ACCEPTINVCHARS`, `FILLRECORD`, `FROM`, `IAM_ROLE`, `CREDENTIALS`, `STATUPDATE`, `MANIFEST`, `EXPLICIT_IDS`. `MAXERROR` não é aceito. | O primeiro erro aborta o `COPY`. A validação acontece antes, no Arrow. |
| `MANIFEST` é aceito. | O `COPY` carrega exatamente os arquivos gravados pela biblioteca. |
| O bucket precisa estar na mesma região do Redshift. | Configuração da infraestrutura. |
| O `COPY` de Parquet usa URLs pré-assinadas válidas por 1 hora. | Políticas IAM do bucket não podem bloquear URLs pré-assinadas. |

## Exportação com UNLOAD

Comportamento do `UNLOAD ... FORMAT AS PARQUET` segundo a documentação:

- O `UNLOAD` grava Parquet 1.0. Cada row group é comprimido com SNAPPY. O row group padrão tem 32 MB;
  `ROWGROUPSIZE` aceita de 32 MB a 128 MB em alguns tipos de nó.
- `MAXFILESIZE` aceita de 5 MB a 6,2 GB (padrão 6,2 GB) e é arredondado para baixo até um múltiplo de
  32 MB.
- Com `PARTITION BY`, as colunas de partição saem dos arquivos, exceto com `INCLUDE`.
- `CLEANPATH` apaga de forma permanente os arquivos das pastas de partição que recebem dados novos.
- Colunas `TIMESTAMPTZ` perdem a informação de fuso horário.
- O `SELECT` externo não aceita `LIMIT`.
- `MANIFEST VERBOSE` lista os arquivos, os nomes e tipos das colunas e as linhas por arquivo.
- Colunas `VARBYTE`, `GEOMETRY` e `HLLSKETCH` só saem em texto ou CSV.

Sugestão: um `UNLOAD` por mês, sem `PARTITION BY`, com destino
`<local da tabela>/data/<mes>/<id_execucao>_`, `MANIFEST VERBOSE` e `MAXFILESIZE` igual ao tamanho alvo
da tabela. O `SELECT` lista as colunas na ordem do modelo, com casts para os tipos do contrato e
`ORDER BY` pela chave de ordenação. A biblioteca confere o manifesto do `UNLOAD` e os rodapés dos
arquivos antes da publicação. `CLEANPATH` não é usado: arquivos de execuções abortadas saem pela remoção
de órfãos, do Glue ou da biblioteca.

A documentação do `UNLOAD` não informa os tipos físicos Parquet de `TIMESTAMP` e `DECIMAL`, a
obrigatoriedade das colunas nem a presença de estatísticas de mínimo e máximo. Os três afetam o
`add_files`, e a prova de conceito verifica.

O `UNLOAD` lê tabelas do sandbox no banco local, fora das regras de escrita por datashare. Sem acesso do
Redshift ao S3, a exportação lê o mês em Arrow pelo driver ADBC e grava o Parquet com o pyarrow e o
papel do projeto.