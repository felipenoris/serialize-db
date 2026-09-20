# Modelagem da biblioteca serialize-db

A biblioteca mantém um banco analítico como um conjunto de tabelas Delta Lake em pastas, no S3 do
projeto ou em disco local, com os modelos SQLAlchemy como contrato de esquema. Ela leva ao DuckDB ou
ao Redshift só os meses que uma execução do pipeline precisa, publica o resultado de volta no Delta
e carrega no Redshift as tabelas que os clientes consultam. Este documento reúne a modelagem: o que
a biblioteca faz, os metadados que ela mantém, as primitivas de cada módulo e o fluxo de cada caso
de uso. As razões do desenho estão em [`estrategia.md`](estrategia.md); o comportamento verificado
do Delta, em [`delta.md`](delta.md); os motores, em [`duckdb.md`](duckdb.md) e
[`redshift.md`](redshift.md); o contrato, em [`schema.md`](schema.md) e
[`sqlalchemy.md`](sqlalchemy.md); as práticas de ETL que o desenho segue, em [`guia.md`](guia.md); as etapas de implementação e as
primitivas de cada módulo, em [`PLAN.md`](PLAN.md).

## Funcionalidades

- **Contrato de esquema a partir dos modelos.** Os modelos declarativos do SQLAlchemy definem cada
  tabela, e deles a biblioteca deriva o esquema Arrow, o esquema Delta e o DDL do sandbox nos dois
  motores. As opções físicas, partição por `mes`, chave de ordenação e distribuição no Redshift,
  ficam em `Table.info["serialize_db"]`. Os arquivos de esquema gerados são versionados no
  repositório do pipeline e comparados por teste.
- **SQL gerado por dialeto.** Cada statement Core do pipeline vira texto SQL do DuckDB e do
  Redshift, com as constantes embutidas, o mês como parâmetro `:mes` e o prefixo do sandbox como
  sentinela. O texto gerado é versionado no repositório do pipeline e substitui, uma interação por
  vez, a compilação pelo dialeto em tempo de execução ([`sqlalchemy.md`](sqlalchemy.md)).
- **Banco em tabelas Delta.** Uma pasta por ambiente e uma subpasta por tabela. A biblioteca cria
  cada tabela a partir do contrato, de forma idempotente, e reconcilia o esquema da tabela com o
  modelo: o diff aditivo é aplicado, o destrutivo exige a reescrita explícita.
- **Ingestão seletiva.** Cada execução fixa a versão de cada tabela lida e leva ao motor só os meses
  que o pipeline usa: views ou tabelas materializadas no DuckDB, `COPY ... MANIFEST` no Redshift.
- **Execução com sandbox, auditoria e publicação.** O pipeline roda num sandbox por execução. A
  auditoria reprova sem tocar o Delta. A publicação substitui meses inteiros, um commit por tabela,
  com `serialize_db_execution_id` e `serialize_db_input_versions` nos metadados. A reexecução é
  idempotente, e o conflito entre duas execuções do mesmo ambiente aborta a segunda.
- **Publicação para clientes no Redshift.** A diferença entre a versão publicada e a atual diz quais
  meses recarregar. Todas as tabelas da execução entram numa única transação, e a tabela de controle
  guarda a versão publicada de cada uma.
- **Snapshots do banco e manutenção.** O conjunto `{tabela: versão}` marcado na periodicidade do
  processo, o `vacuum` que preserva essas versões, a compactação antes do snapshot e a cópia
  profunda para a pasta de arquivo.
- **Entrada e saída em Parquet.** A carga inicial dos Parquet atuais e a exportação de um snapshot
  para pastas Parquet por mês, para o Hive ou para sair do Delta.
- **Linha de comando e documentação.** `serialize-db run` executa o pipeline, e o `pdoc` gera a
  documentação da API.

Os módulos são `serialize_db.schema`, `serialize_db.sql`, `serialize_db.storage`, `serialize_db.delta`,
`serialize_db.engine.duckdb`, `serialize_db.engine.redshift` e `serialize_db.execution`. Os modelos
do projeto em `src/serialize_db/model/` são a primeira instância do contrato e o material dos
testes. As etapas de implementação, com as primitivas e o critério de aceite de cada uma, estão em
[`PLAN.md`](PLAN.md).

## Metadados próprios da biblioteca

Dois sentidos de snapshot convivem nos documentos. O snapshot da tabela é o estado de uma tabela
numa versão do log, o que `DeltaTable(uri, version=v)` carrega. O snapshot do banco é o conjunto
`{tabela: versão}` de todas as tabelas num instante escolhido, que o Delta não tem e a biblioteca
registra; a chave gravada é `serialize_db_snapshot`.

Os metadados próprios têm nomes em inglês, como os identificadores do código. O que a biblioteca
grava fora da pasta `_serialize_db/` leva o prefixo `serialize_db_`, para não colidir com as chaves
do Delta e de outros escritores nem com as tabelas do banco: as chaves de commit
`serialize_db_execution_id`, `serialize_db_input_versions` e `serialize_db_snapshot`, as chaves do
rodapé Parquet `serialize_db_version` e `serialize_db_execution_id`, e a tabela de controle
`serialize_db_publications` no Redshift, cujas colunas dispensam o prefixo porque o nome da tabela
já é o espaço de nomes. O que vive em `_serialize_db/` dispensa o prefixo, como a chave `snapshots`
de `snapshots.json`. As tabelas e colunas do banco de dados continuam em português.

O log de cada tabela guarda tudo o que é da tabela: os arquivos de cada versão, o esquema de cada
versão com os comentários de coluna, as estatísticas por arquivo e os metadados que a biblioteca
grava em cada commit (`serialize_db_execution_id`, `serialize_db_input_versions` e, quando houver,
`serialize_db_snapshot`). O modelo SQLAlchemy dá o DDL e os tipos do contrato atual, e a
reconciliação descrita em [`delta.md`](delta.md), seção "Evolução de esquema", garante que ele e o
esquema atual do log são o mesmo. A biblioteca não guarda cópia de esquema, lista de arquivos nem
estatísticas.

O que ela guarda por conta própria fica em `_serialize_db/`, na raiz do ambiente, ao lado das pastas
das tabelas: `snapshots.json`, com
`{"snapshots": {"2026T3": {"cad_lancamentos": 143, "cad_contratos": 88}}}`. O sublinhado inicial
deixa a pasta fora dos globs `mes=*` e dos leitores no estilo Hive, que ignoram nomes com esse
prefixo. Os nomes de tabela são relativos, sem URI, para a realocação descrita em
[`delta.md`](delta.md), seção "Realocação e cópia do banco", continuar valendo. A escrita é atômica,
com `IfMatch` no S3, a mesma primitiva do log, e só a biblioteca escreve. O arquivo é a fonte
primária e o log é a reconstrução: a marca `serialize_db_snapshot` vive no `commitInfo`, que não
entra nos checkpoints e some do log quando a limpeza passa de `delta.logRetentionDuration`. O
segundo registro próprio é a tabela `serialize_db_publications` no Redshift, que fica lá por ser
transacional com a carga.

Exportar as tabelas de um snapshot usa cada fonte no seu papel. O snapshot atual dispensa metadado
próprio: `get_add_actions()` lista os arquivos, e o DDL sai do modelo. Um snapshot do banco antigo
lê a versão de cada tabela em `snapshots.json`, lista os arquivos com
`DeltaTable(uri, version=v).get_add_actions()` e tira o DDL do esquema daquela versão,
`DeltaTable(uri, version=v).schema()`, não do modelo de hoje, que pode ter ganhado colunas depois.

| Registro | Onde | Conteúdo | Quem grava |
| --- | --- | --- | --- |
| Metadados de commit | `commitInfo` de cada commit da biblioteca. | `serialize_db_execution_id`; `serialize_db_input_versions`, o JSON `{tabela: versão}` das versões lidas, fixado na abertura da execução; `serialize_db_snapshot` só na execução que marca um snapshot. | `publish_month` e `register_files`, por `CommitProperties(custom_metadata=...)`. |
| Arquivo de controle | `<ambiente>/_serialize_db/snapshots.json`. | `{"snapshots": {nome: {tabela: versão}}}`, com todas as tabelas do ambiente, lidas ou gravadas. | `snapshot`, com `IfMatch`. |
| Tabela de controle | `serialize_db_publications(table_name, delta_version, execution_id, published_at)` no esquema do Redshift; `table_name` leva o prefixo do ambiente, como `prod_cad_lancamentos`. | Versão do Delta carregada em cada tabela publicada. | `publish_redshift`, na transação da carga. |

O registro durável de uma execução é o `commitInfo` das tabelas que ela gravou; o relatório da
auditoria e o resumo da execução vão para o log do processo, não para `_serialize_db/`.

## Primitivas

As primitivas de cada módulo, com assinatura, comportamento e testes, estão em [`PLAN.md`](PLAN.md),
etapa a etapa; os fluxos abaixo as citam pelo nome. `table` é sempre um `Table` do SQLAlchemy,
obtido do modelo; `uri` é a pasta da tabela Delta; `reader` é um `RecordBatchReader` do Arrow; `run`
é a `Execution` aberta, e `run.sandbox` o motor onde o pipeline roda.

## Fluxos de uso

### Carga inicial dos Parquet atuais

Uma passagem por tabela e por mês, reexecutável, que termina com os leitores apontados para o Delta.

1. `create_table(uri, table)` para cada modelo, na pasta do ambiente.
2. Para cada mês, o DuckDB ou o PyArrow lê os Parquet do mês, `cast` converte para o contrato (os
   modelos atuais usam `Double` onde o contrato pede `Numeric(18, 2)`) e `publish_month` grava o
   mês em lotes, sem a tabela inteira na memória. Uma carga interrompida recomeça do mês seguinte
   ao último publicado.
3. O relatório compara contagens e somas por mês entre a origem e o Delta; a carga só termina
   quando os dois coincidem.
4. Os leitores passam a abrir o Delta, e as pastas de origem ficam como cópia até a primeira
   publicação no Redshift.

`convert_to_deltalake` registra os arquivos no lugar, sem reescrever, só quando eles já têm os
tipos, a ordem de colunas e o layout Hive do contrato; não foi testado.

### Execução mensal no DuckDB

O exemplo ilustrado, com versões e artefatos de cada passo, está em [`PLAN.md`](PLAN.md).

1. `Execution` abre cada tabela de entrada e fixa `versions`; toda leitura da execução usa essas
   versões, mesmo que outra execução publique no meio.
2. `run.ingest` cria as views com os nomes dos modelos sobre `delta_scan` na versão fixada, e
   materializa as tabelas consultadas muitas vezes com os meses pedidos.
3. O pipeline roda em `run.sandbox`; os intermediários ficam no sandbox, não no Delta.
4. `run.audit` reprova e encerra sem tocar o Delta, ou aprova.
5. `run.publish` reconcilia o esquema, substitui cada mês num commit com
   `serialize_db_execution_id` e `serialize_db_input_versions`, e avança `versions[table]`. Um
   `CommitFailedError` no mesmo mês significa outra execução publicando a mesma tabela, e a
   execução aborta.
6. `run.publish_redshift` carrega os meses alterados de todas as tabelas numa transação.
7. No encerramento, o sandbox é descartado e o resumo vai para o log. Repetir a execução com o
   mesmo `execution_id` repete os mesmos `overwrite` e produz o mesmo snapshot.

### Execução no Redshift

O mesmo ciclo, com o motor Redshift; o que muda é onde os dados ficam.

1. O sandbox são tabelas `exec_<id>_<tabela>` no esquema único, criadas pelo DDL do contrato.
2. `run.ingest` monta o manifesto dos arquivos dos meses pedidos, na versão fixada, e carrega por
   `COPY ... MANIFEST` na staging sem `mes`, seguido de `INSERT ... SELECT *, '<mes>'`. A carga de
   arquivos anteriores a uma coluna nova depende de `FILLRECORD` ou de lista de colunas, pendente da
   prova de conceito.
3. O pipeline roda os mesmos statements Core, compilados para o Redshift; DataFrames entram por
   Parquet em `staging/` mais `COPY`, e saem por ADBC ou `UNLOAD`.
4. `run.audit` roda as mesmas consultas no Redshift.
5. `run.publish` grava cada mês por `UNLOAD ... PARTITION BY (mes) MANIFEST VERBOSE` na pasta da
   tabela e registra os arquivos por `register_files`, com estatísticas do rodapé Parquet; os dados
   não passam pela máquina local.
6. `run.publish_redshift` carrega as tabelas `prod_*` a partir do Delta, pelo mesmo caminho da
   execução no DuckDB, e `cleanup` apaga as tabelas do sandbox e o staging.

### Publicação para clientes no Redshift

As tabelas publicadas têm o prefixo do ambiente e são derivadas do Delta; nada é escrito nelas por
outro caminho.

1. `version_diff` compara, para cada tabela, a versão em `serialize_db_publications` com a versão
   atual e devolve os meses com arquivos novos. Na primeira publicação, todos os meses.
2. A reconciliação repete no Redshift o diff aditivo do Delta, `ALTER TABLE ADD COLUMN` no fim da
   tabela, porque o `COPY` é posicional; um diff destrutivo recria a tabela e recarrega tudo.
3. Numa única transação, para cada tabela e mês: `DELETE` do mês, `COPY ... MANIFEST` na staging,
   `INSERT ... SELECT *, '<mes>'` e a linha de `serialize_db_publications`. A transação dá aos
   clientes a atomicidade entre tabelas que o Delta não tem.
4. Uma execução de correção publica só o mês corrigido.

### Correção de um mês

1. A mesma `Execution`, com o mês a corrigir e um `execution_id` novo.
2. `run.publish` substitui o mês nas tabelas afetadas; a versão anterior continua legível até o
   `vacuum`, dentro dos 400 dias de retenção.
3. `run.publish_redshift` recarrega só esse mês.
4. As versões intermediárias entre snapshots do banco saem no `vacuum` mensal.

### Evolução do esquema

1. O modelo muda no repositório do pipeline; `write_schema_files` regenera `schema/<tabela>.*`, e o
   diff aparece no PR.
2. Na execução seguinte, `reconcile` aplica o diff aditivo: coluna anulável nova (as linhas
   antigas leem nulo; um valor para elas é um `update` com predicado), `NOT NULL` relaxado, `CHECK`.
3. Coluna `NOT NULL` nova em tabela com dados é recusada; a alternativa é a coluna anulável seguida
   do `update`.
4. Renomear, remover ou mudar o tipo de uma coluna é recusado pela reconciliação, e só entra por
   `rewrite`, uma ordem explícita fora da execução mensal: a tabela inteira num commit, sem
   predicado, porque o `overwrite` de um mês com `schema_mode="overwrite"` troca o esquema da tabela
   toda e deixa os outros meses lendo nulo ([`delta.md`](delta.md), seção "Evolução de esquema").
   A versão anterior continua lendo com o esquema antigo.
5. As tabelas publicadas seguem o mesmo diff: `ADD COLUMN` no caso aditivo, recriação e recarga no
   destrutivo.

### Snapshot do banco e manutenção

1. A execução marcada, por exemplo a do fim do trimestre, chama `run.snapshot("2026T3")`: cada
   commit leva `serialize_db_snapshot`, e no encerramento `snapshot` grava em
   `_serialize_db/snapshots.json` a versão de todas as tabelas do ambiente, as gravadas na versão
   nova e as só lidas na versão fixada.
2. `compact` roda antes do snapshot, nunca depois, porque a compactação reescreve arquivos que o
   snapshot continua referenciando.
3. Mensalmente, `vacuum_keeping_snapshots` lista com `keep_versions` lido do arquivo de controle,
   o resultado é revisado e depois aplicado; `full=True` de tempos em tempos remove os órfãos de
   escritas interrompidas.
4. Ler um snapshot é abrir cada tabela na versão registrada: `DeltaTable(uri, version=v)`,
   `delta_scan(uri, version := v)` ou um manifesto de `COPY` daquela versão. `restore` torna a
   versão do snapshot o estado atual, num commit novo.
5. Anualmente, `deep_copy` leva os snapshots mais velhos que o prazo da tabela viva para
   `arquivo/<nome>/<tabela>/`, a entrada sai de `snapshots.json`, e a pasta de arquivo recebe a
   regra de ciclo de vida para a classe de armazenamento mais barata.

### Exportação para pastas Parquet por mês

Para publicar no Hive ou para sair do Delta.

1. O snapshot atual usa `export_snapshot(uri, destination)`: `mode="copy"` copia os arquivos que
   `get_add_actions()` lista, já no layout `mes=.../part-....parquet`, sem ler dados; no S3, um
   `CopyObject` por arquivo. Serve quando os leitores casam colunas por nome ou quando nenhum
   `ADD COLUMN` aconteceu desde a última reescrita de todos os meses.
2. `mode="rewrite"` normaliza: o DuckDB lê por `delta_scan` e grava um `COPY` particionado, com
   todos os arquivos no esquema atual e a coluna de partição fora deles.
3. Um snapshot do banco antigo exporta cada tabela na versão de `snapshots.json`, com o DDL tirado
   de `DeltaTable(uri, version=v).schema()`, não do modelo de hoje.
4. O Hive precisa da tabela declarada com todas as colunas e de `MSCK REPAIR TABLE` ou
   `ALTER TABLE ... ADD PARTITION` para enxergar as pastas.

### Cópia do banco e ambiente de desenvolvimento

1. A pasta do ambiente é copiada inteira, com `_delta_log/` de cada tabela e `_serialize_db/`, por
   `aws s3 sync` entre prefixos ou entre disco e S3; os caminhos do log e do arquivo de controle são
   relativos, e a cópia abre onde estiver, na mesma versão.
2. Um ambiente de desenvolvimento nasce de uma cópia de produção: `Database(root, environment="dev")`
   aponta para a pasta copiada, e as tabelas publicadas levam o prefixo `dev_`.
3. Execuções de ambientes diferentes não conflitam, porque gravam tabelas diferentes; a concorrência
   que resta é entre execuções do mesmo ambiente, que o log serializa.

### Substituição do dialeto em tempo de execução

1. O pipeline escolhe uma interação com o banco: um statement Core que hoje é compilado pelo
   dialeto a cada execução, com o mês como `param("mes")`.
2. `write_sql_files({"total_por_cliente": statement}, metadata, "sql/")` grava
   `sql/total_por_cliente.duckdb.sql` e `.redshift.sql`, com as constantes embutidas, `:mes` e o
   sentinela `{prefix}`; os arquivos entram no repositório do pipeline e no diff da revisão.
3. A chamada troca `run.sandbox.query(statement)` por `run.sandbox.execute(sql, {"mes": run.month})`,
   com o texto lido do arquivo; o motor substitui o prefixo e adapta os parâmetros.
4. Enquanto o statement Core existir, o teste que regenera os arquivos e os compara com os
   versionados acusa uma mudança de modelo. Quando o statement sair, o texto é a fonte, mantido à
   mão e validado por `qualify` do SQLGlot contra o contrato.
5. Com toda interação em texto, o pipeline importa o SQLAlchemy só para os modelos, e
   `duckdb_engine` e `sqlalchemy-redshift` saem das dependências de execução; consulta nova nasce
   em texto, no dialeto do DuckDB, com os testes nos dois motores ([`estrategia.md`](estrategia.md)).
