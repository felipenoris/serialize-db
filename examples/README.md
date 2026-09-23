# `examples/`: conectividade com o Redshift do ambiente alvo

Scripts que rodaram com sucesso no ambiente alvo, guardados como foram executados. Eles são a
referência da conectividade com o Redshift: o probe, a suíte de testes e a
[etapa 5](../plan/PLAN-STAGE-5.md) repetem as chamadas que estão aqui, e não uma variante que
ninguém executou. O que eles mostraram está em [`../plan/POC.md`](../plan/POC.md), com a data.

| Script | Caminho | Chamadas |
| --- | --- | --- |
| [`redshift_native.py`](redshift_native.py) | Protocolo nativo na porta 5439, com credencial temporária derivada da identidade IAM. É o caminho da biblioteca. | `redshift-serverless:GetWorkgroup`, `GetCredentials`; `redshift_connector.connect` |
| [`redshift_data_api.py`](redshift_data_api.py) | Data API por HTTPS, assíncrona: dispara, consulta o estado e pagina o resultado. Serve a comandos e a diagnóstico. | `redshift-data:ExecuteStatement`, `DescribeStatement`, `GetStatementResult` |
| [`redshift_copy_unload.py`](redshift_copy_unload.py) | `USE` no banco do datashare, `CREATE TABLE`, `COPY` de uma pasta Parquet e `UNLOAD`, com as credenciais de quem chama no texto do comando. | as de `redshift_native.py`, mais `s3:ListBucket`, `GetObject` e `PutObject` pela identidade da sessão |
| [`redshift_manifest.py`](redshift_manifest.py) | Os dois comandos com manifesto, pré-requisitos do `export_partition` da [etapa 5](../plan/PLAN-STAGE-5.md) e do `COPY` da publicação da [etapa 8](../plan/PLAN-STAGE-8.md): converte uma partição de `cad_contratos` de Parquet para Delta, monta o manifesto do `COPY` das ações `add`, carrega uma staging por `COPY ... FORMAT AS PARQUET MANIFEST`, acrescenta a coluna de partição por `INSERT`, grava de volta por `UNLOAD ... PARTITION BY (<coluna>) MANIFEST VERBOSE` e registra os arquivos no log do Delta por `create_write_transaction`. Os dois comandos com manifesto são aceitos numa tabela do datashare. | as de `redshift_copy_unload.py`, mais `s3:DeleteObject` sob a pasta de trabalho |

`cad_contas`, a tabela dos outros exemplos, não serve a esse script: ela é uma das dez dimensões sem
partição da base, e o `UNLOAD ... PARTITION BY` precisa de uma coluna de partição. As particionadas
são `cad_contratos`, `cad_operacoes` e `rel_contrato_operacao`, por `data_str`, e `cad_lancamentos`,
por `data_base_str` ([`../plan/POC.md`](../plan/POC.md)).

Os valores literais (região `sa-east-1`, workgroup `controladoria-wg`, banco `dev`, esquema
`sbx_aco_decon` no banco `datalake_rw_shared`) são os do ambiente alvo. O caminho do bucket em
`redshift_copy_unload.py` e `redshift_manifest.py` leva os marcadores `<conta>`, `dzd-<domínio>`
e `<projeto>` no lugar dos identificadores do ambiente, mascarados como em
[`../plan/readings/README.md`](../plan/readings/README.md) (decisão do usuário de 2026-09-23);
para rodar esses dois, o caminho volta aos valores do ambiente. O probe e a suíte tomam os
mesmos parâmetros das variáveis `SERIALIZE_DB_REDSHIFT_*`, descritas em
[`../probes/README.md`](../probes/README.md).

Para rodar, no ambiente alvo, com as credenciais do espaço já no ambiente:

```
.venv/bin/python examples/redshift_native.py
.venv/bin/python examples/redshift_data_api.py
.venv/bin/python examples/redshift_manifest.py
```

`redshift_manifest.py` precisa de `deltalake` e `pyarrow` além de `boto3` e `redshift_connector`,
por isso roda com o interpretador da pasta preparada; ele cria duas tabelas no esquema, apagadas no
fim, e deixa os objetos da execução sob um prefixo próprio no S3, que ele imprime com o comando que
os apaga.

## O que os exemplos fixam

- **O Redshift é serverless**, um workgroup, e o endereço vem de `get_workgroup`. A autenticação é
  por credencial temporária do próprio workgroup (`get_credentials`), não por senha guardada: o
  usuário sai como `IAMR:<papel>`, a senha dura no máximo uma hora e o par é obtido a cada conexão.
- **O esquema do projeto está num banco de datashare.** Uma sessão no banco local `dev` cita a
  tabela por nome em três partes `datalake_rw_shared.sbx_aco_decon.<tabela>`; `USE <banco>` troca o
  banco da sessão, e a partir dele `esquema.tabela` basta, que é como o `CREATE TABLE`, o `COPY` e o
  `UNLOAD` passaram. O que o Redshift permite escrever num datashare, e o que ele recusa, está em
  [`../plan/redshift.md`](../plan/redshift.md).
- **O `COPY` e o `UNLOAD` alcançam o S3 pelas credenciais de quem chama**, por `ACCESS_KEY_ID`,
  `SECRET_ACCESS_KEY` e `SESSION_TOKEN`, e não por `IAM_ROLE`: o namespace não tem papel associado,
  e sem papel associado nem um ARN explícito funcionaria. O texto do comando carrega segredo e nunca
  vai para log, relatório ou arquivo.
- **A Data API devolve todo valor como texto ou número JSON**, uma célula por dicionário de um item,
  e `DECIMAL`, data e hora chegam como texto. Ela não serve à troca de `pa.Table` da biblioteca,
  que vai por S3 e pelo protocolo nativo; serve a executar um comando e a provar que existe caminho
  por HTTPS quando a porta 5439 não abre.

## Acrescentar um exemplo

O script fica como foi executado, com os valores literais que funcionaram, e ganha um docstring em
português dizendo o que ele prova, quando rodou e onde o resultado está registrado. Um exemplo que
contradiz um documento dispara a revisão desse documento na mesma unidade de trabalho.

Um script ainda não executado entra só como próximo experimento, dito no docstring e numa seção
própria fora da tabela, e com uma pergunta em aberto que ele fecha; sem isso, ele não pertence a
esta pasta, porque o probe e a suíte repetem daqui o que foi executado, não o que foi imaginado.
Depois de rodar, ele passa para a tabela com o que mostrou.
