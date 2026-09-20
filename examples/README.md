# `examples/`: conectividade com o Redshift do ambiente alvo

Scripts que rodaram com sucesso no ambiente alvo, guardados como foram executados. Eles são a
referência da conectividade com o Redshift: o probe, a suíte de testes e a
[etapa 5](../docs/PLAN-STAGE-5.md) repetem as chamadas que estão aqui, e não uma variante que
ninguém executou. O que eles mostraram está em [`../docs/POC.md`](../docs/POC.md), com a data.

| Script | Caminho | Chamadas |
| --- | --- | --- |
| [`redshift_native.py`](redshift_native.py) | Protocolo nativo na porta 5439, com credencial temporária derivada da identidade IAM. É o caminho da biblioteca. | `redshift-serverless:GetWorkgroup`, `GetCredentials`; `redshift_connector.connect` |
| [`redshift_data_api.py`](redshift_data_api.py) | Data API por HTTPS, assíncrona: dispara, consulta o estado e pagina o resultado. Serve a comandos e a diagnóstico. | `redshift-data:ExecuteStatement`, `DescribeStatement`, `GetStatementResult` |
| [`redshift_copy_unload.py`](redshift_copy_unload.py) | `USE` no banco do datashare, `CREATE TABLE`, `COPY` de uma pasta Parquet e `UNLOAD`, com as credenciais de quem chama no texto do comando. | as de `redshift_native.py`, mais `s3:ListBucket`, `GetObject` e `PutObject` pela identidade da sessão |

Os valores literais (região `sa-east-1`, workgroup `controladoria-wg`, banco `dev`, esquema
`sbx_aco_decon` no banco `datalake_rw_shared`) são os do ambiente alvo. O probe e a suíte tomam os
mesmos parâmetros das variáveis `SERIALIZE_DB_REDSHIFT_*`, descritas em
[`../probes/README.md`](../probes/README.md).

Para rodar, no ambiente alvo, com as credenciais do espaço já no ambiente:

```
.venv/bin/python examples/redshift_native.py
.venv/bin/python examples/redshift_data_api.py
```

## O que os exemplos fixam

- **O Redshift é serverless**, um workgroup, e o endereço vem de `get_workgroup`. A autenticação é
  por credencial temporária do próprio workgroup (`get_credentials`), não por senha guardada: o
  usuário sai como `IAMR:<papel>`, a senha dura no máximo uma hora e o par é obtido a cada conexão.
- **O esquema do projeto está num banco de datashare.** Uma sessão no banco local `dev` cita a
  tabela por nome em três partes `datalake_rw_shared.sbx_aco_decon.<tabela>`; `USE <banco>` troca o
  banco da sessão, e a partir dele `esquema.tabela` basta, que é como o `CREATE TABLE`, o `COPY` e o
  `UNLOAD` passaram. O que o Redshift permite escrever num datashare, e o que ele recusa, está em
  [`../docs/redshift.md`](../docs/redshift.md).
- **O `COPY` e o `UNLOAD` alcançam o S3 pelas credenciais de quem chama**, por `ACCESS_KEY_ID`,
  `SECRET_ACCESS_KEY` e `SESSION_TOKEN`, e não por `IAM_ROLE`: o namespace não tem papel associado,
  e sem papel associado nem um ARN explícito funcionaria. O texto do comando carrega segredo e nunca
  vai para log, relatório ou arquivo.
- **A Data API devolve todo valor como texto ou número JSON**, uma célula por dicionário de um item,
  e `DECIMAL`, data e hora chegam como texto. Ela não serve à troca de `pa.Table` da biblioteca,
  que vai por S3 e pelo protocolo nativo; serve a executar um comando e a provar que existe caminho
  por HTTPS quando a porta 5439 não abre.

## Acrescentar um exemplo

Só entra aqui o que rodou no ambiente alvo. O script fica como foi executado, com os valores
literais que funcionaram, e ganha um docstring em português dizendo o que ele prova, quando rodou e
onde o resultado está registrado. Um exemplo que contradiz um documento dispara a revisão desse
documento na mesma unidade de trabalho.
