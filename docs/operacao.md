## Operação

O runbook das rotinas de operação do banco Delta, cada uma um subcomando de `serialize-db` sobre
as primitivas de `serialize_db.delta`, com o que conferir antes e o que esperar depois. Todos os
subcomandos recebem `--metadata modulo:atributo`, `--root` (`SERIALIZE_DB_ROOT`) e `--environment`
(`SERIALIZE_DB_ENVIRONMENT`, `dev`), e saem com 0 quando terminam e com 2 no erro de uso, no nome
repetido ou ausente e no conflito de escrita do arquivo de controle. `compact`, `archive` e
`export` imprimem por tabela o tempo e o pico de memória residente do processo (`VmHWM`), a
medida da rotina na tabela com que a máquina é dimensionada; a publicação a põe na linha de log
de cada tabela.

### Tabela de controle da publicação

Uma vez no esquema do Redshift, antes da primeira publicação de qualquer ambiente:

```shell
serialize-db publish --init
```

Antes: as variáveis `SERIALIZE_DB_REDSHIFT_*` da conexão. Depois: a tabela
`serialize_db_publications` no esquema; sem ela, `serialize-db publish` e
`Execution.publish_redshift` recusam publicar com `serialize_db.errors.PublicationError`.

### Snapshot do banco

Na periodicidade do processo, por exemplo o fim do trimestre, dentro da execução marcada
(`run.snapshot("2026T3")`, gravado no encerramento sem erro) ou fora dela, com a versão atual de
cada tabela do ambiente:

```shell
serialize-db snapshot --root s3://bucket/projeto/delta --environment prod \
    --metadata pipeline.models:Base.metadata --name 2026T3
```

Antes: nenhuma execução aberta no ambiente, porque a entrada leva a versão atual de cada tabela,
e um nome inédito em `snapshots` e em `archived` do arquivo de controle
`<ambiente>/_serialize_db/snapshots.json`. Depois: a entrada `{nome: {tabela: versão}}` gravada na
escrita condicional; outro escritor entre a leitura e a escrita dá `serialize_db.errors.ConflictError`,
e o comando se repete. As versões marcadas ficam legíveis qualquer que seja a retenção do `vacuum`.

### Compactação

Antes de um snapshot, nunca depois: a compactação depois do snapshot dobra os arquivos que ele
referencia. Ela junta os arquivos pequenos das partições pedidas e normaliza os que o `UNLOAD`
gravou (`INT64` no lugar de `INT96` e de `FIXED_LEN_BYTE_ARRAY`, estatística em toda coluna):

```shell
serialize-db compact --root s3://bucket/projeto/delta --environment prod \
    --metadata pipeline.models:Base.metadata --table cad_lancamentos_projetados \
    --partitions 2026-07-31 2026-08-31
```

Antes: o arquivo de controle sem snapshot na versão atual da tabela (o comando confere e recusa);
a memória da máquina, porque a reescrita roda no escritor do delta-rs, fora do `memory_limit` do
DuckDB, e a memória dela numa partição de `cad_lancamentos` não foi medida. Depois: `numFilesAdded`
e `numFilesRemoved` impressos com o tempo e o pico de RSS do processo, a medida da memória da
compactação; um commit `OPTIMIZE` com `dataChange` falso, que `serialize_db.delta.version_diff`
não conta; uma partição com um só arquivo não commita.

### `vacuum`

Mensal: a lista dos arquivos fora da retenção e fora das versões dos snapshots, revisada, depois
aplicada; `--full` de tempos em tempos para os órfãos das escritas interrompidas e dos registros
recusados:

```shell
serialize-db vacuum --root s3://bucket/projeto/delta --environment prod \
    --metadata pipeline.models:Base.metadata
serialize-db vacuum --root s3://bucket/projeto/delta --environment prod \
    --metadata pipeline.models:Base.metadata --apply
serialize-db vacuum --root s3://bucket/projeto/delta --environment prod \
    --metadata pipeline.models:Base.metadata --apply --full
```

Antes: o arquivo de controle legível, e a lista sem `--apply` lida: dentro da retenção de 400 dias
(`--retention-hours 9600`, a janela em que toda versão continua legível) nada é listado, e a lista
aparece quando um arquivo removido passa dela. Depois: com `--apply`, os arquivos listados apagados,
a versão de cada snapshot ainda legível e as intermediárias fora da retenção não; num bucket
versionado o espaço só é liberado pela regra `NoncurrentVersionExpiration`, e `probes/bucket.py`
(`BK-14`) mostra o acumulado. A seção "Retenção dos arquivos removidos" da página principal diz
como mudar a retenção.

### Arquivo

Anual: os snapshots mais velhos que o prazo da tabela viva vão para `arquivo/<nome>/<tabela>/`
do ambiente, pela cópia dos arquivos de cada partição e o registro deles, sem os dados passarem
pela máquina, e a entrada passa de `snapshots` para `archived` no mesmo arquivo de controle:

```shell
serialize-db archive --root s3://bucket/projeto/delta --environment prod \
    --metadata pipeline.models:Base.metadata --name 2026T3
```

Antes: o snapshot registrado em `snapshots`; a pasta `arquivo/<nome>/` recebe a regra de ciclo de
vida do bucket. Depois: uma tabela nova por tabela do snapshot, com uma versão por partição, os
mesmos arquivos e as mesmas somas, cada tabela impressa com o tempo da cópia e o pico de RSS do
processo, e o tempo de cada partição no log; a entrada em `archived`, que `vacuum` não prende
mais, e `snapshot` recusando o nome, porque ele dá a pasta. O comando se repete depois de uma
interrupção
e continua de onde parou: a tabela já inteira no arquivo e a partição já registrada nele são
puladas, e só o que falta é copiado.

### Exportação

Sob demanda: as pastas Parquet `<coluna>=<valor>/` de uma versão da tabela, sem o log, sob a raiz:

```shell
serialize-db export --root s3://bucket/projeto/delta --environment prod \
    --metadata pipeline.models:Base.metadata --table cad_lancamentos_projetados \
    --destination s3://bucket/projeto/delta/prod/exportacao/2026T3/cad_lancamentos_projetados \
    --version 143 --mode copy
```

Antes: o destino vazio e sob a raiz. Depois: em `copy`, os mesmos bytes dos arquivos que o log da
versão lista; em `rewrite`, um arquivo por partição pelo `COPY` do DuckDB, com o esquema da versão em
todos; o tempo e o pico de RSS do processo impressos; `--version` ausente é a versão atual.

### Auditoria avulsa

Depois de uma correção, e quando o SQL de uma verificação precisa ser lido:

```shell
serialize-db audit --root s3://bucket/projeto/delta --environment prod \
    --metadata pipeline.models:Base.metadata --table cad_lancamentos --foreign-keys
serialize-db audit --metadata pipeline.models:Base.metadata --table cad_lancamentos --sql
```

Depois: o veredito de cada verificação, com as amostras; 1 quando alguma reprova.

### Monitoração

O histórico de uma tabela com os metadados da biblioteca:

```shell
serialize-db history --root s3://bucket/projeto/delta --environment prod \
    --metadata pipeline.models:Base.metadata --table cad_lancamentos
```

Depois: uma linha por commit, do mais recente ao mais antigo, com a versão, a operação, o
instante em UTC e `serialize_db_execution_id`, `serialize_db_input_versions` e
`serialize_db_snapshot` quando o commit os tem, o que só os commits das partições de uma execução
fazem: a criação da tabela, a reconciliação do esquema, a reescrita, a cópia do arquivo, o
`restore` de uma releitura reprovada, o `vacuum` (`VACUUM START`, `VACUUM END`) e o `OPTIMIZE`
vêm sem eles.

## Opções da linha de comando

As opções de cada subcomando de `serialize-db`. Um valor de partição, um `--execution-id`, um
`--name` e o ambiente seguem a regra da partição, `[0-9A-Za-z][0-9A-Za-z_.-]*`, e o valor fora
dela é erro de uso. A conexão do Redshift, em `publish`, em `run --redshift` e nos subcomandos
com `--engine redshift`, vem das variáveis `SERIALIZE_DB_REDSHIFT_*`
(`serialize_db.engine.redshift.RedshiftConfig.from_environment`).

### Opções comuns

| Opção | Padrão | Descrição |
| --- | --- | --- |
| `--metadata modulo:atributo` | obrigatória | O caminho importável do `MetaData` dos modelos, como `pipeline.models:Base.metadata`. |
| `--root` | `SERIALIZE_DB_ROOT` | A raiz das tabelas Delta, pasta local ou `s3://bucket/prefixo`; obrigatória sem a variável. |
| `--environment` | `SERIALIZE_DB_ENVIRONMENT`, senão `dev`; a variável vazia conta como ausente | O ambiente, a pasta sob a raiz: cada tabela fica em `<raiz>/<ambiente>/<tabela>`. |

`run`, `load` e as rotinas de operação (`snapshot`, `vacuum`, `compact`, `archive`, `export` e
`history`) recebem as três; `audit` e `publish` também, com `--root` e, em `publish`,
`--metadata` dispensáveis nos casos descritos nas seções deles; `schema` e `sql` recebem só
`--metadata`.

### `schema write` e `schema check`

| Opção | Padrão | Descrição |
| --- | --- | --- |
| `directory` (posicional) | obrigatória | A pasta dos arquivos de esquema, que `write` grava e `check` compara com a geração nova. |

### `sql write` e `sql check`

| Opção | Padrão | Descrição |
| --- | --- | --- |
| `--statements modulo:atributo` | obrigatória | O caminho importável do dicionário `{nome: statement}` do pipeline. |
| `directory` (posicional) | obrigatória | A pasta dos arquivos de texto SQL, que `write` grava e `check` compara com a geração nova. |

### `run`

| Opção | Padrão | Descrição |
| --- | --- | --- |
| `pipeline` (posicional) | obrigatória | `modulo:funcao`, a função que recebe a execução aberta. |
| `--partition` | obrigatória | O valor da partição da execução. |
| `--engine` | `SERIALIZE_DB_ENGINE`, senão `duckdb` | O motor do sandbox: `duckdb` ou `redshift`. |
| `--execution-id` | `exec-<AAAA-MM-DD>-<uuid8>`, com a data em UTC | O identificador da execução. |
| `--redshift` | desligada | Dá à execução a configuração do Redshift, para `run.publish_redshift`; com `--engine redshift` ela já entra. |

### `audit`

| Opção | Padrão | Descrição |
| --- | --- | --- |
| `--table` | obrigatória | A tabela do modelo auditada. |
| `--partitions` | todas | As partições auditadas; sem ela, a tabela inteira. |
| `--foreign-keys` | desligada | Confere as chaves estrangeiras contra a versão atual de cada tabela referenciada. |
| `--key-scope` | nenhum | `partition` suprime a verificação da chave contra a versão publicada; `table` a faz também na chave com a coluna de `partition_source`. |
| `--engine` | `SERIALIZE_DB_ENGINE`, senão `duckdb` | O dialeto do texto e o motor da auditoria: `duckdb` ou `redshift`. |
| `--sql` | desligada | Imprime o texto das verificações, sem conexão nem armazenamento. |
| `--root` | `SERIALIZE_DB_ROOT` | Obrigatória sem `--sql`. |

### `load`

| Opção | Padrão | Descrição |
| --- | --- | --- |
| `--source` | obrigatória | A raiz da base Parquet de origem, pasta local ou `s3://bucket/prefixo`. |
| `--tables` | todas do modelo | As tabelas carregadas, na ordem da carga: as sem partição antes das particionadas. |
| `--partitions` | todas | As partições carregadas; com ela, as tabelas sem partição ficam de fora. |

### `publish`

| Opção | Padrão | Descrição |
| --- | --- | --- |
| `--init` | desligada | Cria a tabela de controle `serialize_db_publications`, uma vez; dispensa `--metadata` e `--root`. |
| `--status` | desligada | Mostra a versão publicada, a atual e as partições pendentes de cada tabela do modelo que existe no ambiente. |
| `--unpublish` | desligada | Despublica as tabelas: `DROP TABLE` e a linha de controle. |
| `--tables` | todas do modelo | As tabelas publicadas ou despublicadas. |
| `--max-workers` | 1 | As tabelas publicadas ao mesmo tempo, cada uma na sua conexão. |
| `--execution-id` | `publicacao-<AAAA-MM-DD>-<uuid8>`, com a data em UTC | O identificador gravado na linha de controle. |
| `--metadata`, `--root` | obrigatórias | Dispensadas por `--init`. |

`--init`, `--status` e `--unpublish` valem nesta ordem; sem nenhuma delas, o comando publica.

### `snapshot`

| Opção | Padrão | Descrição |
| --- | --- | --- |
| `--name` | obrigatória | O nome do snapshot, inédito em `snapshots` e em `archived` do arquivo de controle. |

### `vacuum`

| Opção | Padrão | Descrição |
| --- | --- | --- |
| `--apply` | desligada | Apaga os arquivos listados; sem ela, só lista. |
| `--full` | desligada | Inclui os arquivos órfãos, que nenhuma versão do log referencia. |
| `--retention-hours` | 9600, os 400 dias | A retenção, em horas. |

### `compact`

| Opção | Padrão | Descrição |
| --- | --- | --- |
| `--table` | obrigatória | A tabela do modelo compactada. |
| `--partitions` | a tabela inteira | As partições compactadas; obrigatória numa tabela particionada. |

### `archive`

| Opção | Padrão | Descrição |
| --- | --- | --- |
| `--name` | obrigatória | O snapshot arquivado, registrado em `snapshots`. |

### `export`

| Opção | Padrão | Descrição |
| --- | --- | --- |
| `--table` | obrigatória | A tabela do modelo exportada. |
| `--destination` | obrigatória | A URI da pasta de destino, vazia e sob a raiz. |
| `--version` | a atual | A versão exportada. |
| `--mode` | `copy` | `copy` copia os arquivos que o log da versão lista; `rewrite` grava um arquivo por partição pelo `COPY` do DuckDB. |

### `history`

| Opção | Padrão | Descrição |
| --- | --- | --- |
| `--table` | obrigatória | A tabela do modelo cujos commits são listados. |
