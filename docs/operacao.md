## Operação

O runbook das rotinas de operação do banco Delta, cada uma um subcomando de `serialize-db` sobre as
primitivas de `serialize_db.delta`, com o que conferir antes e o que esperar depois. Os subcomandos
recebem `--metadata modulo:atributo`, `--root` (`SERIALIZE_DB_ROOT`) e `--environment`
(`SERIALIZE_DB_ENVIRONMENT`, `dsv`), menos `publish_redshift --init`, que lê só as variáveis
`SERIALIZE_DB_REDSHIFT_*`. Saem com 0 quando terminam; com 1 na auditoria reprovada, na carga com
diferença de contagem ou soma ou com partição fora do contrato, e no erro sem tratamento, que
imprime o traceback; e com 2 no erro de uso, na configuração do Redshift sem conexão ou com a porta
que não é número, no nome repetido ou ausente e no conflito com outro escritor, no arquivo de
controle ou na tabela. `compact`, `archive` e `export` imprimem por tabela o tempo e o pico de
memória residente do processo (`VmHWM`), a medida da rotina na tabela com que a máquina é
dimensionada; a publicação a põe na linha de cada tabela no log `serialize_db.publication`. A linha
de comando imprime o log no stderr a partir do nível `INFO`.

### Tabela de controle da publicação

Uma vez no esquema do Redshift, antes da primeira publicação de qualquer ambiente:

```shell
serialize-db publish_redshift --init
```

Antes: as variáveis `SERIALIZE_DB_REDSHIFT_*` da conexão. Depois: a tabela
`serialize_db_publications` no esquema; sem ela, `serialize-db publish_redshift` recusa publicar com
`serialize_db.errors.PublicationError`.

### Snapshot do banco

Na periodicidade do processo, por exemplo o fim do trimestre, dentro da execução marcada
(`run.snapshot("2026T3")`, que recusa o nome já usado antes dos commits e grava a entrada no
encerramento sem erro) ou fora dela, com a versão atual de cada tabela do ambiente:

```shell
serialize-db snapshot --root s3://bucket/projeto/delta --environment prd \
    --metadata pipeline.models:Base.metadata --name 2026T3
```

Antes: nenhuma execução aberta no ambiente, porque a entrada leva a versão atual de cada tabela,
e um nome inédito em `snapshots` e em `archived` do arquivo de controle
`<ambiente>/_serialize_db/snapshots.json`. Depois: a entrada `{nome: {tabela: versão}}` gravada na
escrita condicional; outro escritor entre a leitura e a escrita dá
`serialize_db.errors.ConflictError`, e o comando se repete. As versões marcadas ficam legíveis
qualquer que seja a retenção do `vacuum`.

O nome de um snapshot é imutável: a entrada gravada nunca muda de versões, e o nome não volta a ser
usado, nem depois do arquivamento. O leitor, a publicação e os canais citam o snapshot pelo nome, e
o arquivamento o usa na pasta `<raiz>/<ambiente>/arquivo/<nome>/`.

### Canal do snapshot

Depois do snapshot que os clientes vão ler, o canal `default` do ambiente aponta para ele: é o
snapshot que o leitor Delta abre sem argumento e que
`serialize-db publish_redshift --channel default` publica. Só este comando o move:

```shell
serialize-db channel --root s3://bucket/projeto/delta --environment prd \
    --metadata pipeline.models:Base.metadata --name default --snapshot 2026T3
serialize-db channel --root s3://bucket/projeto/delta --environment prd \
    --metadata pipeline.models:Base.metadata
```

Antes: o snapshot em `snapshots` do arquivo de controle, não arquivado. Depois: o canal sob a
chave `channels` do mesmo arquivo, impresso com o snapshot anterior e o novo
(`default: 2026T2 -> 2026T3`); sem `--name` e `--snapshot`, o comando lista os canais. O canal
`current` é reservado, a versão atual de cada tabela, e nada o move; o `archive` recusa o
snapshot de um canal até o canal ser movido.

### Publicação no Redshift

Depois da execução, com o snapshot escolhido pelo canal ou pelo nome:

```shell
serialize-db publish_redshift --root s3://bucket/projeto/delta --environment prd \
    --metadata pipeline.models:Base.metadata --channel default --max-workers 4
serialize-db publish_redshift --root s3://bucket/projeto/delta --environment prd \
    --metadata pipeline.models:Base.metadata --status
```

Antes: a tabela de controle no esquema, as variáveis `SERIALIZE_DB_REDSHIFT_*` e, com
`--channel default`, o canal apontado. Depois: cada tabela do modelo presente no snapshot, na versão
dele, na tabela `<ambiente>_<tabela>` do esquema do Redshift, que a primeira publicação cria, só com
as partições alteradas desde a versão publicada; a linha de controle com a versão e o
`--execution-id`; e os manifestos do `COPY` em
`<raiz>/<ambiente>/publicacao/<execution_id>/<tabela>/<valor>/`, que nenhum subcomando apaga. A
tabela do modelo fora do snapshot é erro de uso, e `--tables` a deixa de fora. A publicação de um
snapshot anterior ao publicado volta a tabela: as partições alteradas entre as duas versões recebem
os arquivos da versão pedida, e a partição que só a versão publicada tinha sai. `--channel current`
publica a versão atual de cada tabela, sem snapshot.

### Refazer um snapshot

Os dados de um snapshot já gravado se refazem num snapshot novo, e o canal passa a apontar para ele:

1. A execução roda de novo marcada com outro nome, como `run.snapshot("2026T3.r2")`. O
   `publish_delta` substitui as partições refeitas, e a entrada nova leva a versão atual de toda
   tabela do ambiente, inclusive das que não mudaram.
2. `serialize-db channel --name default --snapshot 2026T3.r2` aponta o canal para o snapshot novo,
   que o leitor Delta passa a abrir sem argumento.
3. `serialize-db publish_redshift --channel default` leva ao Redshift as partições alteradas desde
   a versão publicada.

O `2026T3` continua legível pelo nome e prende as versões dele no `vacuum` até o `archive`.

Se a correção não servir, até o `archive` do `2026T3`, o canal volta para ele, e a publicação pelo
canal devolve as tabelas do Redshift às versões dele:

```shell
serialize-db channel --root s3://bucket/projeto/delta --environment prd \
    --metadata pipeline.models:Base.metadata --name default --snapshot 2026T3
serialize-db publish_redshift --root s3://bucket/projeto/delta --environment prd \
    --metadata pipeline.models:Base.metadata --channel default
```

A volta troca o snapshot que o leitor Delta abre sem argumento e as tabelas do Redshift, e a
versão atual de cada tabela no Delta continua com as partições refeitas: a próxima execução as lê
na versão fixada, o snapshot seguinte as leva, e a publicação dele as devolve ao Redshift. A
correção que não serve se desfaz no Delta, antes da próxima execução, por outra execução que grava
as partições de novo, marcada com outro nome, como `2026T3.r3`.

### Compactação

Antes de um snapshot, nunca depois: a compactação depois do snapshot dobra os arquivos que ele
referencia. Ela junta os arquivos pequenos das partições pedidas e normaliza, entre eles, os que o
`UNLOAD` gravou (`INT64` no lugar de `INT96` e de `FIXED_LEN_BYTE_ARRAY`, estatística em toda
coluna):

```shell
serialize-db compact --root s3://bucket/projeto/delta --environment prd \
    --metadata pipeline.models:Base.metadata --table cad_lancamentos_projetados \
    --partitions 2026-07-31 2026-08-31
```

Antes: o arquivo de controle sem snapshot na versão atual da tabela (o comando confere e recusa);
a memória da máquina, porque a reescrita roda no escritor do delta-rs, fora do `memory_limit` do
DuckDB, e a memória dela numa partição de `cad_lancamentos` não foi medida. Depois: `numFilesAdded`
e `numFilesRemoved` impressos com o tempo e o pico de RSS do processo, a medida da memória da
compactação; um commit `OPTIMIZE` com `dataChange` falso, que `serialize_db.delta.version_diff`
não conta.

O delta-rs junta numa partição só os arquivos que cabem juntos no tamanho alvo, a propriedade
`delta.targetFileSize` da tabela ou 100 MB sem ela, e deixa como está o arquivo que não cabe com
outro. Sem nada a juntar, como na partição de um arquivo só que a carga inicial grava, ele não grava
nem commita, e o comando imprime `nada a juntar em <n> arquivo(s), nenhum commit`, com os arquivos
que leu nas partições pedidas.

### `vacuum`

Mensal: a lista dos arquivos fora da retenção e fora das versões dos snapshots, revisada, depois
aplicada; `--full` de tempos em tempos para os órfãos das escritas interrompidas e dos registros
recusados:

```shell
serialize-db vacuum --root s3://bucket/projeto/delta --environment prd \
    --metadata pipeline.models:Base.metadata
serialize-db vacuum --root s3://bucket/projeto/delta --environment prd \
    --metadata pipeline.models:Base.metadata --apply
serialize-db vacuum --root s3://bucket/projeto/delta --environment prd \
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

Anual: os snapshots mais velhos que o prazo da tabela viva vão para
`<raiz>/<ambiente>/arquivo/<nome>/<tabela>/`, pela cópia dos arquivos de cada partição e o registro
deles, sem os dados passarem pela máquina, e a entrada passa de `snapshots` para `archived` no mesmo
arquivo de controle:

```shell
serialize-db archive --root s3://bucket/projeto/delta --environment prd \
    --metadata pipeline.models:Base.metadata --name 2026T3
```

Antes: o snapshot registrado em `snapshots` e apontado por nenhum canal, que `serialize-db channel`
move antes; a pasta `<raiz>/<ambiente>/arquivo/<nome>/` recebe a regra de ciclo de vida do bucket.
Depois: uma tabela nova por tabela do snapshot, com uma versão por partição, os mesmos arquivos e as
mesmas somas, cada tabela impressa com o tempo da cópia e o pico de RSS do processo, e o tempo de
cada partição no log `serialize_db.delta`; a entrada em `archived`, que `vacuum` não prende mais, e
`snapshot` recusando o nome, porque ele dá a pasta. O comando se repete depois de uma interrupção e
continua de onde parou: a tabela já inteira no arquivo e a partição já registrada nele são puladas,
e só o que falta é copiado.

### Exportação

Sob demanda: uma versão da tabela Delta gravada em pastas Parquet `<coluna>=<valor>/`, sem o log, na
pasta de `--destination`, sob a raiz:

```shell
serialize-db export --root s3://bucket/projeto/delta --environment prd \
    --metadata pipeline.models:Base.metadata --table cad_lancamentos_projetados \
    --destination s3://bucket/projeto/delta/prd/exportacao/2026T3/cad_lancamentos_projetados \
    --version 143 --mode copy
```

Antes: o destino vazio e sob a raiz. Depois: em `copy`, os mesmos bytes dos arquivos que o log da
versão lista; em `rewrite`, um arquivo por partição pelo `COPY` do DuckDB, com o esquema da versão
em todos; o tempo e o pico de RSS do processo impressos; `--version` ausente é a versão atual.

### Auditoria avulsa

Depois de uma correção, e quando o SQL de uma verificação precisa ser lido:

```shell
serialize-db audit --root s3://bucket/projeto/delta --environment prd \
    --metadata pipeline.models:Base.metadata --table cad_lancamentos --foreign-keys
serialize-db audit --metadata pipeline.models:Base.metadata --table cad_lancamentos --sql
```

Depois: o veredito de cada verificação, com as amostras; 1 quando alguma reprova. Quando o banco
recusa a ingestão pelo DDL do modelo, como no nulo numa coluna `NOT NULL` ou no JSON malformado, o
comando imprime `ingestão: reprovada (<erro do banco>)`, sem as outras contagens, e sai com 1.

### Monitoração

O histórico de uma tabela com os metadados da biblioteca:

```shell
serialize-db history --root s3://bucket/projeto/delta --environment prd \
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
`--name`, um `--snapshot`, um `--channel` e o ambiente seguem a regra da partição,
`[0-9A-Za-z][0-9A-Za-z_.-]*`, e o valor fora dela é erro de uso. A conexão do Redshift, em
`publish_redshift` e nos subcomandos com `--engine redshift`, vem das variáveis
`SERIALIZE_DB_REDSHIFT_*` (`serialize_db.engine.redshift.RedshiftConfig.from_environment`).

### Opções comuns

| Opção | Padrão | Descrição |
| --- | --- | --- |
| `--metadata modulo:atributo` | obrigatória | O caminho importável do `MetaData` dos modelos, como `pipeline.models:Base.metadata`. |
| `--root` | `SERIALIZE_DB_ROOT` | A raiz das tabelas Delta, pasta local ou `s3://bucket/prefixo`; obrigatória sem a variável. Fora dos armazenamentos da biblioteca, ou no S3 sem região, é erro de uso, em todo subcomando. |
| `--environment` | `SERIALIZE_DB_ENVIRONMENT`, senão `dsv`; a variável vazia conta como ausente | O ambiente, a pasta sob a raiz: cada tabela fica em `<raiz>/<ambiente>/<tabela>`, e em `publish_redshift` a tabela publicada se chama `<ambiente>_<tabela>`. |

`run`, `import` e as rotinas de operação (`snapshot`, `channel`, `vacuum`, `compact`, `archive`,
`export` e `history`) recebem as três; `audit` e `publish_redshift` também, com `--root` e, em
`publish_redshift`, `--metadata` dispensáveis nos casos descritos nas seções deles; `schema` e `sql`
recebem só `--metadata`.

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
| `--partition` | nenhum | O valor da partição da execução; sem ela, a execução não tem partição e `run.partition` é `None`. |
| `--engine` | `SERIALIZE_DB_ENGINE`, senão `duckdb` | O motor do sandbox: `duckdb` ou `redshift`. |
| `--execution-id` | `exec-<AAAA-MM-DD>-<uuid8>`, com a data em UTC | O identificador da execução. |

### `audit`

| Opção | Padrão | Descrição |
| --- | --- | --- |
| `--table` | obrigatória | A tabela do modelo auditada. |
| `--partitions` | todas | As partições auditadas; sem ela, a tabela inteira. Numa tabela sem partição, é erro de uso. |
| `--foreign-keys` | desligada | Confere as chaves estrangeiras contra a versão atual de cada tabela referenciada. |
| `--key-scope` | nenhum | `partition` suprime a verificação da chave contra a versão atual do Delta; `table` a faz também na chave com a coluna de `partition_source`. |
| `--engine` | `SERIALIZE_DB_ENGINE`, senão `duckdb` | O dialeto do texto e o motor da auditoria: `duckdb` ou `redshift`. |
| `--sql` | desligada | Imprime o texto das verificações, sem conexão nem armazenamento. |
| `--root` | `SERIALIZE_DB_ROOT` | Obrigatória sem `--sql`. |

### `import`

| Opção | Padrão | Descrição |
| --- | --- | --- |
| `--source` | obrigatória | A raiz da base Parquet de origem, pasta local ou `s3://bucket/prefixo`, com uma pasta por tabela, `<origem>/<tabela>/`, e as partições em `<coluna>=<valor>/`; a carga só a lê e grava em `<raiz>/<ambiente>/<tabela>`. |
| `--tables` | todas do modelo | As tabelas carregadas, na ordem da carga: as sem partição antes das particionadas. |
| `--partitions` | todas | As partições carregadas e conferidas no relatório, que toda tabela particionada da carga precisa ter na origem: uma que falta é recusada antes de qualquer gravação, com a saída 1; com ela, a tabela sem partição fica inteira de fora, sem carga nem relatório, numa linha da saída. |

### `publish_redshift`

| Opção | Padrão | Descrição |
| --- | --- | --- |
| `--init` | desligada | Cria a tabela de controle `serialize_db_publications`, uma vez; dispensa `--metadata` e `--root`. |
| `--status` | desligada | Mostra a versão publicada, a atual e as partições pendentes de cada tabela do modelo que existe no ambiente. |
| `--unpublish` | desligada | Despublica as tabelas: `DROP TABLE` e a linha de controle. |
| `--snapshot` | nenhum | O snapshot publicado, pelo nome, registrado em `snapshots`; o arquivado é erro de uso. |
| `--channel` | nenhum | O canal cujo snapshot é publicado: `default`, ou `current`, a versão atual de cada tabela do modelo que existe no ambiente. |
| `--tables` | todas do modelo | As tabelas publicadas ou despublicadas; a tabela do modelo sem versão no snapshot é erro de uso, e a opção a deixa de fora. |
| `--max-workers` | 1 | As tabelas publicadas ao mesmo tempo, cada uma na sua conexão. |
| `--execution-id` | `publicacao-<AAAA-MM-DD>-<uuid8>`, com a data em UTC | O identificador gravado na linha de controle, que dá a pasta dos manifestos do `COPY`, `<raiz>/<ambiente>/publicacao/<execution_id>/`. |
| `--metadata`, `--root` | obrigatórias | Dispensadas por `--init`. |

`--init`, `--status` e `--unpublish` valem nesta ordem e não recebem `--snapshot` nem
`--channel`; sem nenhuma delas, o comando publica as versões de `--snapshot` ou de `--channel`,
um dos dois obrigatório e excludentes.

### `snapshot`

| Opção | Padrão | Descrição |
| --- | --- | --- |
| `--name` | obrigatória | O nome do snapshot, inédito em `snapshots` e em `archived` do arquivo de controle. |

### `channel`

| Opção | Padrão | Descrição |
| --- | --- | --- |
| `--name` | nenhum | O canal apontado, pela regra da partição; `current` é reservado e é erro de uso. Vai com `--snapshot`; sem os dois, o comando lista os canais do ambiente. |
| `--snapshot` | nenhum | O snapshot apontado, registrado em `snapshots`; o ausente e o arquivado são erro de uso. |

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
| `--name` | obrigatória | O snapshot arquivado, registrado em `snapshots`; a cópia vai para `<raiz>/<ambiente>/arquivo/<nome>/<tabela>`. |

### `export`

| Opção | Padrão | Descrição |
| --- | --- | --- |
| `--table` | obrigatória | A tabela do modelo exportada. |
| `--destination` | obrigatória | A URI da pasta que recebe os arquivos Parquet, vazia e sob a raiz. |
| `--version` | a atual | A versão exportada. |
| `--mode` | `copy` | `copy` copia os arquivos que o log da versão lista; `rewrite` grava um arquivo por partição pelo `COPY` do DuckDB. |

### `history`

| Opção | Padrão | Descrição |
| --- | --- | --- |
| `--table` | obrigatória | A tabela do modelo cujos commits são listados. |
