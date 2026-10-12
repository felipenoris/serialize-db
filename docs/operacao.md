## Operação

Este runbook descreve as rotinas de operação do banco: a carga inicial, a publicação no Redshift,
a auditoria avulsa e as rotinas sobre as primitivas de `serialize_db.delta`. Cada rotina é um
subcomando de `serialize-db`, apresentado com o que conferir antes de rodá-lo e o que esperar
depois.

Os subcomandos recebem três opções comuns: `--metadata modulo:atributo`, `--root` (ou a variável
`SERIALIZE_DB_ROOT`) e `--environment` (ou `SERIALIZE_DB_ENVIRONMENT`, com o padrão `dsv`). Duas
exceções: `publish_redshift --init` lê só as variáveis `SERIALIZE_DB_REDSHIFT_*`, e `audit --sql`
lê só `--metadata`.

Os códigos de saída:

- 0 quando o subcomando termina;
- 1 na auditoria reprovada, na carga com diferença de contagem ou soma ou com partição fora do
  contrato, e no erro sem tratamento, que imprime o traceback;
- 2 no erro de uso, na configuração do Redshift sem conexão ou com a porta que não é número, no
  nome repetido ou ausente, e no conflito com outro escritor, no arquivo de controle ou na tabela.

`compact`, `archive` e `export` imprimem, por tabela, o tempo e o pico de memória residente do
processo (`VmHWM`). Essa medida, lida na rotina sobre a tabela, dimensiona a máquina. A
publicação põe a mesma medida na linha de cada tabela no log `serialize_db.publication`, e
`scripts/migrate_parquet_to_delta.py` a imprime por partição da carga. A linha de comando imprime
o log no stderr a partir do nível `INFO`.

Todo subcomando que abre o DuckDB, da carga à releitura de um commit, carrega as extensões `delta`
e, no S3, `httpfs`. Ele as lê da pasta de `SERIALIZE_DB_DUCKDB_EXTENSIONS`; sem a variável, de
`.duckdb/` ao lado do ambiente virtual, quando existe; senão, da pasta padrão do DuckDB. Nada é
baixado.

### Carga inicial

A carga inicial roda uma vez por ambiente, antes da primeira execução. Ela leva a base Parquet de
origem para as tabelas Delta do ambiente, uma partição por commit, e o relatório confere contagem
e somas por partição:

```shell
serialize-db import --root s3://bucket/projeto/delta --environment prd \
    --metadata pipeline.models:Base.metadata --source s3://bucket/projeto/db_projetado
```

Antes: a origem tem uma pasta por tabela, `<origem>/<tabela>/`, e a carga só a lê. Confira a
memória da máquina, porque o pico do processo passa do `memory_limit` do DuckDB. Nas dez cargas da
base de 2026-09-25 a 2026-10-11, em máquinas de 2, 4 e 8 vCPUs, o pico do processo da carga ficou
entre 1,38 e 1,51 vez o `memory_limit` do DuckDB, entre 69% e 76% da memória disponível, sempre na
maior partição de `cad_lancamentos`: 4.138 MB com 5.801 MB disponíveis e 2.900 MiB de limite em
2026-10-07, e 9.557 MB com 12.651 MB e 6.325 MiB em 2026-10-09. O pico acompanha o limite, que é
metade da memória disponível que `environment_limits` lê na abertura de cada conexão, e não o
tamanho da partição. A carga coube em todas as máquinas medidas, com ao menos um quarto da memória
disponível de folga; a máquina maior só a encurta (0,35 milhão de linhas por segundo com 2 vCPUs,
0,66 com 8).

Depois: cada partição da origem está num commit da tabela Delta, com `carga-<uuid8>` em
`serialize_db_execution_id`. A segunda chamada não grava nada. A chamada depois de uma falha
recomeça da partição que falhou. A saída é 1 na diferença de contagem ou soma e na partição fora
do contrato. `--partitions` restringe a carga a algumas partições, e `--ignore-partitions` deixa
partições fora dela.

### Tabela de controle da publicação

A tabela de controle é criada uma vez no esquema do Redshift, antes da primeira publicação de
qualquer ambiente:

```shell
serialize-db publish_redshift --init
```

Antes: as variáveis `SERIALIZE_DB_REDSHIFT_*` da conexão. Depois: a tabela
`serialize_db_publications` existe no esquema. Sem ela, `serialize-db publish_redshift` recusa
publicar, com a saída 2 e a mensagem de `serialize_db.errors.PublicationError`.

### Snapshot do banco

Um snapshot registra a versão atual de cada tabela do ambiente sob um nome. Ele é gravado na
periodicidade do processo, por exemplo no fim do trimestre, de um de dois jeitos. Dentro da
execução marcada, `run.snapshot("2026T3")` recusa o nome já usado antes dos commits e grava a
entrada no encerramento sem erro. Fora da execução, este comando grava a entrada com a versão
atual de cada tabela do ambiente:

```shell
serialize-db snapshot --root s3://bucket/projeto/delta --environment prd \
    --metadata pipeline.models:Base.metadata --name 2026T3
```

Antes: nenhuma execução aberta no ambiente, porque a entrada leva a versão atual de cada tabela; e
um nome inédito em `snapshots` e em `archived` do arquivo de controle
`<ambiente>/_serialize_db/snapshots.json`. Depois: a entrada `{nome: {tabela: versão}}` gravada
por escrita condicional. Se outro escritor gravou o arquivo entre a leitura e a escrita, o comando
sai com 2 e a mensagem do conflito (`serialize_db.errors.ConflictError`), e basta repeti-lo. As
versões marcadas ficam legíveis qualquer que seja a retenção do `vacuum`.

O nome de um snapshot é imutável: a entrada gravada nunca muda de versões, e o nome não volta a ser
usado, nem depois do arquivamento. O leitor, a publicação e os canais citam o snapshot pelo nome, e
o arquivamento o usa na pasta `<raiz>/<ambiente>/arquivo/<nome>/`.

### Canal do snapshot

Um canal é um nome que aponta para um snapshot. O canal `default` do ambiente aponta para o
snapshot que os clientes vão ler: é o que o leitor Delta abre sem argumento e o que
`serialize-db publish_redshift --channel default` publica. Só este comando o move:

```shell
serialize-db channel --root s3://bucket/projeto/delta --environment prd \
    --metadata pipeline.models:Base.metadata --name default --snapshot 2026T3
serialize-db channel --root s3://bucket/projeto/delta --environment prd \
    --metadata pipeline.models:Base.metadata
```

Antes: o snapshot está em `snapshots` do arquivo de controle, não arquivado. Depois: o canal fica
sob a chave `channels` do mesmo arquivo, e o comando imprime o snapshot anterior e o novo
(`default: 2026T2 -> 2026T3`). Sem `--name` e `--snapshot`, o comando lista os canais. O canal
`current` é reservado: ele significa a versão atual de cada tabela, e nada o move. O `archive`
recusa o snapshot apontado por um canal até o canal ser movido.

### Publicação no Redshift

A publicação roda depois da execução, com o snapshot escolhido pelo canal ou pelo nome:

```shell
serialize-db publish_redshift --root s3://bucket/projeto/delta --environment prd \
    --metadata pipeline.models:Base.metadata --channel default --max-workers 4
serialize-db publish_redshift --root s3://bucket/projeto/delta --environment prd \
    --metadata pipeline.models:Base.metadata --status
```

Antes: a tabela de controle no esquema, as variáveis `SERIALIZE_DB_REDSHIFT_*` e, com
`--channel default`, o canal apontado. Depois: cada tabela do modelo presente no snapshot está na
versão dele na tabela `<ambiente>_<tabela>` do esquema do Redshift. A primeira publicação cria essa
tabela; as seguintes levam só as partições alteradas desde a versão publicada. A linha de controle
guarda a versão e o `--execution-id`. Os manifestos do `COPY` ficam em
`<raiz>/<ambiente>/publicacao/<execution_id>/<tabela>/<valor>/`, e nenhum subcomando os apaga. A
tabela do modelo fora do snapshot é erro de uso, e `--tables` a deixa de fora. Publicar um snapshot
anterior ao publicado volta a tabela: as partições alteradas entre as duas versões recebem os
arquivos da versão pedida, e a partição que só a versão publicada tinha sai. `--channel current`
publica a versão atual de cada tabela, sem snapshot.

### Refazer um snapshot

Os dados de um snapshot já gravado se refazem num snapshot novo, e o canal passa a apontar para ele:

1. A execução roda de novo, marcada com outro nome, como `run.snapshot("2026T3.r2")`. O
   `publish_delta` substitui as partições refeitas, e a entrada nova leva a versão atual de toda
   tabela do ambiente, inclusive das que não mudaram.
2. `serialize-db channel` aponta o canal `default` para o snapshot novo, que o leitor Delta passa
   a abrir sem argumento.
3. `serialize-db publish_redshift --channel default` leva ao Redshift as partições alteradas desde
   a versão publicada.

Os comandos dos passos 2 e 3:

```shell
serialize-db channel --root s3://bucket/projeto/delta --environment prd \
    --metadata pipeline.models:Base.metadata --name default --snapshot 2026T3.r2
serialize-db publish_redshift --root s3://bucket/projeto/delta --environment prd \
    --metadata pipeline.models:Base.metadata --channel default
```

O `2026T3` continua legível pelo nome e prende as versões dele no `vacuum` até o `archive`.

Se a correção não servir, o canal volta para o `2026T3`, enquanto ele não for arquivado, e a
publicação pelo canal devolve as tabelas do Redshift às versões dele:

```shell
serialize-db channel --root s3://bucket/projeto/delta --environment prd \
    --metadata pipeline.models:Base.metadata --name default --snapshot 2026T3
serialize-db publish_redshift --root s3://bucket/projeto/delta --environment prd \
    --metadata pipeline.models:Base.metadata --channel default
```

A volta troca duas coisas: o snapshot que o leitor Delta abre sem argumento e as tabelas do
Redshift. A versão atual de cada tabela no Delta continua com as partições refeitas: a próxima
execução as lê na versão fixada, o snapshot seguinte as leva, e a publicação dele as devolve ao
Redshift. Para desfazer no Delta a correção que não serve, antes da próxima execução, outra
execução grava as partições de novo, marcada com outro nome, como `2026T3.r3`.

### Compactação

A compactação junta os arquivos pequenos das partições pedidas. Entre eles, ela normaliza os que o
`UNLOAD` gravou: `INT64` no lugar de `INT96` e de `FIXED_LEN_BYTE_ARRAY`, e estatística em toda
coluna. Ela roda antes de um snapshot, nunca depois: a compactação depois do snapshot dobra os
arquivos que ele referencia.

```shell
serialize-db compact --root s3://bucket/projeto/delta --environment prd \
    --metadata pipeline.models:Base.metadata --table cad_lancamentos_projetados \
    --partitions 2026-07-31 2026-08-31
```

Antes: o arquivo de controle não tem snapshot na versão atual da tabela (o comando confere e
recusa). Confira a memória da máquina, porque a reescrita roda no escritor do delta-rs, fora do
`memory_limit` do DuckDB. As compactações no ambiente alvo da mesma partição de `cad_lancamentos`,
de 33.239.719 linhas, que saiu sempre em 6 arquivos:

| Data | vCPUs | Memória disponível | Arquivos antes | Arquivos depois | Tempo | Pico do processo |
| --- | --- | --- | --- | --- | --- | --- |
| 2026-10-05 | 8 | 12,5 GiB | 64, de 5,3 a 16,7 MB, 551,5 MB gravados pelo DuckDB | de 43,5 a 67,8 MB, 370,0 MB | 6,0 s | 1.714 MB |
| 2026-10-07 | 2 | 5,6 GiB | 40, de 11,6 a 16,7 MB | de 51,4 a 65,1 MB, 367,1 MB | 23,6 s | 481 MB |
| 2026-10-09 | 4 | 12,9 GiB | 50, de 5,3 a 19,5 MB | de 53,4 a 65,3 MB, 368,6 MB | 12,4 s | 762 MB |
| 2026-10-09, 21:55 | 8 | 12,4 GiB | 64, de 5,4 a 20,5 MB | de 55,1 a 66,9 MB, 370,1 MB | 5,6 s | 1.445 MB |
| 2026-10-10 | 8 | 12,6 GiB | 64, de 5,4 a 16,6 MB | de 54,4 a 65,8 MB, 370,5 MB | 6,0 s | 1.649 MB |
| 2026-10-10, 18:25 | 8 | 12,6 GiB | 61, de 5,6 a 21,2 MB | de 54,6 a 65,7 MB, 370,1 MB | 5,2 s | 1.502 MB |
| 2026-10-11 | 8 | 12,4 GiB | 66, de 5,5 a 18,7 MB | de 47,6 a 68,5 MB, 369,9 MB | 5,6 s | 1.653 MB |

Depois: `numFilesAdded` e `numFilesRemoved` impressos com o tempo e o pico de RSS do processo, a
medida da memória da compactação; e um commit `OPTIMIZE` com `dataChange` falso, que
`serialize_db.delta.version_diff` não conta.

O delta-rs junta numa partição só os arquivos que cabem juntos no tamanho alvo (a propriedade
`delta.targetFileSize` da tabela, ou 100 MB sem ela) e deixa como está o arquivo que não cabe com
outro. Sem nada a juntar, como na partição de um arquivo só que a carga inicial grava, ele não
grava nem commita, e o comando imprime `nada a juntar em <n> arquivo(s), nenhum commit`, com os
arquivos que leu nas partições pedidas.

### Limpeza dos arquivos removidos

`serialize-db vacuum` roda uma vez por mês. Ele lista os arquivos fora da retenção e fora das
versões dos snapshots; a lista é revisada e depois aplicada. `--full` entra de tempos em tempos,
para os órfãos das escritas interrompidas e dos registros recusados:

```shell
serialize-db vacuum --root s3://bucket/projeto/delta --environment prd \
    --metadata pipeline.models:Base.metadata
serialize-db vacuum --root s3://bucket/projeto/delta --environment prd \
    --metadata pipeline.models:Base.metadata --apply
serialize-db vacuum --root s3://bucket/projeto/delta --environment prd \
    --metadata pipeline.models:Base.metadata --apply --full
```

Antes: o arquivo de controle legível, e a lista sem `--apply` lida. Dentro da retenção de 400 dias
(`--retention-hours 9600`, a janela em que toda versão continua legível) nada é listado; a lista
aparece quando um arquivo removido passa dela. Depois: com `--apply`, os arquivos listados foram
apagados; a versão de cada snapshot continua legível, e as versões intermediárias fora da retenção
não. Num bucket versionado o espaço só é liberado pela regra `NoncurrentVersionExpiration`, e
`probes/bucket.py` (`BK-14`) mostra o acumulado. A seção "Retenção dos arquivos removidos" da
página principal diz como mudar a retenção.

### Arquivamento de um snapshot

`serialize-db archive` roda uma vez por ano. Os snapshots mais velhos que o prazo da tabela viva
vão para `<raiz>/<ambiente>/arquivo/<nome>/<tabela>/`: os arquivos de cada partição são copiados e
registrados, sem os dados passarem pela máquina, e a entrada passa de `snapshots` para `archived`
no mesmo arquivo de controle:

```shell
serialize-db archive --root s3://bucket/projeto/delta --environment prd \
    --metadata pipeline.models:Base.metadata --name 2026T3
```

Antes: o snapshot está registrado em `snapshots`, e nenhum canal aponta para ele
(`serialize-db channel` move o canal antes); a pasta `<raiz>/<ambiente>/arquivo/<nome>/` recebe a
regra de ciclo de vida do bucket. Depois: uma tabela nova por tabela do snapshot, com uma versão
por partição, os mesmos arquivos e a mesma contagem de linhas, lida pelos dois leitores. Cada
tabela é impressa com o tempo da cópia e o pico de RSS do processo, e o tempo de cada partição vai
ao log `serialize_db.delta`. A entrada fica em `archived`, que o `vacuum` não prende mais, e
`snapshot` recusa o nome, porque ele dá a pasta. O comando se repete depois de uma interrupção e
continua de onde parou: a tabela já inteira no arquivo e a partição já registrada nele são
puladas, e só o que falta é copiado.

### Exportação

`serialize-db export` roda sob demanda. Ele grava uma versão da tabela Delta em pastas Parquet
`<coluna>=<valor>/`, sem o log, na pasta de `--destination`, sob a raiz:

```shell
serialize-db export --root s3://bucket/projeto/delta --environment prd \
    --metadata pipeline.models:Base.metadata --table cad_lancamentos_projetados \
    --destination s3://bucket/projeto/delta/prd/exportacao/2026T3/cad_lancamentos_projetados \
    --version 143 --mode copy
```

Antes: o destino está vazio e sob a raiz. Em `rewrite`, confira a memória da máquina, porque o
`COPY` roda com o `memory_limit` do DuckDB e o pico do processo o passa, como na carga. No ambiente
alvo, em 2026-10-10, com 8 vCPUs e 6.315 MiB de limite, `cad_lancamentos` inteira (424.598.150
linhas em 6 partições) levou 80,8 s com o pico em 7.898 MB, contra 21,1 s com 258 MB em `copy`;
em 2026-10-07, com 2 vCPUs e 2.900 MiB de limite, a tabela sem a partição 2026-08-31 levou
278,0 s com 3.507 MB. Depois: em `copy`, os mesmos bytes dos arquivos que o log da versão lista;
em `rewrite`, um arquivo por partição pelo `COPY` do DuckDB, com o esquema da versão em todos. O
tempo e o pico de RSS do processo são impressos. Sem `--version`, a versão exportada é a atual.

### Auditoria avulsa

A auditoria avulsa roda depois de uma correção, e quando o SQL de uma verificação precisa ser lido:

```shell
serialize-db audit --root s3://bucket/projeto/delta --environment prd \
    --metadata pipeline.models:Base.metadata --table cad_lancamentos --foreign-keys
serialize-db audit --metadata pipeline.models:Base.metadata --table cad_lancamentos --sql
```

Depois: o veredito de cada verificação, com as amostras; a saída é 1 quando alguma reprova. Quando
o banco recusa a ingestão pelo DDL do modelo, como no nulo numa coluna `NOT NULL` ou no JSON
malformado, o comando imprime `ingestão: reprovada (<erro do banco>)`, sem as outras contagens, e
sai com 1.

### Histórico de uma tabela

`serialize-db history` lista os commits de uma tabela com os metadados da biblioteca:

```shell
serialize-db history --root s3://bucket/projeto/delta --environment prd \
    --metadata pipeline.models:Base.metadata --table cad_lancamentos
```

Depois: uma linha por commit, do mais recente ao mais antigo, com a versão, a operação, o instante
em UTC e, quando o commit os tem, `serialize_db_execution_id`, `serialize_db_input_versions` e
`serialize_db_snapshot`. Só os commits das partições de uma execução e os da carga têm esses
metadados (a carga com `carga-<uuid8>` e `{}` nas versões lidas, sem snapshot). Os outros commits
vêm sem eles: a criação da tabela, a reconciliação do esquema, a reescrita, a cópia do arquivo, o
`restore` de uma releitura reprovada, o `vacuum` (`VACUUM START`, `VACUUM END`) e o `OPTIMIZE`.

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
| `--root` | `SERIALIZE_DB_ROOT` | A raiz das tabelas Delta, pasta local ou `s3://bucket/prefixo`; obrigatória sem a variável. Uma raiz fora dos armazenamentos da biblioteca, ou no S3 sem região, é erro de uso em todo subcomando. |
| `--environment` | `SERIALIZE_DB_ENVIRONMENT`, senão `dsv`; a variável vazia conta como ausente | O ambiente, a pasta sob a raiz: cada tabela fica em `<raiz>/<ambiente>/<tabela>`, e em `publish_redshift` a tabela publicada se chama `<ambiente>_<tabela>`. |

`run`, `import` e as rotinas de operação (`snapshot`, `channel`, `vacuum`, `compact`, `archive`,
`export` e `history`) recebem as três. `audit` e `publish_redshift` também as recebem, com `--root`
e, em `publish_redshift`, `--metadata` dispensáveis nos casos descritos nas seções deles. `schema`
e `sql` recebem só `--metadata`.

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
| `--partition` | nenhum | O valor da partição da execução. Sem ela, a execução não tem partição e `run.partition` é `None`. |
| `--engine` | `SERIALIZE_DB_ENGINE`, senão `duckdb` | O motor do sandbox: `duckdb` ou `redshift`. |
| `--execution-id` | `exec-<AAAA-MM-DD>-<uuid8>`, com a data em UTC | O identificador da execução. Ele é gravado em `serialize_db_execution_id` nos metadados de cada commit dela, no nome dos arquivos que o motor grava e no nome da pasta `staging/<execution_id>/` do motor Redshift. |

### `audit`

| Opção | Padrão | Descrição |
| --- | --- | --- |
| `--table` | obrigatória | A tabela do modelo auditada. |
| `--partitions` | todas | As partições auditadas; sem ela, a tabela inteira. Numa tabela sem partição, a opção é erro de uso. |
| `--foreign-keys` | desligada | Confere as chaves estrangeiras contra a versão atual de cada tabela referenciada. |
| `--key-scope` | nenhum | `partition` suprime a verificação da chave contra a versão atual do Delta; `table` a faz também na chave com a coluna de `partition_source`. |
| `--engine` | `SERIALIZE_DB_ENGINE`, senão `duckdb` | O dialeto do texto e o motor da auditoria: `duckdb` ou `redshift`. |
| `--sql` | desligada | Imprime o texto das verificações, sem conexão nem armazenamento. |
| `--root` | `SERIALIZE_DB_ROOT` | Obrigatória sem `--sql`. |

### `import`

| Opção | Padrão | Descrição |
| --- | --- | --- |
| `--source` | obrigatória | A raiz da base Parquet de origem, pasta local ou `s3://bucket/prefixo`, com uma pasta por tabela, `<origem>/<tabela>/`, e as partições em `<coluna>=<valor>/`. A carga só a lê, e grava em `<raiz>/<ambiente>/<tabela>`. |
| `--tables` | todas do modelo | As tabelas carregadas, na ordem da carga: as sem partição antes das particionadas. |
| `--partitions` | todas | As partições carregadas e conferidas no relatório. Toda tabela particionada da carga precisa tê-las na origem: uma que falta é recusada antes de qualquer gravação, com a saída 1. Com a opção, a tabela sem partição fica inteira de fora, sem carga nem relatório, numa linha da saída. |
| `--ignore-partitions` | nenhuma | As partições deixadas fora da carga e do relatório em toda tabela particionada que as tem, cada uma numa linha da saída. A tabela sem partição entra inteira, e um valor que a origem não tem não é recusado. Não combina com `--partitions`: as duas juntas são erro de uso, com a saída 2. |

### `publish_redshift`

| Opção | Padrão | Descrição |
| --- | --- | --- |
| `--init` | desligada | Cria a tabela de controle `serialize_db_publications`, uma vez; dispensa `--metadata` e `--root`. |
| `--status` | desligada | Mostra a versão publicada, a atual e as partições pendentes de cada tabela do modelo que existe no ambiente. |
| `--unpublish` | desligada | Despublica as tabelas: `DROP TABLE` e a linha de controle. |
| `--snapshot` | nenhum | O snapshot publicado, pelo nome, registrado em `snapshots`; o arquivado é erro de uso. |
| `--channel` | nenhum | O canal cujo snapshot é publicado: `default`, ou `current`, a versão atual de cada tabela do modelo que existe no ambiente. |
| `--tables` | todas do modelo | As tabelas publicadas ou despublicadas. A tabela do modelo sem versão no snapshot é erro de uso, e a opção a deixa de fora. |
| `--max-workers` | 1 | As tabelas publicadas ao mesmo tempo, cada uma na sua conexão. |
| `--execution-id` | `publicacao-<AAAA-MM-DD>-<uuid8>`, com a data em UTC | O identificador gravado na linha de controle. Ele dá a pasta dos manifestos do `COPY`, `<raiz>/<ambiente>/publicacao/<execution_id>/`. |
| `--metadata`, `--root` | obrigatórias | Dispensadas por `--init`. |

`--init`, `--status` e `--unpublish` valem nesta ordem e não recebem `--snapshot` nem
`--channel`. Sem nenhuma delas, o comando publica as versões de `--snapshot` ou de `--channel`:
um dos dois é obrigatório, e os dois juntos são erro de uso.

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
| `--apply` | desligada | Apaga os arquivos listados; sem ela, o comando só lista. |
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
| `--name` | obrigatória | O snapshot arquivado, registrado em `snapshots` e apontado por nenhum canal, o que o comando confere e recusa. A cópia vai para `<raiz>/<ambiente>/arquivo/<nome>/<tabela>`. |

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
