## Operação

O runbook das rotinas de operação do banco Delta, cada uma um subcomando de `serialize-db` sobre
as primitivas de `serialize_db.delta`, com o que conferir antes e o que esperar depois. Todos os
subcomandos recebem `--metadata modulo:atributo`, `--root` (`SERIALIZE_DB_ROOT`) e `--environment`
(`SERIALIZE_DB_ENVIRONMENT`, `dev`), e saem com 0 quando terminam e com 2 no erro de uso, no nome
repetido ou ausente e no conflito de escrita do arquivo de controle.

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
e `numFilesRemoved` impressos, um commit `OPTIMIZE` com `dataChange` falso, que
`serialize_db.delta.version_diff` não conta; uma partição com um só arquivo não commita.

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
mesmos arquivos e as mesmas somas; a entrada em `archived`, que `vacuum` não prende mais, e
`snapshot` recusando o nome, porque ele dá a pasta. Uma tabela já no arquivo é pulada, e o comando
se repete depois de uma interrupção.

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
todos; `--version` ausente é a versão atual.

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
`serialize_db_snapshot` quando o commit os tem; os commits de `vacuum` (`VACUUM START`,
`VACUUM END`) e de `OPTIMIZE` vêm sem eles.
