# Modelagem da biblioteca serialize-db

A biblioteca mantém um banco analítico como um conjunto de tabelas Delta Lake em pastas, no S3 do
projeto ou em disco local, com os modelos SQLAlchemy como contrato de esquema. Ela leva ao DuckDB ou
ao Redshift só as partições que uma execução do pipeline precisa, publica o resultado de volta no Delta
e carrega no Redshift as tabelas que os clientes consultam. Este documento reúne a modelagem: o que
a biblioteca faz, os metadados que ela mantém, as primitivas de cada módulo e o fluxo de cada caso
de uso. As razões do desenho estão em [`estrategia.md`](estrategia.md); o comportamento verificado
do Delta, em [`delta.md`](delta.md); os motores, em [`duckdb.md`](duckdb.md) e
[`redshift.md`](redshift.md); o contrato, em [`schema.md`](schema.md) e
[`sqlalchemy.md`](sqlalchemy.md); as práticas de ETL que o desenho segue, em [`guia.md`](guia.md); as
etapas de implementação, em [`PLAN.md`](PLAN.md), e o plano de cada etapa, com as primitivas do
módulo, em `PLAN-STAGE-<n>.md`.

## Funcionalidades

- **Contrato de esquema a partir dos modelos.** Os modelos declarativos do SQLAlchemy definem cada
  tabela, e deles a biblioteca deriva o esquema Arrow, o esquema Delta e o DDL do sandbox nos dois
  motores. As opções físicas, a coluna de partição com a data de que ela deriva, chave de ordenação e
  distribuição no Redshift, ficam em `Table.info["serialize_db"]`. Os arquivos de esquema gerados são versionados no
  repositório do pipeline e comparados por teste.
- **Statements Core executados pelo motor, e o texto SQL como opção.** O pipeline submete cada
  statement Core a `run.sandbox.query` ou `stream`, que o compila pelo dialeto com os
  parâmetros do cliente; o mesmo statement roda num `sqlalchemy.Connection` criado fora da
  biblioteca (decisão do usuário de 2026-09-22). Para um pipeline que queira sair do SQLAlchemy,
  cada statement vira texto SQL do DuckDB e do Redshift, com as constantes embutidas, a partição
  como parâmetro nomeado e o prefixo do sandbox como sentinela, versionado no repositório do
  pipeline e executado no lugar da compilação, uma interação por vez
  ([`sqlalchemy.md`](sqlalchemy.md)).
- **Banco em tabelas Delta.** Uma pasta por ambiente e uma subpasta por tabela. A biblioteca cria
  cada tabela a partir do contrato, de forma idempotente, e reconcilia o esquema da tabela com o
  modelo: o diff aditivo é aplicado, o destrutivo exige a reescrita explícita.
- **Ingestão seletiva.** Cada execução fixa a versão de cada tabela lida e leva ao motor só as partições
  que o pipeline usa: views ou tabelas materializadas no DuckDB, `COPY ... MANIFEST` no Redshift.
- **Paralelismo em cada comando, nas threads da biblioteca e no código cliente.** A API é síncrona
  e as primitivas podem ser chamadas de qualquer thread; cada motor tem uma sessão por execução sob
  um lock reentrante, e cada comando usa o paralelismo do motor. Os dados cruzam a fronteira em
  lotes `RecordBatch`: `stream` lê o lote seguinte e `loader` grava o anterior enquanto o cliente
  trabalha no atual, e `query` e `load` são as formas por `pa.Table`; `publish` e
  `publish_redshift` aceitam `max_workers`. A seção "Paralelismo" diz como operar em cada cenário.
- **Restrições aplicadas por consulta.** Nem o Parquet nem o Delta têm chave primária, unicidade ou
  chave estrangeira, e o Redshift só as registra. A auditoria da execução as aplica com consultas
  derivadas dos próprios modelos, e o texto SQL de cada verificação pode ser impresso ou gravado,
  para depuração.
- **Execução com sandbox, auditoria e publicação.** O pipeline roda num sandbox por execução. A
  auditoria reprova sem tocar o Delta, e a publicação a exige aprovada. A publicação substitui partições inteiras, um commit por partição,
  com `serialize_db_execution_id` e `serialize_db_input_versions` nos metadados. A reexecução é
  idempotente, e o conflito entre duas execuções do mesmo ambiente aborta a segunda.
- **Publicação para clientes no Redshift.** A diferença entre a versão publicada e a atual diz quais
  partições recarregar. Todas as tabelas da execução entram numa única transação, e a tabela de controle
  guarda a versão publicada de cada uma.
- **Snapshots do banco e manutenção.** O conjunto `{tabela: versão}` marcado na periodicidade do
  processo, o `vacuum` que preserva essas versões, a compactação antes do snapshot e a cópia
  profunda para a pasta de arquivo.
- **Entrada e saída em Parquet.** A carga inicial dos Parquet atuais e a exportação de um snapshot
  para pastas Parquet por partição, para o Hive ou para sair do Delta.
- **Linha de comando e documentação.** `serialize-db run` executa o pipeline, e o `pdoc` gera a
  documentação da API.

Os módulos são `serialize_db.errors`, `serialize_db.schema`, `serialize_db.sql`, `serialize_db.storage`,
`serialize_db.delta`, `serialize_db.audit`, `serialize_db.engine.duckdb`, `serialize_db.engine.redshift`,
`serialize_db.execution`, `serialize_db.load`, `serialize_db.publication` e `serialize_db.cli`; o pacote não
contém modelos, que vêm da biblioteca cliente. O modelo de referência em `tests/reference_model/`, o
modelo SQLAlchemy da base original, fica como está; o modelo cliente, a sua cópia corrigida em
`tests/client_model/`, é a primeira instância do contrato e o material dos testes,
que o entregam à API como um pipeline entregaria os seus modelos. As etapas de implementação, com o critério de aceite de cada uma, estão
em [`PLAN.md`](PLAN.md), e as primitivas de cada etapa, em `PLAN-STAGE-<n>.md`.

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

O log de cada tabela guarda tudo o que é da tabela: os arquivos de cada versão, o nome e o
comentário da tabela na `description` da ação `metaData`, o esquema de cada versão com os
comentários de coluna, as estatísticas por arquivo e os metadados que a biblioteca
grava em cada commit (`serialize_db_execution_id`, `serialize_db_input_versions` e, quando houver,
`serialize_db_snapshot`). O modelo SQLAlchemy dá o DDL e os tipos do contrato atual, e a
reconciliação descrita em [`delta.md`](delta.md), seção "Evolução de esquema", garante que ele e o
esquema atual do log são o mesmo. A biblioteca não guarda cópia de esquema, lista de arquivos nem
estatísticas.

O que ela guarda por conta própria fica em `_serialize_db/`, na raiz do ambiente, ao lado das pastas
das tabelas: `snapshots.json`, com
`{"snapshots": {"2026T3": {"cad_lancamentos": 143, "cad_contratos": 88}}}`. O sublinhado inicial
deixa a pasta fora dos globs `<coluna>=*` e dos leitores no estilo Hive, que ignoram nomes com esse
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
| Metadados de commit | `commitInfo` de cada commit da biblioteca. | `serialize_db_execution_id`; `serialize_db_input_versions`, o JSON `{tabela: versão}` das versões lidas, fixado na abertura da execução; `serialize_db_snapshot` só na execução que marca um snapshot. | `publish_partition` e `register_files`, por `CommitProperties(custom_metadata=...)`. |
| Arquivo de controle | `<ambiente>/_serialize_db/snapshots.json`. | `{"snapshots": {nome: {tabela: versão}}}`, com todas as tabelas do ambiente, lidas ou gravadas, e `"archived"` com as entradas dos snapshots arquivados, no mesmo formato ([etapa 9](PLAN-STAGE-9.md)). | `snapshot`, com `IfMatch`; `archive` move a entrada. |
| Tabela de controle | `serialize_db_publications(table_name, delta_version, execution_id, published_at)` no esquema do Redshift; `table_name` leva o prefixo do ambiente, como `prod_cad_lancamentos`. | Versão do Delta carregada em cada tabela publicada. | `publish_redshift`, na transação da carga; a tabela é criada uma vez pelo usuário, por `create_publications_table`. |

O registro durável de uma execução é o `commitInfo` das tabelas que ela gravou; o relatório da
auditoria e o resumo da execução vão para o log do processo, não para `_serialize_db/`.

## Primitivas

As primitivas de cada módulo, com assinatura, comportamento e testes, estão em `PLAN-STAGE-<n>.md`,
um arquivo por etapa, que [`PLAN.md`](PLAN.md) indexa; os fluxos abaixo as citam pelo nome. `table`
é sempre um `Table` do SQLAlchemy, obtido do modelo; `uri` é a pasta da tabela Delta; `data` é o que o
código cliente entrega a `load`, uma `pa.Table`, um `pa.RecordBatch`, um `RecordBatchReader` ou um
iterável de lotes, ou um `RecordBatchReader` nas primitivas internas; o cliente lê lotes por `stream`
e grava por `loader`; `run` é a `Execution` aberta, e `run.sandbox` o motor onde o pipeline roda.

## Fluxos de uso

### Carga inicial dos Parquet atuais

Uma passagem por tabela e por partição, reexecutável, que termina com os leitores apontados para o
Delta. A origem é a base de `data_str=<AAAA-MM-DD>/chunk_<n>.parquet` lida em 2026-09-20
([`PLAN-STAGE-7.md`](PLAN-STAGE-7.md)).

1. `create_table(uri, table)` para cada modelo, na pasta do ambiente.
2. Para cada partição da origem, o DuckDB ou o PyArrow lê os Parquet da pasta, a coluna de partição
   recebe o valor do caminho, as chaves passam de `int32` a `int64`, o `timestamp` `INT96` é
   truncado a microssegundos, as colunas `double` entram como estão, `cast` converte para o
   contrato e a partição entra conforme `export_mode`: `register` registra por `register_files` o
   arquivo do `COPY ... (RETURN_STATS)` do DuckDB, `rewrite` grava por `publish_partition` a partir
   do leitor, sem a tabela inteira na memória ([`PLAN-STAGE-7.md`](PLAN-STAGE-7.md)). Uma
   carga interrompida recomeça da partição seguinte à última publicada.
3. O relatório compara contagens e somas por partição entre a origem e o Delta; a carga só termina
   quando os dois coincidem.
4. Os leitores passam a abrir o Delta, e as pastas de origem ficam como cópia até a primeira
   publicação no Redshift.

`alembic_version`, `meta_update_status` e `schema.json` ficam fora da carga. `convert_to_deltalake`
registra os arquivos no lugar, sem reescrever, só quando eles já têm os tipos, a ordem de colunas e
o layout Hive do contrato; a origem tem o layout e não os tipos, e ele não é o caminho.

### Execução mensal no DuckDB

O exemplo ilustrado, com versões e artefatos de cada passo, está em [`PLAN.md`](PLAN.md).

1. `Execution` abre cada tabela de entrada e fixa `versions`; toda leitura da execução usa essas
   versões, mesmo que outra execução publique no meio.
2. `run.ingest` cria as views com os nomes dos modelos sobre `delta_scan` na versão fixada, e
   materializa as tabelas consultadas muitas vezes com as partições pedidas.
3. O pipeline roda em `run.sandbox`; o que sai para o Python sai em lotes por `stream`, ou como
   `pa.Table` por `query`, e volta por `loader` ou `load`; os intermediários ficam no
   sandbox, não no Delta. O nome de uma tabela no sandbox é do `ingest` ou do `loader`, nunca dos
   dois: a tabela que a execução grava é lida na versão publicada por `run.published(table)`.
4. `run.audit` reprova e encerra sem tocar o Delta, ou aprova.
5. `run.publish` reconcilia o esquema, substitui cada partição num commit (`export_mode`: `register`
   registra o arquivo do `COPY ... (RETURN_STATS)` depois das conferências da
   [etapa 3](PLAN-STAGE-3.md), `rewrite` grava pelo `write_deltalake`) com
   `serialize_db_execution_id` e `serialize_db_input_versions`, e avança `versions[table]`. Um
   `CommitFailedError` na mesma partição significa outra execução publicando a mesma tabela, e a
   execução aborta; ela também aborta quando a versão da tabela avançou desde a abertura, para que
   duas execuções abertas na mesma versão não publiquem a mesma faixa de identificadores.
6. `run.publish_redshift` carrega as partições alteradas de todas as tabelas numa transação.
7. No encerramento, o sandbox é descartado e o resumo vai para o log. Repetir a execução com o
   mesmo `execution_id` repete os mesmos `overwrite` e produz as mesmas linhas; os identificadores
   podem diferir, porque `run.next_ids` recomeça do máximo da versão fixada.

### Execução no Redshift

O mesmo ciclo, com o motor Redshift; o que muda é onde os dados ficam.

1. O sandbox são tabelas `exec_<id>_<tabela>` no esquema único, criadas pelo DDL do contrato.
2. `run.ingest` monta o manifesto dos arquivos das partições pedidas, na versão fixada, e carrega por
   `COPY ... MANIFEST` na staging sem a coluna de partição, seguido de `INSERT ... SELECT *, '<valor>'`. A carga de
   arquivos anteriores a uma coluna nova vai por lista de colunas, confirmada em 2026-09-21, ou por
   `FILLRECORD`, que carregou o mesmo arquivo com a coluna nova nula, o caminho de todo `COPY` da
   biblioteca (decisão do usuário de 2026-09-23, [etapa 8](PLAN-STAGE-8.md)).
3. O pipeline roda os mesmos statements Core, compilados para o Redshift, numa sessão só; os lotes
   entram por Parquet em `staging/` mais `COPY`, um row group por lote, e saem por `UNLOAD` em
   `stream` e das tuplas do cursor em `query`.
4. `run.audit` roda as mesmas consultas no Redshift.
5. `run.publish` grava cada partição por `UNLOAD ... MANIFEST VERBOSE`, sem `PARTITION BY`, num
   prefixo novo por tentativa, `<coluna>=<valor>/<execution_id>_<uuid>/`, e, conforme
   `export_mode`, registra os arquivos por `register_files` depois das conferências da
   [etapa 3](PLAN-STAGE-3.md) (`register`: os dados não passam pela máquina local) ou os relê pelo
   leitor da [etapa 7](PLAN-STAGE-7.md) e grava por `publish_partition` (`rewrite`).
6. `run.publish_redshift` carrega as tabelas `prod_*` a partir do Delta, pelo mesmo caminho da
   execução no DuckDB, e `cleanup` apaga as tabelas do sandbox e o staging.

### Publicação para clientes no Redshift

As tabelas publicadas têm o prefixo do ambiente e são derivadas do Delta; nada é escrito nelas por
outro caminho. A tabela `serialize_db_publications` é criada uma vez no esquema pelo usuário, por
`serialize-db publish --init`, e a publicação para sem escrever nada quando ela não existe
([etapa 8](PLAN-STAGE-8.md), decisão do usuário de 2026-09-23).

1. `version_diff` compara, para cada tabela, a versão em `serialize_db_publications` com a versão
   atual e devolve as partições com arquivos novos. Na primeira publicação, todas as partições.
2. A reconciliação repete no Redshift o diff aditivo do Delta, `ALTER TABLE ADD COLUMN` no fim da
   tabela, porque o `COPY` é posicional; um diff destrutivo recria a tabela e recarrega tudo.
3. Numa única transação, para cada tabela e partição: `DELETE` da partição, `COPY ... MANIFEST` na
   staging, `INSERT ... SELECT *, '<valor>'` e a linha de `serialize_db_publications`. A transação dá aos
   clientes a atomicidade entre tabelas que o Delta não tem.
4. Uma execução de correção publica só a partição corrigida.

### Correção de uma partição

1. A mesma `Execution`, com a partição a corrigir e um `execution_id` novo.
2. `run.publish` substitui a partição nas tabelas afetadas; a versão anterior continua legível até o
   `vacuum`, dentro dos 400 dias de retenção.
3. `run.publish_redshift` recarrega só essa partição.
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
   predicado, porque o `overwrite` de uma partição com `schema_mode="overwrite"` troca o esquema da
   tabela toda e deixa as outras partições lendo nulo ([`delta.md`](delta.md), seção "Evolução de esquema").
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
   `arquivo/<nome>/<tabela>/`, a entrada passa de `snapshots` para `archived` no mesmo arquivo, e a
   pasta de arquivo recebe a regra de ciclo de vida para a classe de armazenamento mais barata.

### Exportação para pastas Parquet por partição

Para publicar no Hive ou para sair do Delta.

1. O snapshot atual usa `export_snapshot(uri, destination)`: `mode="copy"` copia os arquivos que
   `get_add_actions()` lista, já no layout `<coluna>=<valor>/part-....parquet`, sem ler dados; no S3, um
   `CopyObject` por arquivo. Serve quando os leitores casam colunas por nome ou quando nenhum
   `ADD COLUMN` aconteceu desde a última reescrita de todas as partições.
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
2. Um ambiente de desenvolvimento nasce de uma cópia de produção: `Database(root, environment="dev", metadata=Base.metadata)`
   aponta para a pasta copiada, e as tabelas publicadas levam o prefixo `dev_`.
3. Execuções de ambientes diferentes não conflitam, porque gravam tabelas diferentes; a concorrência
   que resta é entre execuções do mesmo ambiente, que o log serializa.

### Substituição do dialeto em tempo de execução

A opção de migração para fora do SQLAlchemy, não o caminho padrão (decisão do usuário de
2026-09-22).

1. O pipeline escolhe uma interação com o banco: um statement Core que hoje é compilado pelo
   dialeto a cada execução, com a partição como parâmetro (`param("mes")` no exemplo de `sqlalchemy.md`).
2. `write_sql_files({"total_por_cliente": statement}, metadata, "sql/")` grava
   `sql/total_por_cliente.duckdb.sql` e `.redshift.sql`, com as constantes embutidas, `:mes` e o
   sentinela `{prefix}`; os arquivos entram no repositório do pipeline e no diff da revisão.
3. A chamada troca `run.sandbox.query(statement)` por `run.sandbox.query(sql, {"mes": run.partition})`,
   com o texto lido por `read_sql(..., prefix=...)`, que troca o sentinela pelo prefixo informado,
   obrigatório (decisão do usuário de 2026-09-21), e `bind` adapta os marcadores ao motor.
4. Enquanto o statement Core existir, o teste que regenera os arquivos e os compara com os
   versionados acusa uma mudança de modelo. Quando o statement sair, o texto é a fonte, mantido à
   mão e validado por `qualify` do SQLGlot contra o contrato.
5. Com toda interação em texto, o pipeline importa o SQLAlchemy só para os modelos;
   `duckdb_engine` e `sqlalchemy-redshift` continuam dependências de execução da biblioteca, porque
   os motores compilam pelo dialeto o statement Core que recebem (decisão do usuário de
   2026-09-21); consulta nova nasce
   em texto, no dialeto do DuckDB, com os testes nos dois motores ([`estrategia.md`](estrategia.md)).

## Paralelismo

A biblioteca não tem scheduler, grafo de tarefas nem API assíncrona: nenhum dos quatro drivers
(`duckdb`, `deltalake`, `redshift_connector`, `boto3`) tem API assíncrona em Python, e um `async`
na biblioteca seria uma thread por dentro. O paralelismo vem de três lugares: de cada comando, que o
motor paraleliza (as `threads` do DuckDB, as slices do Redshift) e que cresce com a máquina; das
threads da biblioteca, que leem e gravam lotes e publicam tabelas fora da sessão; e do código
cliente, com `concurrent.futures`, sobre primitivas que podem ser chamadas de qualquer thread. O que
a troca por lotes, o `cast` e o GIL fixam está em [`PLAN.md`](PLAN.md), seção "A troca de dados com
o código cliente"; as medições, em `tests/proof_of_concept/test_concurrency.py` e `test_parallel.py`,
e os exemplos do Redshift em `test_redshift.py`.

### O que a biblioteca garante

1. Toda primitiva é síncrona: quando ela retorna, o efeito está visível para a chamada seguinte, de
   qualquer thread, porque os dois motores confirmam cada comando ao terminar.
2. Cada motor tem uma sessão por execução e um lock reentrante, e todo comando do sandbox passa por
   ela em série (decisão do usuário de 2026-09-22): uma tabela temporária vale para os comandos
   seguintes, de qualquer thread, nos dois motores. As primitivas tomam e soltam o lock, e nenhuma
   espera pelo código do cliente com ele tomado; `with run.sandbox.session() as connection:` dá a
   conexão crua ao que elas não cobrem, e `with run.sandbox.new_session() as other:` abre uma sessão
   a mais para o que roda em paralelo, sem as tabelas temporárias da principal. O cliente nunca cria
   conexão para o sandbox.
3. `run.next_ids(table, n)` devolve faixas de identificadores que não se sobrepõem entre threads.
4. O estado da execução (`versions`, auditorias aprovadas, o alocador) fica sob lock.
5. O DuckDB, o delta-rs e o PyArrow liberam o GIL no trabalho nativo: threads Python bastam para o
   paralelismo, e o custo de uma extensão em Rust não se justifica por ele.
6. As threads da biblioteca são os pools de `publish`, de `publish_redshift` e de `ingest`, este
   com uma sessão a mais por tabela, e a auxiliar de cada `stream` e de cada `loader`, encerrada no
   `close`: a de `stream` roda a consulta e entrega cada lote à memória, até um orçamento de
   64 MiB, ou a um arquivo intermediário depois dele, enquanto o cliente lê os lotes já entregues, e
   a de `loader` grava os lotes num arquivo fora da sessão, que o `close` carrega numa tabela criada
   ali. O cliente trabalha no lote atual enquanto a consulta produz o seguinte ou a biblioteca grava
   o anterior, também com o `loader` aberto depois do `stream`, porque a abertura dele não usa a
   sessão, e o `close` de um stream cancela a consulta que ainda roda (decisões do usuário de
   2026-09-23).

### Leituras em paralelo

- **Delta.** São objetos no armazenamento mais o log, sem sessão nem bloqueio: leitores ilimitados,
  cada um preso à versão que carregou, mesmo que um commit entre no meio. O delta-rs lê os arquivos
  de uma tabela em paralelo por conta própria, e um pool do cliente sobre
  `DeltaTable(uri).to_pyarrow_dataset()` lê várias tabelas; o ganho aparece no S3, onde a latência
  domina.
- **O sandbox.** Um comando por vez em cada sessão, cada um com o paralelismo do motor. No DuckDB,
  o pool `threads` é da instância e muda em execução por `SET threads`; duas consultas simultâneas o
  dividem, cada uma também com a thread que a chamou, e ganham quando uma espera o S3, quando as
  consultas são pequenas ou quando um operador não se paraleliza (as medições de 2026-09-23 estão
  em [`duckdb.md`](duckdb.md)); no Redshift, cada comando corre nas slices.
  Várias threads chamam `run.sandbox.query` e `stream` ao mesmo tempo e esperam a vez na
  sessão, e a leitura dos lotes de cada `stream` corre fora dela. A ingestão de várias tabelas corre
  em paralelo, uma sessão a mais por tabela: em disco local, quatro tabelas de 150.000 linhas
  entraram em 0,017 s assim e em 0,066 s em série (`test_parallel.py`), e quatro de 8.000.000 de
  linhas em 1,629 s contra 3,498 s com `threads = 2` e em 1,037 s contra 1,382 s com 11, num macOS
  de 11 núcleos em que as threads que chamam as sessões somam núcleos ao pool; com 2 vCPUs, o ganho
  fica na espera do S3, ainda não medida (2026-09-23, [`POC.md`](POC.md)). O cliente abre
  as suas sessões a mais para as consultas independentes, que não veem as tabelas temporárias da
  sessão principal.

### Escritas em paralelo

- **Publicação no Delta.** `run.publish(*tables, partitions, max_workers=n)` grava as tabelas em paralelo:
  cada tabela tem o seu log, e partições distintas da mesma tabela entram em commits distintos. A
  parte de cada tabela que usa a sessão, o `COPY` da partição, corre em série; o commit, as
  conferências de `register_files` e a releitura correm em paralelo. O limite é a memória por
  escrita, não a CPU: o padrão é 1, e as tabelas grandes saem pelo `COPY` do DuckDB mais
  `create_write_transaction`, com memória constante. Na primeira falha, as tarefas em curso
  terminam, as não iniciadas são canceladas, e a exceção lista o resultado por tabela; os commits
  feitos ficam, porque o Delta não tem transação entre tabelas, e a reexecução repete só o que faltou.
  Dois escritores na mesma partição da mesma tabela conflitam: o segundo recebe `ExecutionConflict`, e é o
  sinal de duas execuções no mesmo ambiente.
- **Escritas no motor com dependências.** Um passo posterior que lê o que um passo anterior gravou
  espera o `Future` desse passo; a dependência é do fluxo de controle do cliente, não da biblioteca.
  Passos independentes podem ir para um pool: os comandos deles correm em série na sessão, e o ganho
  é o trabalho Python de cada passo, que corre fora dela; numa sessão a mais por passo, quando o
  passo não usa as tabelas temporárias da principal, os comandos também correm juntos. Cada `load` e cada `query` gravam
  tabelas distintas, e cada comando é confirmado ao terminar, então nada fica meio gravado para a
  leitura seguinte.
- **Publicação no Redshift.** `run.publish_redshift(*tables, max_workers=n)`: um `COPY` por tabela,
  uma conexão por tabela, fora da sessão do sandbox, limitados pelas slots do WLM. Dois `COPY` na
  mesma tabela serializam.

```python
from concurrent.futures import ThreadPoolExecutor

with Execution(db, engine="duckdb", partition="2026-08-31", execution_id="exec-2026-09-05") as run:
    run.ingest(Lancamento, Contrato, Operacao, RelContratoOperacao, partitions=run.previous_partitions(Lancamento, 12))

    with ThreadPoolExecutor(max_workers=2) as pool:                 # dois passos independentes; os comandos correm em série na sessão
        saldos = pool.submit(run.sandbox.query, SALDOS_SQL, {"data_base_str": run.partition})       # grava {prefix}saldos
        limites = pool.submit(run.sandbox.query, LIMITES_SQL, {"data_base_str": run.partition})     # grava {prefix}limites
        saldos.result()                                              # a leitura abaixo depende dos dois
        limites.result()

    statement = select(Saldo).where(Saldo.data_base_str == run.partition)   # já vê saldos e limites
    with run.sandbox.stream(statement) as stream, run.sandbox.loader(LancamentoProjetado) as loader:
        for batch in stream:                                         # a consulta segue gravando os lotes seguintes
            frame = batch.to_pandas(types_mapper=pd.ArrowDtype)
            frame["id_lancamento"] = run.next_ids(LancamentoProjetado, len(frame))   # faixa contígua, sob lock
            loader.write(pa.RecordBatch.from_pandas(frame, preserve_index=False))    # o lote anterior vai para o arquivo do loader
    run.audit(LancamentoProjetado, partitions=[run.partition])
    run.publish(LancamentoProjetado, Saldo, partitions=[run.partition], max_workers=2)   # duas tabelas, dois logs
```

### Identificadores

As chaves inteiras são geradas pelo cliente, nunca pelo motor: o DuckDB tem sequências e o Redshift
não, e `IDENTITY` no Redshift salta e não ordena em cargas paralelas. A regra de `run.next_ids` está
em [`PLAN.md`](PLAN.md), seção "Regras que as etapas obedecem". O código que hoje lê a chave depois
do `INSERT` passa a pedi-la antes do `load`, e as linhas filhas referenciam os ids do pai na mesma
tabela em memória.

