# Plano de implementação

Este documento registra o que a biblioteca é e como ela chega lá: as decisões, a troca de dados com
o código cliente, as regras que as etapas obedecem, a organização do pacote, as etapas de
implementação e o pipeline de atualização mensal. O plano de cada etapa, com as primitivas do
módulo, os testes e as provas de conceito que o exercitam, está num arquivo próprio, de
[`PLAN-STAGE-0.md`](PLAN-STAGE-0.md) a [`PLAN-STAGE-9.md`](PLAN-STAGE-9.md), que a seção "Etapas"
indexa. O estado da implementação, com
a situação de cada etapa e o que cada artefato contém, está em
[`CURRENT_STATE.md`](CURRENT_STATE.md); o resultado das provas de conceito, das suítes e dos probes,
com as consequências no plano, em [`POC.md`](POC.md); as pendências e as decisões em aberto, em
[`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md). As razões das decisões e as comparações entre ferramentas
estão em [`estrategia.md`](estrategia.md); o comportamento verificado do Delta, em
[`delta.md`](delta.md); os motores, em [`duckdb.md`](duckdb.md) e [`redshift.md`](redshift.md); o
esquema a partir dos modelos, em [`schema.md`](schema.md) e [`sqlalchemy.md`](sqlalchemy.md); as
funcionalidades, os metadados próprios e o fluxo de cada caso de uso, em
[`serialize-db.md`](serialize-db.md).

## Decisões

A biblioteca mantém um banco analítico como tabelas Delta Lake, gravadas e lidas pelo pacote
`deltalake` (delta-rs) sobre Parquet, no bucket do projeto ou numa pasta local. Os modelos
declarativos do SQLAlchemy são o contrato de esquema: deles saem o esquema Arrow, o esquema Delta e
o DDL do sandbox nos dois motores. Chave primária, unicidade e chave estrangeira não entram nesse
DDL, porque o Parquet não as tem, o DuckDB as cobra na carga e o Redshift só as registra: quem as
aplica é a auditoria da execução, com consultas derivadas dos mesmos modelos, e a reprovação impede
a publicação. Os statements Core do pipeline continuam válidos: o cliente os submete a `run.sandbox.query`
ou `stream`, que os compilam pelo dialeto do motor com os parâmetros dele e os executam
na conexão crua, e pode submetê-los a um `sqlalchemy.Connection` que ele mesmo crie fora da
biblioteca, porque o contrato não exige do modelo nem do statement nada que um `Connection` não
aceite (decisão do usuário de 2026-09-22). O texto SQL gerado por dialeto, com os parâmetros para
o bind posterior, é a opção para um pipeline que queira substituir o SQLAlchemy no futuro, não o
caminho padrão.
Os dados cruzam a fronteira da biblioteca em lotes `pyarrow.RecordBatch` (seção "A troca de dados
com o código cliente"). A evolução do esquema é uma reconciliação entre o modelo SQLAlchemy e o
log da tabela Delta Lake, sem Alembic.

As premissas, declaradas pelo usuário, e o que cada uma fixa:

- **O pipeline é majoritariamente lógica Python.** O SQLAlchemy define os modelos e gera `select` e
  `insert` que movem tabelas; nenhuma classe ORM é instanciada. O SQLAlchemy fica como contrato e
  Core; o SQLMesh e o dbt saem; o `insert(...)` com listas de linhas sai, porque no Redshift ele
  vira uma ida ao servidor por linha.
- **A troca de dados com o código cliente é por streaming de `pyarrow.RecordBatch`, nos dois
  sentidos, e a `pyarrow.Table` é aceita e devolvida por conveniência** (decisão de 2026-09-20); o
  pandas com backend pyarrow é o formato dos pipelines (declaração do usuário de 2026-09-20). A
  seção "A troca de dados com o código cliente" fixa a API, as medições e as regras.
- **O Redshift do ambiente alvo é serverless, e o esquema do projeto vem de um datashare**
  (decisão do usuário de 2026-09-20). A conexão é a credencial temporária do workgroup
  ([`../examples/redshift_native.py`](../examples/redshift_native.py), executado lá), e nenhum
  outro caminho de autenticação entra na biblioteca sem ter rodado no ambiente alvo. A conexão roda
  `USE datalake_rw_shared` e cita `sbx_aco_decon.<tabela>`, e confirma o `USE` resolvendo um
  nome em duas partes, porque `current_database()` continua a responder `dev` depois dele
  (leitura de 2026-09-21, confirmada pelo usuário no mesmo dia); o nome em três partes fica para
  uma sessão aberta em outro banco, como a Data API. A escrita segue o que um datashare aceita, com o
  `COPY` sem cláusula `COMPUPDATE` e transação explícita, e o `COPY` e o `UNLOAD` alcançam o S3
  pelas credenciais de quem chama, porque o namespace não tem papel IAM associado
  ([`redshift.md`](redshift.md)). A Data API fica fora da biblioteca: ela devolve `DECIMAL` e data e
  hora como texto e limita o resultado a 500 MB, o que não serve à troca de lotes Arrow.
- **Nenhum serviço de catálogo está habilitado.** A camada de tabela não depende de serviço, e o
  Delta atende sem código próprio. O Iceberg com catálogo em arquivo fica documentado em
  `estrategia.md` e volta à mesa se o Glue ou o S3 Tables forem habilitados; `probes/catalog.py`
  mede esse gatilho.
- **O Delta é a fonte da verdade depois da carga inicial.** A tabela é criada do modelo por
  `DeltaTable.create`, sem DDL em SQL; a evolução e o histórico ficam no log de cada tabela Delta Lake.
- **Execuções de desenvolvimento e de produção gravam tabelas separadas.** Um caminho por ambiente,
  `<raiz>/<ambiente>/<tabela>/`, e o prefixo do ambiente nas tabelas do Redshift: uma execução não
  toca as tabelas de outro ambiente. Dentro de um ambiente roda uma execução por vez; se duas se
  sobrepõem, o log de cada tabela ordena os commits e `publish` aborta a segunda com
  `ExecutionConflict` (seção "Regras que as etapas obedecem"). A exceção é a tabela de controle
  `serialize_db_publications` do Redshift, uma só para todos os ambientes: a transação da
  publicação lê a linha da tabela no início e a grava no fim, por `INSERT` na primeira publicação e
  por `UPDATE` condicionado à versão lida nas seguintes, e a publicação que perde a corrida sai com
  `ExecutionConflict` ([etapa 8](PLAN-STAGE-8.md), decisão do usuário de 2026-09-23).
- **Renomear ou remover colunas é raro.** A evolução é aditiva; o caso raro reescreve a tabela
  inteira num commit e recria a tabela publicada, sem esperar o column mapping do delta-rs.
- **Os dois armazenamentos são suportados.** Toda primitiva recebe a URI de uma pasta local ou de
  um prefixo S3; a pasta local é o ambiente dos testes e do desenvolvimento sem AWS.
- **Um teste só grava onde o usuário autorizou.** A variável de raiz de cada suíte é a
  autorização: sem ela a suíte é pulada, com ela o que impede a escrita é falha, e `pytest` sem
  variável não grava arquivo algum.
- **A partição é uma coluna de texto do modelo do cliente** (decisão de 2026-09-22, que generaliza
  a de 2026-09-20): `String(n)`, declarada em `Table.info["serialize_db"]` (`partition_by`
  `["data_str"]`), e cada valor é uma partição, a unidade de ingestão, auditoria, publicação e
  substituição; o valor serve de nome de pasta e de literal e segue a regra da partição,
  `[0-9A-Za-z][0-9A-Za-z_.-]*`, que também vale para o `execution_id` (decisão do usuário de
  2026-09-23), e a ordem de texto dos valores é a que `previous_partitions` devolve. Na base atual
  ela é a data em texto `AAAA-MM-DD` derivada de uma coluna de data por `strftime(<coluna de data>,
  '%Y-%m-%d')`, declarada em `partition_source` (`"data"`; em `cad_lancamentos`, `data_base_str` de
  `data_base`), e a biblioteca confere a derivação quando o modelo a declara. A biblioteca não
  fixa nome nem granularidade; `mes` nos exemplos de `delta.md`, `duckdb.md`, `parquet.md` e
  `sqlalchemy.md` é uma coluna de partição ilustrativa.
- **Toda coluna numérica da base de origem é `double`, e o modelo de referência a mantém `Double`**
  (decisão de 2026-09-20): sem arredondamento nem `Numeric` de precisão fixa. O pacote suporta
  `Numeric(p, s)` pela tabela de tipos da documentação ([`../docs/index.md`](../docs/index.md)), e
  a transição de `valor` para `Numeric(18, 2)`, mais adequada a dados contábeis, é uma melhoria
  futura, por `rewrite` da tabela com o `cast` que recusa o `double` fora da escala.
- **As chaves inteiras passam a `int64` na migração para o Delta** (decisão de 2026-09-20): a origem
  as tem em `int32`, com `id_lancamento` em 1.113.599.996 na base de desenvolvimento e em 952.517.158
  na de produção; o modelo cliente declara `BigInteger`
  nas chaves primárias inteiras e nas colunas que as referenciam, e a carga inicial faz o cast sem
  perda.
- **O `timestamp` em `INT96` da origem vira `INT64` na migração** (decisão de 2026-09-20): o formato
  Parquet marca o `INT96` como obsoleto (`parquet.thrift`: "deprecated, new Parquet writers should
  not write data in INT96"), e a precisão dos timestamps da origem não importa: a carga trunca a
  microssegundos, o `timestamp[us]` do contrato, e o delta-rs grava `INT64`. A regra vale para o que
  a biblioteca grava; o `UNLOAD` do Redshift grava `INT96` e a exportação o traz de volta, lido como
  `timestamp[us]` pelos dois leitores e sem estatística (2026-09-21, `redshift.md`).
- **A nulidade é a do modelo** (decisão de 2026-09-20): sete colunas de `cad_contratos` são
  anuláveis nos arquivos e `NOT NULL` no modelo, sem nulo nos dados; o `cast` da carga as recusa
  com nulo, e a regra só muda se a migração o mostrar.

As partes da biblioteca: o esquema a partir dos modelos; o SQL gerado por dialeto; a camada Delta
sobre os dois armazenamentos; a ingestão seletiva e o sandbox por execução em cada motor; a execução
com auditoria e publicação; a publicação para clientes no Redshift; a carga inicial dos Parquet
atuais; a operação (snapshots do banco, `vacuum`, compactação, arquivo). Fora da biblioteca ficam o
ORM para cargas linha a linha, as chaves estrangeiras `DEFERRABLE`, o `Identity`, os manifestos
próprios e o Alembic; o SQLGlot entra só no grupo `dev`, como teste de que o texto gerado para o
Redshift analisa (decisão do usuário de 2026-09-22).

## A troca de dados com o código cliente

Os dados cruzam a fronteira da biblioteca em lotes `pyarrow.RecordBatch`, nos dois sentidos, e a
`pyarrow.Table` é a forma conveniente do que cabe na memória, redirecionada para a mesma API de
lotes (decisão do usuário de 2026-09-20). O cliente trabalha no lote atual enquanto a biblioteca lê
o seguinte ou grava o anterior, cada primitiva numa thread auxiliar com uma fila limitada. O pandas
com backend pyarrow é o formato dos pipelines (declaração do usuário de 2026-09-20), e a regra
apoia-se na conversão barata, medida na subseção "A conversão para o pandas": 2,3 ms sem cópia
para 300.000 linhas, 1,4 ms para um lote de 100.000; o pandas não entra nas dependências de
execução. Cada motor tem uma sessão por execução sob um lock, e nenhum lock espera pelo código do
cliente, porque o que usa a sessão termina sem esperar por ele e os lotes esperam o cliente fora
dela. No DuckDB, a saída é cada lote de `to_arrow_reader()` entregue por uma thread que roda a
consulta sob o lock à memória, enquanto os lotes guardados cabem em 64 MiB, e a um arquivo Arrow IPC
com LZ4 depois disso, e o cliente lê os lotes enquanto a consulta continua; a entrada é um arquivo
Arrow IPC carregado no `close`, numa transação, com o `CREATE TABLE` e um único `INSERT ... BY NAME`
(decisões do usuário de 2026-09-23). No
Redshift, `stream` lê os arquivos Parquet de um `UNLOAD` no `staging/`, `query` lê as tuplas que o
driver materializa, e a entrada é Parquet no S3 mais `COPY ... MANIFEST` (decisões do usuário de
2026-09-23). O que roda em paralelo, sem as
tabelas temporárias da sessão, abre uma sessão a mais com `run.sandbox.new_session()`.

### A API

O pipeline lê com `run.sandbox.stream(statement_ou_texto, params, batch_size)`, que
devolve um `BatchStream` (iterável de `RecordBatch` com `schema`, `read_next_batch`, `read_all`,
`close`, gerenciador de contexto e `__arrow_c_stream__`), ou com
`run.sandbox.query(statement_ou_texto, params)`, que devolve a `pa.Table` de `stream(...).read_all()`
e roda também o comando sem resultado (decisão do usuário de 2026-09-23, que tirou `execute`); grava
com `with run.sandbox.loader(Modelo) as loader: loader.write(lote)`, ou com
`run.sandbox.load(Modelo, data)`, que aceita `pa.Table`, `RecordBatch`, `RecordBatchReader` ou
iterável de lotes e os passa ao mesmo `loader`. `query` compila o statement pelo dialeto, com os `bindparam` do
cliente e as constantes como parâmetros do driver, e o executa na conexão crua, sem `Session`;
`select(Lancamento)` é aceito como statement Core, e o mesmo statement roda num
`sqlalchemy.Connection` que o cliente crie fora da biblioteca. A forma por
tabela serve ao que cabe na memória e à lógica que precisa de todas as linhas: `query(statement)`
devolve a `pa.Table`, `to_pandas(types_mapper=pd.ArrowDtype)` a leva ao pandas, e
`load(Modelo, pa.Table.from_pandas(frame, preserve_index=False))` grava. O caminho de um resultado
até o Delta é `loader` ou `load`, `audit` e `publish`. Nenhuma primitiva pública recebe ou devolve
um DataFrame, uma lista de linhas ou uma instância ORM; a mensagem que recusa um DataFrame aponta
`pa.Table.from_pandas` e `pa.RecordBatch.from_pandas`. O nome de cada tabela no sandbox é de um
só dono: `loader` recusa com `SandboxError` um nome que o `ingest` ou outro `loader` já ocupou, e a
tabela que a execução grava é lida na versão publicada por `run.published(Modelo)`, que não cria
objeto no sandbox (decisões do usuário de 2026-09-22). Dentro da biblioteca, o que não cabe na
memória corre por `RecordBatchReader` (a troca do motor Redshift para `publish_partition` na
partição com `Double` não finito), cada um lendo o seu arquivo intermediário; o registro da
partição e a `rewrite` da tabela vão pelo `COPY ... (RETURN_STATS)` do DuckDB, sem passar pelo
Python. O exemplo de uso, `stream` e `loader` dentro de uma
execução, está na seção "Pipeline de atualização mensal".

### O que as sondagens fixaram em cada primitiva

As medições, com a data e o ambiente de cada uma, estão em [`POC.md`](POC.md) ("O que a fronteira
por lotes mostrou", de 2026-09-20, e "O que a sessão única mostrou", de 2026-09-22), e as asserções
em `test_duckdb.py`, `test_pyarrow.py` e `tests/test_engine_duckdb.py`. O que elas fixaram:

- **`stream` roda a consulta numa thread auxiliar, sob o lock, e entrega cada lote à memória
  enquanto os lotes guardados cabem em 64 MiB, e a um arquivo intermediário o lote que não cabe e os
  seguintes** (decisão do usuário de 2026-09-23); o cliente lê a memória e depois o arquivo, na sua
  thread, na ordem da consulta, enquanto ela continua. O leitor do DuckDB é esvaziado, sem erro, pelo
  comando seguinte na mesma conexão, e por isso a thread o consome inteiro antes de soltar o lock,
  sem esperar pelo cliente. O arquivo é Arrow IPC com LZ4, um terço do tamanho sem compressão. Numa
  consulta sem operador bloqueante sobre 20.000.000 de linhas, com 13.333.333 no resultado, o
  primeiro lote chegou em 4 ms, e o total foi 0,411 s sem trabalho do cliente, 0,673 s com 5 ms de
  Python puro por lote e 0,411 s com pandas, contra 0,622 s, 0,965 s e 0,641 s quando todo lote
  passava pelo arquivo com LZ4, e 0,399 s, 0,673 s e 0,408 s do leitor direto de um cursor próprio
  (2026-09-23, [`POC.md`](POC.md)). A memória do lado Python fica no orçamento mais um lote: com o
  cliente atrasado, 297 MB de pico com 96 lotes no arquivo; o arquivo sozinho manteve 10.000.000 de
  linhas em 94 MB, contra 83 MB do leitor direto e 322 MB da tabela inteira.
- **Dentro de `session()`, na mesma thread, a consulta roda na thread de quem chama**, porque a
  auxiliar esperaria o bloco, e o bloco o stream; o cliente lê o arquivo depois da consulta
  inteira. Toda espera por um lote tem prazo e confere o encerramento, a thread não referencia o
  stream, e `close` (ou o fim do `with`) cancela por `interrupt()` a consulta que ainda roda e apaga
  o arquivo, e o `cleanup` do motor cancela o comando em curso antes de fechar a conexão (decisão do
  usuário de 2026-09-23): o `close` depois do primeiro lote de uma varredura longa terminou em 7 a
  10 ms, e o `cleanup` com uma ordenação em curso, em 2 ms. A thread marca o fim da consulta ainda
  com o lock tomado, e o `interrupt()` do `close` nunca alcança o comando seguinte da sessão. Um
  stream abandonado é coletado, a consulta para no lote seguinte e o arquivo sai. O erro que a
  consulta encontra antes do primeiro lote chega ao cliente na construção, como `duckdb.Error` ou
  como `OSError` com a mensagem do DuckDB; o que ela encontra depois chega na leitura seguinte ao
  último lote entregue e nas que vêm depois dela, nunca em silêncio.
- **O streaming limita a memória do lado Python, não a do DuckDB.** A consulta roda sob
  `memory_limit` e `temp_directory`, e uma ordenação materializa o resultado antes do primeiro lote.
  O `memory_limit` e as `threads` saem do ambiente na abertura do motor, metade da memória que o
  processo ainda pode usar e as CPUs dele, porque a máquina muda de tamanho e parte das alocações
  do DuckDB foge do limite (instrução do usuário de 2026-09-24, [etapa 4](PLAN-STAGE-4.md)).
  A ordem dos lotes é a da consulta: sem `ORDER BY`, com `preserve_insertion_order = false`, é
  arbitrária.
- **O ganho do encadeamento é o trabalho do cliente escondido atrás da leitura e da escrita**,
  limitado pelo estágio mais lento: 1,25x com o trabalho em pandas por lote e 1,55x com um laço
  Python puro, em 6.000.000 de linhas. Ler um lote é uma chamada nativa longa, e a thread auxiliar
  paga no máximo um intervalo de troca do GIL por lote ao lado do laço Python do cliente.
- **`loader` grava os lotes num arquivo intermediário numa thread auxiliar e, no `close`, cria a
  tabela e os insere num único `INSERT ... BY NAME`, numa transação**, sob o lock, enquanto o
  cliente prepara o lote seguinte: `write`
  faz o `cast` do lote na thread do cliente, para o erro aparecer com o lote em mãos, e bloqueia
  quando a fila está cheia; nada existe antes do `close`, e uma exceção dentro do `with`, um lote
  recusado pelo `cast`, um erro do `INSERT` ou um loader abandonado não deixam tabela. O comando único
  é o caminho rápido: um `INSERT` por lote custa cerca de 3,5 ms, e num banco em arquivo o pipeline
  de três estágios sobre 3.000.000 de linhas levou 0,400 s na sessão única, contra 0,565 s com um
  cursor por stream e por loader e um `INSERT` por lote numa transação. A abertura só confere o
  nome, num cursor à parte e sem o lock, porque um `CREATE TABLE` sob o lock esperaria a consulta
  inteira de um `stream` aberto antes: em 20.000.000 de linhas, o primeiro lote chegava em 0,811 s,
  contra 0,006 s com a tabela criada no `close` (decisão do usuário de 2026-09-23).
- **O `INSERT` único sobre um leitor alimentado por gerador Python não é o caminho**, embora seja um
  comando só e atômico: o `arrow_scan` do DuckDB puxa o fluxo por uma thread de leitura antecipada do
  Arrow, que chama o gerador em outra thread além do que o comando consumiu e depois da falha, e essa
  thread, ainda chamando Python na saída do processo, pendurou o processo no destrutor do pool de
  threads do Arrow. O leitor do arquivo intermediário é nativo, sem Python no meio. É por isso que
  `BatchStream.__arrow_c_stream__` serve ao `write_deltalake` e ao `RecordBatchReader.from_stream`,
  não ao `register`.
- **O `cast` por lote é barreira de segurança, não só de contrato.** `RecordBatchReader.from_batches`
  não confere cada lote contra o esquema declarado, e o `arrow_scan` do DuckDB lê os buffers pelo
  esquema declarado: um lote com as colunas em outra ordem entrou sem erro com os bytes trocados, e
  uma coluna a mais ou a menos falha. O DuckDB não confere a nulidade do esquema Arrow; a coluna
  `NOT NULL` da tabela ele confere. `RecordBatch.cast` recusa o que `Table.cast` recusa: nulo em
  campo `nullable=False`, nomes fora de ordem e escala perdida.
- **As conversões do lote não copiam**: `to_batches`, `Table.from_batches`,
  `RecordBatch.to_pandas(types_mapper=pd.ArrowDtype)` e `RecordBatch.from_pandas` compartilham os
  buffers e mantêm os tipos do contrato, como na tabela inteira.
- **A lógica do cliente por lote é a lógica por linha.** Uma agregação, um `merge`, uma ordenação ou
  uma janela precisam de todas as linhas: vão para SQL no sandbox, onde o motor usa todos os
  núcleos, ou para a `pa.Table` de `query`. `run.next_ids(table, batch.num_rows)` dá a cada lote a
  sua faixa.
- **No Redshift**, `stream` vai sempre por `UNLOAD` para `staging/<execution_id>/`, sob o lock, na
  thread de quem chama, e a thread auxiliar lê os lotes dos arquivos por `ParquetFile.iter_batches`
  enquanto o cliente trabalha (decisão do usuário de 2026-09-23): o `redshift_connector` materializa
  o resultado inteiro no `execute` (leitura do código, 2026-09-21), e o motor não sabe o tamanho do
  resultado antes dele. `query` monta a `pa.Table` das tuplas do cursor por colunas, `zip(*linhas)` e
  `pa.array(coluna, type=...)`, um terço do tempo de `from_pylist` por dicionários em 200.000 linhas.
  O `loader` grava um row group por lote com `ParquetWriter.write_batch` em `staging/<execution_id>/`
  e faz o `COPY` no `close`, então nada entra antes dele, e `load` passa sempre por ele
  ([etapa 5](PLAN-STAGE-5.md)).

### A conversão para o pandas

A hipótese da regra, de que converter para pandas é barato, foi medida em 2026-09-20 em macOS arm64
com pandas 3.0.6, PyArrow 25.0.1 e DuckDB 1.5.5, sobre 300.000 linhas de `operacoes` com sete
colunas (`int64`, `date32`, `int64`, `decimal128(18, 2)`, `string`, `timestamp[us]`, `string`), uma
execução de cada conversão:

| Conversão | Tempo | Cópia | Tipos |
| --- | --- | --- | --- |
| `table.to_pandas(types_mapper=pd.ArrowDtype)` | 2,3 ms | Nenhuma: os buffers de cada coluna são os da tabela, 128 bytes alocados. | `int64[pyarrow]`, `date32[day][pyarrow]`, `decimal128(18, 2)[pyarrow]`, `string[pyarrow]`, `timestamp[us][pyarrow]`; inteiro com nulo continua inteiro, com `<NA>`. |
| `table.to_pandas()` | 37,6 ms | 12 MB. | `valor` em `object` de `Decimal`, `data_ref` em `object` de `date` (`datetime64[ms]` com `date_as_object=False`), `descricao` em `str`; inteiro com nulo vira `float64`. |
| `pa.Table.from_pandas(df, preserve_index=False)` do DataFrame com `ArrowDtype` | 0,6 ms | Nenhuma. | Os mesmos tipos, todo campo anulável e sem metadados; `cast` devolve o esquema do contrato em menos de 0,1 ms. |
| `pa.Table.from_pandas` do DataFrame com backend numpy | 9,7 ms | Parcial: 3,8 MB alocados para uma tabela de 8,6 MB, numa versão de três colunas. | Inferidos: `double` para `float64` e para inteiro com `NaN` (o `NaN` vira nulo), `timestamp[ms]` ou `[ns]` para datas, `large_string` para o `str` do pandas 3, `struct` para uma coluna de `dict`. |

A aritmética do pandas sobre `decimal128[pyarrow]` fica em decimal: `sum` em 1,4 ms e
`groupby().sum()` em 24 ms devolvem `Decimal` e `decimal128(18, 2)`; `valor * 2` dá
`decimal128(38, 2)`, `valor * Decimal("1.1")` dá `decimal128(21, 3)` e `valor * 1.1` dá `double`;
`merge` preserva os dtypes. Sobre `object` de `Decimal`, `sum` levou 14,3 ms. `round(2)` mantém a
escala do dtype, e o cast seguro para `decimal128(18, 2)` então aceita; sem o `round`, recusa com
`Rescaling Decimal value would cause data loss`.

### O `cast` por lote

O que a sondagem fixa em `cast`:

- `Table.cast(schema, safe=True)` recusa nulo em campo `nullable=False` (`Casting field ... with
  null values to non-nullable`), estouro de inteiro e escala perdida em decimal, e exige os mesmos
  nomes na mesma ordem: `cast` seleciona e reordena as colunas do contrato presentes e deixa as
  ausentes para o `BY NAME`. Um inteiro numa coluna `Numeric(p, s)` passa por `decimal128(38, s)`,
  porque o cast direto pede que `p` comporte qualquer `int64` (precisão 21 na escala 2), e o
  segundo cast confere se cada valor cabe em `p` (leitura de 2026-09-22).
- `safe=True` não acusa duas perdas: `double` para `decimal128(18, 2)` arredonda o valor binário
  exato (`1.236` vira `1.24` e `2.675` vira `2.67`, como o `round` do Python) e `timestamp` para
  `date32` descarta a hora. `cast` aceita as duas conversões só quando nada se perde: um `double` é
  representável na escala quando `pc.round(x, 2)` o devolve igual (os 300.000 valores `k/100.0`
  passaram em 1,1 ms, e os 270.000 de `k/1000.0` com terceira casa foram apontados), e um
  `timestamp` cabe em `date` quando a ida e volta o devolve igual; fora disso, recusa com a
  mensagem que pede o arredondamento explícito no cliente. `pc.round` e o cast discordam em
  `2.675` (2,68 contra 2,67). A carga inicial (etapa 7) não arredonda: as colunas numéricas do
  modelo de referência são `Double` e entram como estão (decisão de 2026-09-20), e a regra do
  `double` fora da escala vale para uma coluna `Numeric` futura.
- Um documento JSON é `string` no contrato Arrow, sem a extensão `arrow.json` (decisão do usuário de
  2026-09-20): no pandas com backend pyarrow, o dtype da extensão não tem os kernels de `.str`
  (`utf8_length` e `match_substring_regex` falham com `ArrowNotImplementedError`), nenhum motor a
  devolve, e o DuckDB valida o texto ao carregar numa coluna `JSON` (`Malformed JSON`). O documento
  entra já serializado: uma coluna de `dict` vira `struct` com a união das chaves (`{"k": 1}` volta
  como `{"k": 1, "x": null}`), dicts heterogêneos falham na inferência e `pa.array(dicts,
  pa.string())` falha; `json.dumps` de 300.000 documentos levou 204 ms. `cast` recusa `struct`,
  `list` e `map` numa coluna JSON.
- `large_string`, `string_view` e dicionário viram `string`, e o comprimento do texto é medido
  depois da conversão, porque `pa.types.is_string` não reconhece os três (leitura de 2026-09-22);
  `timestamp[ns]` vira `[us]` quando a parte perdida é zero e é recusado quando não é.
- Uma coluna calculada no pandas em `float64` chega como `double`: `loader.write` e `load` a recusam
  numa coluna `Numeric` enquanto houver valor fora da escala, até o pipeline arredondar; nas colunas
  `Double` do modelo de referência (decisão de 2026-09-20) ela entra como chega. O DataFrame que a
  lógica do cliente devolve mantém os dtypes `ArrowDtype` do lote, e `from_pandas` os devolve ao
  Arrow sem cópia.

### O paralelismo do cliente e as threads da biblioteca

- A API é síncrona: toda primitiva bloqueia até o efeito estar visível para a chamada seguinte, de
  qualquer thread, com autocommit por comando nos dois motores, e nenhuma é `async`, porque nenhum
  dos drivers (`duckdb`, `deltalake`, `redshift_connector`, `boto3`) tem API assíncrona em Python. O
  paralelismo de cada comando é o do motor (as `threads` do DuckDB, as slices do Redshift), e cresce
  com a máquina; o do código cliente vem de `concurrent.futures`, e `Future.result()` expressa a
  dependência entre um `load` e a leitura que o segue. A biblioteca não tem scheduler nem grafo de
  tarefas, e as suas threads são os pools de `publish`, de `publish_redshift` e de `ingest`, uma
  sessão a mais por tabela, e a auxiliar de cada `stream` e de cada `loader`, encerrada no `close`.
  O DuckDB, o delta-rs e o
  PyArrow liberam o GIL no trabalho nativo, então threads bastam, e uma extensão em Rust não entra
  por paralelismo (`test_concurrency.py`, `serialize-db.md`, seção "Paralelismo").
- Uma chamada nativa que solta e retoma o GIL ao lado de uma thread em Python puro espera o
  intervalo de troca a cada retomada: 200 `os.stat` levaram 0,3 s contra 0,2 ms, e o
  `import pyarrow.dataset` que `pq.read_table` faz na primeira chamada levou 15 s contra 0,19 s.
  Chamadas longas (uma consulta do DuckDB, um `write_deltalake`) não sofrem; chamadas curtas e
  repetidas sofrem. A biblioteca importa seus módulos na abertura, e o cliente não roda laços Python
  puros ao lado das threads da biblioteca que fazem chamadas curtas, como o `redshift_connector`
  lendo pelo socket e o `boto3`, ou leva o cálculo para SQL, onde o motor usa todos os núcleos;
  `sys.setswitchinterval(0.0005)` reduziu a espera nove vezes e é o ajuste quando a convivência for
  inevitável. Processos não alcançam o sandbox do DuckDB, que um único processo escreve
  (`test_concurrency.py`).

## Regras que as etapas obedecem

Cada regra vem de um comportamento verificado, registrado no documento citado.

- A coluna de partição (`data_str` no modelo cliente) vive na ação `add`, não no arquivo de
  dados: no modelo cliente ela deriva de uma coluna de data do arquivo por `strftime('%Y-%m-%d')`,
  e o Redshift a recebe por uma staging sem ela e `INSERT ... SELECT *, '<valor>'`; a lista de colunas no `COPY`,
  confirmada em 2026-09-21, não fornece o valor da coluna ausente (`delta.md`).
- `DECIMAL(18, 2)` sai como `INT64` do delta-rs e do DuckDB; o `COPY` desse tipo físico passou no
  ambiente alvo em 2026-09-21 (`parquet.md`, `POC.md`).
- O delta-rs não impõe duas regras de evolução: `add_columns` aceita coluna `NOT NULL` em tabela
  com dados e a deixa nula, e o `append` converte os dados para o tipo da tabela em vez de acusar a
  diferença. `reconcile` recusa a primeira, e `cast` aplica os tipos antes de gravar (`delta.md`).
- O log é limpo no checkpoint com `delta.logRetentionDuration` de 30 dias por padrão (`delta.md`;
  uma sondagem de 2026-09-19 com `interval 0 days`, seis commits e um checkpoint manteve a versão 0
  legível, então a limpeza não vira asserção); a tabela nasce com `interval 3650 days`,
  `delta.deletedFileRetentionDuration` fica em `interval 400 days`, e `keep_versions` protege os
  snapshots do banco. Com essa retenção, um arquivo de log ausente significa tabela fora do estado
  que `create_table` cria, e `version_diff` recusa com `LogUnavailable` em vez de adivinhar as
  partições alteradas (decisão do usuário de 2026-09-22).
- Dois `overwrite` da mesma partição conflitam (`CommitFailedError`); partições diferentes e
  `append` entram. Uma execução por ambiente por vez, e o conflito é o sinal de que houve duas; a ação `txn`
  não impede repetição, e a idempotência é do `overwrite` por partição (`delta.md`).
- A biblioteca escreve uma partição pelo registro do arquivo que o motor gravou, o
  `COPY ... (RETURN_STATS)` do DuckDB ou o `UNLOAD` do Redshift mais `create_write_transaction`: o
  `INSERT INTO` do DuckDB numa tabela Delta grava a coluna de partição dentro do arquivo e quebraria
  o `COPY` posicional (`delta.md`). O usuário aprovou em 2026-09-24 o registro como padrão nas
  etapas [4](PLAN-STAGE-4.md), [5](PLAN-STAGE-5.md) e [7](PLAN-STAGE-7.md), depois das partições
  medidas no ambiente alvo em 2026-09-23, em que o `rewrite` levou de 1,14 a 1,52 vez o tempo do
  registro (`POC.md`), e tirou o `rewrite` das etapas 4 e 7, com a flag `export_mode`; o
  `write_deltalake` fica só na troca da etapa 5 para a partição com `Double` não finito.
- As regras que mantêm o `COPY` do Redshift lendo os arquivos e a saída do Delta aberta: sem vetores
  de exclusão, sem column mapping, sem `Identity`, caminhos relativos no log e nunca um arquivo
  registrado por URI absoluta (`delta.md`, `estrategia.md`).
- Uma tabela alimentada pela biblioteca e pelo `UNLOAD` guarda duas codificações físicas da mesma
  coluna lógica: o delta-rs grava `DECIMAL(18, 2)` em `INT64` e timestamp em `INT64`, o `UNLOAD`
  grava em `FIXED_LEN_BYTE_ARRAY(8)` e `INT96`. Os leitores leem as duas, e o que se perde é a
  estatística da coluna de timestamp, que o `INT96` não carrega (2026-09-21, `POC.md`).
- O log guarda `minValues` e `maxValues` como valor JSON, e a transcrição do escritor do delta-rs só
  é exata em inteiro, data, `Double` e texto: um `decimal(18, 2)` de 18 dígitos significativos vira
  um dobro, e o máximo abaixo do valor real poda o arquivo que tem a linha, sem erro, nos dois
  leitores. `register_files` registra mínimo e máximo só desses quatro tipos, e uma coluna `Numeric`
  larga carrega o defeito também por `publish_partition`; o modelo cliente não tem nenhuma
  (2026-09-22, `POC.md`, `delta.md`).
- Ler no lugar custa o mesmo que ler Parquet solto; cada `delta_scan` relê o log, e toda tabela
  consultada mais de uma vez é materializada no DuckDB (`delta.md`). O `delta_scan` poda partição
  por `=`, por `IN` de um valor e por intervalo, e abre todos os arquivos com um `IN` de mais de um
  valor ou um `OR` (2026-09-23, `POC.md`): um filtro de várias partições leva o intervalo delas ao
  lado do `IN`.
- Um programa que encerra logo depois de ler uma tabela Delta lê por `to_pyarrow_dataset()`, nunca
  por `to_pyarrow_table()`: o segundo deixa uma tarefa do Acero em voo, e o destrutor do pool de
  threads do Arrow espera por ela para sempre. Meio segundo de qualquer trabalho depois da leitura
  desfaz a corrida, e é por isso que a suíte nunca a viu; quem a vê é a linha de comando, que lê e
  termina (`POC.md`).
- O delta-rs não lê a região de `~/.aws/config`: com `region = us-west-2` no perfil `default` e
  sem `AWS_REGION` nem `AWS_DEFAULT_REGION`, foi a `us-east-1` (2026-09-20); a cadeia de credenciais
  consulta o perfil (`credential_source = EcsContainer`, aviso `aws_config::profile::credentials`)
  mas encontra o contêiner sem ele (`HOME` vazio e `AWS_REGION` bastaram). A região precisa estar
  em `AWS_REGION` ou `AWS_DEFAULT_REGION`; as credenciais vêm do ambiente, do contêiner ou do IMDS,
  pela cadeia padrão, que as renova no `DeltaTable` que a execução segura; o secret do DuckDB, que
  guarda a credencial resolvida no `CREATE SECRET`, leva `REFRESH auto` (decisão do usuário de
  2026-09-24, [etapa 3](PLAN-STAGE-3.md)). `storage_options` leva a
  região, o endpoint, as chaves de SSE e `max_retries` e `retry_timeout`, para uma rede morta falhar
  em 10 s em vez de 59 s, e credencial alguma (decisão do usuário de 2026-09-22): um trio congelado
  expiraria em cerca de uma hora no meio de uma execução longa e circularia num dicionário que um log
  ou uma exceção imprime (`README.md`, `delta.md`).
- O cliente HTTP do delta-rs lê `HTTP_PROXY` e `HTTPS_PROXY` nas duas grafias e `NO_PROXY` antes de
  `no_proxy`; vazia, `NO_PROXY` anula as exceções e a chamada ao endpoint de credenciais vai pelo
  proxy (403). `prepare_environment` exporta `NO_PROXY` de `no_proxy` quando a maiúscula está
  ausente ou vazia, antes da primeira abertura de tabela; exportar depois do `import deltalake`
  basta, porque a suíte importa na coleta e exporta na fixture da sessão (`delta.md`).
- O DuckDB carrega extensões só da pasta configurada, com `autoinstall_known_extensions` e
  `autoload_known_extensions` desligados: o `LOAD` de uma extensão conhecida baixaria a extensão
  para `~/.duckdb` sem aviso, e o destino não tem internet (`README.md`).
- O ambiente alvo, lido pelos probes em 2026-09-21 (`POC.md`): sem variável de proxy e sem
  internet; o S3 responde pelo endpoint de gateway, e o STS, as três APIs do Redshift, o Glue, o
  Athena, o Secrets Manager e o DataZone por endpoints de interface; o IAM, o KMS, o Lake
  Formation e o S3 Tables não respondem. A biblioteca não chama o IAM nem o KMS: a criptografia
  SSE-KMS do bucket é aplicada pelo S3, e a permissão sobre a raiz é provada pela primeira escrita,
  não por simulação. A máquina tinha 2 vCPUs e 7,6 GiB de memória em 2026-09-21, 4 vCPUs e
  15.786 MB em 2026-09-23 e 16 vCPUs e 31.159 MB em 2026-09-24, quando a migração inteira terminou,
  com o pico de 16.430 MB na carga ordenada da maior partição de `cad_lancamentos`, sempre com
  cerca de 30 GiB livres num disco só para `HOME`, `/tmp` e o
  repositório: o motor DuckDB nasce em arquivo, com `temp_directory` conferido e os limites lidos
  do ambiente na abertura, as CPUs do processo e metade da memória que ele ainda pode usar,
  registrados no log (instrução do usuário de 2026-09-24), e o registro do arquivo do `COPY` é o
  caminho das partições (etapas [4](PLAN-STAGE-4.md) e [7](PLAN-STAGE-7.md)). Os programas que usam o pacote rodam em computação escalável da AWS, que o
  usuário escolhe, e o plano otimiza para o processamento paralelo (instrução do usuário de
  2026-09-23): o tamanho da máquina é dimensionado pelas medições, e não o contrário.
- Um campo JSON é `string` no esquema Arrow do contrato, sem a extensão `arrow.json`, `string` no
  Delta e texto nos arquivos; `JSON` no DuckDB e `SUPER` no Redshift são tipos do motor, aplicados na
  carga; a auditoria confere `json_valid` antes de publicar, porque nem o Arrow nem o Delta validam
  (`schema.md`).
- O SQLGlot transpila funções, não garante suporte; os testes de integração no Redshift continuam
  (`estrategia.md`).
- O pacote `sagemaker-studio` fica fora do projeto: arrasta `deltalake`, `duckdb` e `pandas` sem
  versão fixa, e numa instalação de teste rebaixou o `duckdb` para 1.5.1. A raiz do banco é
  configuração explícita, nunca inferida do projeto (`probes/README.md`).
- A troca de dados com o código cliente obedece à seção "A troca de dados com o código cliente":
  lotes com `cast` em cada um, nenhum gerador Python entregue ao `register` do DuckDB, API síncrona
  com o paralelismo do lado do cliente, e nenhum laço Python puro ao lado das threads da biblioteca.
- Os dois motores guardam uma sessão por execução e um `threading.RLock` que todo comando toma pelo
  tempo do comando (decisão do usuário de 2026-09-22): uma tabela temporária que o pipeline crie
  vale para os comandos seguintes nos dois motores, e se perde quando a sessão cai e o motor
  reconecta. `duckdb` e `redshift_connector` declaram `threadsafety` 1, e uma conexão DuckDB
  compartilhada sem lock entrega a uma thread o resultado da outra, sem erro: o lock é o que deixa
  várias threads usarem a sessão (`test_concurrency.py`).
- Nenhum lock espera pelo código do cliente: a parte de cada primitiva que usa a sessão termina sem
  esperar por ele. `stream` roda a consulta numa thread auxiliar, sob o lock, e entrega cada lote à
  memória, até o orçamento, ou a um arquivo intermediário, e o cliente lê os lotes enquanto a
  consulta continua; o `loader` confere o nome na abertura sem a sessão, grava os lotes num arquivo
  fora dela e, no `close`, cria a tabela e o carrega num comando só, sob o lock. Por isso o cliente
  trabalha no lote atual enquanto a consulta produz o
  seguinte, ou enquanto a biblioteca grava o anterior, e nenhuma combinação de `stream`, `loader` e
  outras primitivas, de uma thread ou de várias, trava: outro comando espera só a consulta em curso
  (`tests/test_engine_duckdb.py`, [`POC.md`](POC.md)).
- O cliente não toca o lock: as primitivas o tomam e soltam, e `with run.sandbox.session() as
  connection:` dá a conexão crua ao que elas não cobrem, com o lock tomado pelo bloco. O lock é
  reentrante, então uma primitiva chamada dentro do bloco, na mesma thread, não trava, e um `stream`
  aberto nele roda a consulta na thread do bloco; o bloco não espera por outra thread que use o
  sandbox, e uma transação que o cliente abra nele fecha nele. O estado mutável de `Execution` fica
  sob lock; o cliente não cria conexão para o sandbox, e a biblioteca não cria `Engine` do
  SQLAlchemy.
- O paralelismo entre comandos usa sessões a mais: `with run.sandbox.new_session() as other:` abre
  outra conexão ao mesmo banco, com o seu lock e as mesmas primitivas (um `cursor()` no DuckDB, uma
  conexão com credencial própria e o `USE` no Redshift), fechada no fim do bloco. Ela vê o que a
  sessão principal confirmou e não as tabelas temporárias dela, e a ordem entre as duas é a dos
  commits: o cliente que lê numa sessão o que grava na outra espera o `close` do `loader` ou o fim
  do comando. O cursor da sessão a mais nasce sem o lock da principal, porque `cursor()` não
  espera o comando em curso nela. `run.ingest` de mais de uma tabela abre uma sessão a mais por
  tabela; quatro tabelas de 150.000 linhas entraram em 0,017 s assim e em 0,066 s em série na sessão
  principal (`test_parallel.py`), e quatro de 8.000.000 de linhas, em 1,629 s contra 3,498 s com
  `threads = 2` e em 1,037 s contra 1,382 s com 11, e o pico de memória subiu de 373 MB para
  514 MB e de 803 MB para 917 MB (2026-09-23, `POC.md`). No DuckDB, o pool `threads` é da
  instância e vale para todas as sessões, e a thread que chama cada sessão também executa a consulta
  dela: com `threads` igual às CPUs do processo, o padrão, uma varredura grande em memória já
  ocupa a máquina, a ingestão por `CREATE TABLE AS` sobre `delta_scan` não ocupa, e a sessão a mais ganha
  nela, nas consultas pequenas, nos operadores que não se paralelizam e na espera do S3, onde a
  documentação do DuckDB recomenda `threads` de 2 a 5 vezes os núcleos (2026-09-23,
  [`duckdb.md`](duckdb.md)). No ambiente alvo, com 4 vCPUs, quatro tabelas de 30.001.596 linhas
  juntas entraram em 15,588 s em sessões a mais e em 19,547 s em série, e a materialização ficou
  limitada pela CPU, mais lenta com `threads` acima dos núcleos (2026-09-23, `POC.md`); com 16
  vCPUs e o cache de arquivos desligado, a materialização foi melhor com 16 threads, pior com 8 e
  com 32 a 80, a leitura agregada do S3 1,4 vez mais rápida com 48, e as sessões a mais 1,89 vez
  mais rápidas que a série: o padrão são as CPUs do processo (2026-09-24, `POC.md`). No Redshift,
  cada
  sessão a mais pede a sua credencial temporária; dois `COPY` em conexões abertas dentro da tarefa
  levaram 4,3 s e 3,8 s no ambiente alvo (2026-09-21).
- As chaves sequenciais, a chave primária inteira de uma coluna, vêm de `run.next_ids(table, n)`:
  faixas contíguas sob lock, a partir de `max_key + 1` na versão fixada, lido de `max.<coluna>` das
  ações `add` e pela varredura da coluna quando um arquivo não tem a estatística; a tabela vazia
  começa em 1. Numa chave primária composta o cliente decide os ids e não chama `next_ids`
  (decisão do usuário de 2026-09-23). Os ids de uma reexecução diferem, e a unicidade continua na
  auditoria; `publish` confere por `version_diff` que nenhuma alteração de dados entrou na tabela
  desde a versão fixada e aborta com `ExecutionConflict` quando entrou, para que duas execuções
  abertas na mesma versão não publiquem a mesma faixa; um commit só de metadados ou de manutenção
  (`reconcile`, `compact`, `vacuum`) passa e atualiza a versão fixada (`tests/test_execution.py`).
- Todo identificador que a biblioteca emite, tabela ou coluna, vai entre aspas duplas: `to`, coluna
  de `cad_contratos`, é palavra reservada no DuckDB e no Redshift, e `timestamp`, coluna de
  `cad_lancamentos`, no Redshift; sem aspas, `CREATE TABLE t (to VARCHAR(2))` falha no DuckDB
  (2026-09-21, `PLAN-STAGE-1.md`, `POC.md`). O DML da [etapa 2](PLAN-STAGE-2.md) a cumpre pela
  cópia prefixada com todo nome em `quoted_name(quote=True)`, o sentinela dentro das aspas (decisão
  do usuário de 2026-09-21).

## Organização do pacote

| Módulo | Etapa | Conteúdo |
| --- | --- | --- |
| `serialize_db.errors` | 1 | As exceções da biblioteca (`ContractError`, `SqlError`, `ConflictError`, `ExecutionConflict`, `RegistrationRefused`, `SchemaDiffRefused`, `LogUnavailable`, `SandboxError`, `AuditFailed`, `PublicationError`), num módulo sem dependências, porque `delta` levanta o que `execution` captura. |
| `serialize_db.schema` | 1 | O esquema a partir dos modelos: Arrow, Delta, DDL por dialeto gerado pela tabela de tipos com todo identificador entre aspas, opções físicas, cast seguro, arquivos gerados. |
| `serialize_db.sql` | 2 | A cópia prefixada dos statements Core que os motores compilam, e o texto SQL por dialeto, a opção de migração para fora do SQLAlchemy: parâmetro, prefixo, renderização, arquivos gerados. |
| `serialize_db.storage` | 3 | Os dois armazenamentos pelo `pyarrow.fs`: URIs, listagem, leitura, cópia, a escrita condicional do arquivo de controle (`boto3` no S3), `storage_options` e o secret do DuckDB. |
| `serialize_db.delta` | 3 | A camada Delta: criação, publicação por partição, registro de arquivos, reconciliação, reescrita, manifesto, diferença de versões, snapshots, `vacuum`, compactação, cópia profunda, exportação. |
| `serialize_db.audit` | 4 | As verificações derivadas do contrato: chaves, nulos, limites de tipo, JSON e totais; o texto SQL por dialeto e o `AuditReport`. |
| `serialize_db.resources` | 4 | As CPUs e a memória que o processo pode usar, lidas do ambiente a cada chamada, com o cgroup v1 e v2 no Linux: a fonte dos limites do motor DuckDB (instrução do usuário de 2026-09-24). |
| `serialize_db.engine` | 4 e 5 | O protocolo `Engine` e os motores `duckdb` e `redshift`, com a mesma interface. |
| `serialize_db.execution` | 6 | `Database` e `Execution`, o ciclo de uma execução. |
| `serialize_db.load` | 7 | A carga inicial dos Parquet atuais. |
| `serialize_db.publication` | 8 | A publicação para clientes: a tabela de controle, a transação por tabela, a despublicação, a reconciliação das tabelas publicadas e o estado da publicação. |
| `serialize_db.cli` | 1 a 9 | `serialize-db run`, `schema`, `sql`, `audit`, `load`, `publish`, `snapshot`, `vacuum`, `compact`, `archive`, `export` e `history`: cada subcomando entra com a etapa que entrega a primitiva por trás dele (`schema` na 1, `sql` na 2), e a etapa 6 monta o `run` e o despacho comum. |

Dependências: `pyproject.toml` passa a declarar as de execução, `sqlalchemy`, `deltalake`, `duckdb`,
`pyarrow` e `boto3`, nas versões fixadas pelos documentos; `duckdb-engine` e `sqlalchemy-redshift`
ficam no grupo `dev` até a etapa 2 e entram nas dependências de execução com ela, porque `render`
compila por esses dialetos (decisão do usuário de 2026-09-21, [`PLAN-STAGE-2.md`](PLAN-STAGE-2.md))
e os motores chamam `render` em tempo de execução (a etapa 1 gera o DDL pela tabela de tipos, sem
dialeto); `redshift-connector==2.1.17` entra no extra `redshift`, fixado porque o motor lê o
`type_modifier` do `row_desc` privado do driver ([etapa 5](PLAN-STAGE-5.md); a versão do ambiente
de desenvolvimento, decisão do usuário de 2026-09-24), e `sqlglot`
no grupo `dev`; o pandas fica no grupo `dev`, para o teste do ciclo com `ArrowDtype`, porque a
biblioteca não o importa. `prepare_offline.sh` passa a instalar os extras (`--all-extras`) e é rodado
de novo a cada mudança.

Nomes: cada módulo separa três níveis (decisões do usuário de 2026-09-21). O público é a
interface que o código cliente importa, e está obrigatoriamente na documentação do `pdoc`. O
protegido não é interface pública, mas outro módulo da biblioteca o usa: fica sem prefixo e fora
do `__all__`, e com isso fora da documentação, então um módulo que tenha um nome protegido declara
o `__all__` para o `pdoc` não o mostrar. O privado é usado só dentro do módulo e leva o prefixo
`_`. Na etapa 1,
`serialize_db.schema` deixou no `__all__` os nomes que o cliente chama e prefixou os demais
(`_cast_batch`, `_contract_column`, `_ARROW_TYPES`); `serialize_db.cli` declara só `main`,
`serialize_db.errors` as exceções que o cliente captura, e `serialize_db.delta` deixa fora do
`__all__` a protegida `file_from_return_stats`, que o motor DuckDB usa. O `__all__` de um pacote lista
também os seus submódulos públicos, porque o `pdoc` só documenta os que ele nomeia:
`serialize_db.engine` lista `duckdb`, e `tests/test_package.py` confere cada pacote.

Configuração: argumentos explícitos de `Database` e da linha de comando, com as variáveis
`SERIALIZE_DB_ROOT`, `SERIALIZE_DB_ENVIRONMENT`, `SERIALIZE_DB_ENGINE`,
`SERIALIZE_DB_DUCKDB_EXTENSIONS` e `SERIALIZE_DB_REDSHIFT_*` (as de `probes/redshift.py`) como
padrão; nenhum arquivo de configuração.

Testes: `tests/` na raiz testa o pacote, um módulo de teste por módulo do pacote;
`tests/proof_of_concept/` guarda as provas de conceito e os testes das bibliotecas externas,
comentados passo a passo porque também são o material de estudo das APIs; `tests/reference_model/` é
o modelo de referência, o modelo SQLAlchemy da base original em Parquet particionado, que fica como
está (decisão de 2026-09-21), e `tests/client_model/` é o modelo cliente, o modelo de dados que o
código cliente apresenta para usar a biblioteca (decisão de 2026-09-21), a cópia corrigida pela
etapa 1, que faz o papel da biblioteca cliente: os testes do pacote o entregam à API como um
pipeline entregaria os seus modelos. Um teste que não grava (esquema, renderização, DuckDB em
memória) roda sem variável. Um teste que grava usa a fixture `local_location`, sob
`SERIALIZE_DB_TEST_LOCAL_ROOT`, e é pulado sem ela; o fim da sessão imprime, sem erro, o comando que
autoriza cada suíte pulada e o que ela grava. O marcador `s3` repete no bucket os testes que dependem
do armazenamento, sob `SERIALIZE_DB_TEST_S3_ROOT`; o marcador `redshift` roda só com
`SERIALIZE_DB_TEST_REDSHIFT_SCHEMA`, o esquema onde a suíte pode criar tabelas
`serialize_db_test_<id>_*`, e conecta pelas variáveis `SERIALIZE_DB_REDSHIFT_*`. Os arquivos gerados
do modelo cliente (`tests/client_model/schema/` e `tests/client_model/sql/`, o que um pipeline versionaria na
raiz do seu repositório) são comparados por teste com uma geração nova, sem gravar.

## Etapas

Cada etapa entrega um módulo com testes. As etapas 1 a 4 e 6 rodam em pastas locais, sem AWS; a
etapa 5 e a parte Redshift da etapa 0 exigem a conexão; a etapa 7 exige os Parquet de origem.

| Etapa | Entrega | Critério de aceite |
| --- | --- | --- |
| 0. Prova de conceito na AWS | `tests/proof_of_concept/`: S3 verificado; no Redshift, a conexão, a escrita no datashare e os dois comandos com manifesto provados por `examples/`, e a suíte `-m redshift` limpa duas vezes seguidas no ambiente alvo (2026-09-21, 13:35 e 13:39 UTC). | Cada item respondido em `delta.md` e `redshift.md`; nenhum bloqueio sem alternativa. |
| 1. `schema` | O modelo cliente, a cópia corrigida do modelo de referência; esquema Arrow, Delta e DDL; cast; os arquivos `schema/` do modelo cliente. | O DDL de cada tabela executa no DuckDB em memória; o teste de diff falha quando um modelo muda sem regenerar; `cast` recusa perda de precisão, `double` fora da escala, texto longo e nulo em `NOT NULL`. |
| 2. `sql` | `prefixed`, `render`, `bind`, `write_sql_files`. | O texto de um statement com parâmetro, `%` em literal e prefixo roda no DuckDB com `$nome`; o teste de diff dos arquivos `sql/`. |
| 3. `storage` e `delta` | Os dois armazenamentos; a camada Delta inteira. | Testes locais de substituição da partição, conflito, reconciliação aditiva e destrutiva, reescrita num commit, `keep_versions`, exportação por partição e realocação; os mesmos no bucket com `-m s3`. |
| 4. `audit` e motor DuckDB | As verificações do contrato e seu texto por dialeto; conexão, ingestão, consulta, execução de texto, carga, auditoria, exportação da partição. | O pipeline de exemplo roda num banco em arquivo sobre um Delta local; a auditoria reprova a chave repetida entre a partição nova e uma já publicada. |
| 5. Motor Redshift | O mesmo protocolo, numa sessão única por execução sob lock, com sandbox `exec_<id>_`, `COPY ... MANIFEST` e `UNLOAD`. | SQL gerado coberto por testes sem conexão; integração com amostra, marcador `redshift`. |
| 6. Execução e linha de comando | `Database`, `Execution`, `serialize-db run`. | Reexecução idempotente; auditoria reprovada não altera o Delta; conflito abortado com mensagem. |
| 7. Carga inicial | Migração dos Parquet atuais por tabela e por partição, com relatório; `initial_load` absorve a migração adiantada de `scripts/migrate_parquet_to_delta.py`, que vem logo depois da etapa 1. | Contagens e somas por partição iguais entre origem e Delta. |
| 8. Publicação para clientes | Tabelas `<ambiente>_*` no Redshift, `version_diff`, transação única, `serialize_db_publications`, despublicação. | Uma partição alterada recarrega só essa partição. |
| 9. Operação | Snapshots, `vacuum`, compactação, arquivo, exportação, `history`, runbook, `pdoc`. | Runbook escrito e testes de manutenção passando. |

O plano de cada etapa está num arquivo próprio, que fixa as primitivas do módulo, a estratégia de
implementação de cada primitiva, os pré-requisitos e as pós-condições, os testes por caso, as
provas de conceito que o exercitam e as decisões pendentes, cada uma também um item de
[`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md). Até a etapa ser implementada, o arquivo guarda também a
interface com as assinaturas e os rascunhos executados em 2026-09-21; depois, o módulo e os testes
os substituem, como nas etapas 1 e 2:

- [Etapa 0: prova de conceito na AWS](PLAN-STAGE-0.md)
- [Etapa 1: `schema`](PLAN-STAGE-1.md)
- [Etapa 2: `sql`](PLAN-STAGE-2.md)
- [Etapa 3: `storage` e `delta`](PLAN-STAGE-3.md)
- [Etapa 4: `audit` e motor DuckDB](PLAN-STAGE-4.md)
- [Etapa 5: motor Redshift](PLAN-STAGE-5.md)
- [Etapa 6: execução e linha de comando](PLAN-STAGE-6.md)
- [Etapa 7: carga inicial](PLAN-STAGE-7.md)
- [Etapa 8: publicação para clientes](PLAN-STAGE-8.md)
- [Etapa 9: operação](PLAN-STAGE-9.md)

## Pipeline de atualização mensal

Uma execução de exemplo: `exec-2026-09-05`, ambiente `prod`, motor DuckDB, partição de referência
`2026-08-31` (`data_base_str`). As entradas são `cad_lancamentos` (as doze partições até
2026-08-31), `cad_contratos`, `cad_operacoes`, `rel_contrato_operacao` e as tabelas `dom_*`; a saída
ilustrativa é `cad_lancamentos` do banco projetado, partição 2026-08-31. Os números de versão são
ilustrativos.

| Passo | O que acontece | Artefatos |
| --- | --- | --- |
| 1. Abertura | Lê `_serialize_db/snapshots.json`; abre cada tabela de entrada e registra a versão. `serialize_db_publications` é lida só na publicação no Redshift (decisão do usuário de 2026-09-23). | `versions = {cad_lancamentos: 143, cad_contratos: 88, ...}` gravado no log da execução. |
| 2. Ingestão | DuckDB: views com os nomes dos modelos sobre `delta_scan(uri, version := 143)`; `cad_lancamentos` materializada com `WHERE data_base_str BETWEEN '2025-09-30' AND '2026-08-31'`; dimensões como views. Redshift: `COPY ... MANIFEST` dos arquivos dessas partições em `exec_2026_09_05_cad_lancamentos`, via staging. | Sandbox em `/tmp/exec-2026-09-05.duckdb` ou tabelas com prefixo no esquema único. |
| 3. Execução | O pipeline roda statements Core, texto gerado e lógica Python sobre o sandbox; o que sai para o Python sai em lotes por `stream`, ou como `pa.Table` por `query`, e volta por `loader` ou `load`; intermediários ficam no sandbox. | Tabela `cad_lancamentos_projetados` no sandbox, partição 2026-08-31. |
| 4. Auditoria | Contagem, nulos, unicidade da chave contra as demais partições da versão 57, `data_base_str = strftime(data_base, '%Y-%m-%d')`, limites de tipo, `json_valid`, totais de controle. | Relatório com o SQL de cada verificação no log da execução; reprovação encerra sem tocar o Delta. |
| 5. Publicação no Delta | `reconcile`; `register_files` do arquivo que o motor gravou (`COPY ... (RETURN_STATS)` do DuckDB, `UNLOAD` do Redshift para `data_base_str=2026-08-31/<execution_id>_<uuid>/`), depois das conferências; no motor Redshift, a partição com `Double` não finito troca para `publish_partition(uri, table, "2026-08-31", data, commit_metadata(...), storage)` a partir do leitor. | `cad_lancamentos` projetado passa da versão 57 para 58; um arquivo em `data_base_str=2026-08-31/`. |
| 6. Publicação no Redshift | Confere que `serialize_db_publications`, criada uma vez pelo usuário, existe; a transação lê a linha de controle, 57, e `version_diff(57, 58)` aponta a partição 2026-08-31; `DELETE` da partição, `COPY ... MANIFEST` na staging, `INSERT ... SELECT *, '2026-08-31'` e o `UPDATE` da linha de controle de 57 para 58. | `prod_cad_lancamentos_projetados` com a partição nova; `serialize_db_publications` em 58. |
| 7. Snapshot do banco | Só na execução marcada, por exemplo a do fim do trimestre: `serialize_db_snapshot = "2026T3"` nos commits e a entrada em `_serialize_db/snapshots.json`. | Versões do snapshot protegidas por `keep_versions`. |
| 8. Encerramento | Sandbox descartado, staging apagado, resumo no log. | Execução idempotente: repetir os passos 5 e 6 reproduz o mesmo estado. |

A API da etapa 6:

```python
import pandas as pd
import pyarrow as pa
from sqlalchemy import select

from serialize_db import Database, Execution
from pipeline import compute_in_sandbox, project
from pipeline.models import Base, Contrato, Lancamento, LancamentoProjetado, Operacao, RelContratoOperacao

db = Database("s3://bucket/projeto/delta", environment="prod", metadata=Base.metadata)
with Execution(db, engine="duckdb", partition="2026-08-31", execution_id="exec-2026-09-05") as run:
    run.ingest(Lancamento, partitions=run.previous_partitions(Lancamento, 12), materialize=True)
    run.ingest(Contrato, Operacao, RelContratoOperacao)              # views sobre a versão fixada
    compute_in_sandbox(run.sandbox)                                  # statements Core e texto gerado, por query
    entries = select(Lancamento).where(Lancamento.data_base_str == "2026-08-31")
    with run.sandbox.stream(entries, batch_size=100_000) as stream, run.sandbox.loader(LancamentoProjetado) as loader:
        for batch in stream:                                         # a biblioteca já lê o lote seguinte
            frame = batch.to_pandas(types_mapper=pd.ArrowDtype)      # sem cópia; decimal128 e date32 mantidos
            projected = project(frame)                               # lógica Python por linha; devolve um DataFrame
            projected["id_lancamento"] = run.next_ids(LancamentoProjetado, len(projected))   # faixa contígua, sob lock
            loader.write(pa.RecordBatch.from_pandas(projected, preserve_index=False))        # cast aqui; o lote anterior entra na thread do loader
    run.audit(LancamentoProjetado, partitions=["2026-08-31"])          # exigida por publish
    run.publish(LancamentoProjetado, partitions=["2026-08-31"])        # overwrite por partição, metadados
    run.publish_redshift(LancamentoProjetado)                        # só as partições alteradas
```

O passo 3 usa a API da seção "A troca de dados com o código cliente": o exemplo mostra `stream` e
`loader`, e `query` e `load` são a forma por tabela.

Uma reexecução com o mesmo `execution_id` repete os `overwrite` das mesmas partições e produz as
mesmas linhas; os ids podem diferir, porque `next_ids` recomeça do máximo da versão fixada. Uma
correção de uma partição antiga é a mesma chamada com outra `partition` e um `execution_id` novo:
`publish_redshift` recarrega só essa partição, e as versões intermediárias entre snapshots do banco
saem no `vacuum` mensal. A execução no Redshift é o mesmo ciclo com `engine="redshift"`: o sandbox
são as tabelas `exec_<id>_*`, a ingestão é `COPY ... MANIFEST`, e a publicação sai por `UNLOAD` mais
`register_files`, sem passar pela máquina local, ou, na partição com `Double` não finito, mais
`publish_partition`, que grava pelo `write_deltalake`, confere tudo e paga a memória (decisões do
usuário de 2026-09-23 e 2026-09-24, [etapa 5](PLAN-STAGE-5.md)).

## Ordem do trabalho

1. Etapa 1, em pasta local: o modelo cliente em `tests/client_model/` (escrito em 2026-09-21, a
   cópia corrigida de `tests/reference_model/`, conferida por `tests/test_client_model.py`) e o
   módulo `serialize_db.schema`, com os arquivos `schema/` versionados em `tests/client_model/`.
2. A migração adiantada ([`PLAN-STAGE-7.md`](PLAN-STAGE-7.md), seção "A migração adiantada"):
   `scripts/migrate_parquet_to_delta.py`, o rascunho da etapa 7 sobre `serialize_db.schema`, o
   `deltalake` e o DuckDB, testado sobre a base fictícia e executado no ambiente alvo sobre a cópia
   da base de produção, tabela a tabela. Ela dá a base Delta sobre a qual as etapas seguintes se
   desenvolvem e a medição da partição de `cad_lancamentos`.
3. Etapa 2, depois 3, 4 e 6: um pipeline completo em disco local, o critério de aceite da etapa 6
   sobre o motor DuckDB; a etapa 7 absorve o script.
4. Em paralelo, no ambiente alvo: os cinco probes rodaram lá em 2026-09-21 e de novo em 2026-09-23,
   com a leitura `RS-8` ([`POC.md`](POC.md)); a manutenção da suíte S3 se confirmada;
   `tests/proof_of_concept/` e os testes `-m s3` das etapas 3 e 4 no bucket.
5. O `test_redshift.py` da etapa 0 rodou limpo duas vezes no ambiente alvo em 2026-09-21, pela
   conexão de `examples/`; as etapas 5 e 8 vêm depois das etapas 3, 4 e 6, com essa conexão.
6. Etapa 7, implementada em 2026-09-24 sobre a base fictícia, com o script de migração fino sobre
   o pacote e a carga pelo pacote ainda por rodar no ambiente alvo; etapa 9 por último, com o
   runbook.
