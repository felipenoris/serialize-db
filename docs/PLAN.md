# Plano de implementação

Este documento registra o que a biblioteca é e como ela chega lá: as decisões, a troca de dados com
o código cliente, as regras que as etapas obedecem, a organização do pacote, as etapas de implementação e o pipeline de atualização
mensal. O plano de cada etapa, com as primitivas do módulo, os testes e as provas de conceito que o
exercitam, está num arquivo próprio, de [`PLAN-STAGE-0.md`](PLAN-STAGE-0.md) a
[`PLAN-STAGE-9.md`](PLAN-STAGE-9.md), que a seção "Etapas" indexa. O estado da implementação, com
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
a publicação. Os statements Core do pipeline continuam válidos, e o texto SQL gerado por dialeto os
substitui, uma interação com o banco por vez, até o SQLAlchemy terminar nos modelos e na geração.
Os dados cruzam a fronteira da biblioteca em lotes `pyarrow.RecordBatch` (seção "A troca de dados
com o código cliente"). A evolução do esquema é uma reconciliação entre o modelo e o log da tabela,
sem Alembic.

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
  `USE datalake_rw_shared` e cita `sbx_aco_decon.<tabela>`; o nome em três partes fica para uma
  sessão aberta em outro banco, como a Data API. A escrita segue o que um datashare aceita, com o
  `COPY` sem cláusula `COMPUPDATE` e transação explícita ([`redshift.md`](redshift.md)). A Data API
  fica fora da biblioteca: ela devolve `DECIMAL` e data e hora como texto e limita o resultado a
  500 MB, o que não serve à troca de lotes Arrow.
- **Nenhum serviço de catálogo está habilitado.** A camada de tabela não depende de serviço, e o
  Delta atende sem código próprio. O Iceberg com catálogo em arquivo fica documentado em
  `estrategia.md` e volta à mesa se o Glue ou o S3 Tables forem habilitados; `probes/catalog.py`
  mede esse gatilho.
- **O Delta é a fonte da verdade depois da carga inicial.** A tabela é criada do modelo por
  `DeltaTable.create`, sem DDL em SQL; a evolução e o histórico ficam no log.
- **Execuções de desenvolvimento e de produção gravam tabelas separadas.** Um caminho por ambiente,
  `<raiz>/<ambiente>/<tabela>/`, e o prefixo do ambiente nas tabelas do Redshift; a concorrência
  que resta é entre execuções do mesmo ambiente, que o log serializa.
- **Renomear ou remover colunas é raro.** A evolução é aditiva; o caso raro reescreve a tabela
  inteira num commit e recria a tabela publicada, sem esperar o column mapping do delta-rs.
- **Os dois armazenamentos são suportados.** Toda primitiva recebe a URI de uma pasta local ou de
  um prefixo S3; a pasta local é o ambiente dos testes e do desenvolvimento sem AWS.
- **Um teste só grava onde o usuário autorizou.** A variável de raiz de cada suíte é a
  autorização: sem ela a suíte é pulada, com ela o que impede a escrita é falha, e `pytest` sem
  variável não grava arquivo algum.
- **A partição é por data em texto `AAAA-MM-DD`, nos moldes da base de referência** (decisão de
  2026-09-20). A coluna de partição é do modelo do cliente, não da biblioteca: o modelo a declara
  em `Table.info["serialize_db"]` com a coluna de data de que ela deriva (`partition_by`
  `["data_str"]` e `partition_source` `"data"`; em `cad_lancamentos`, `data_base_str` de
  `data_base`), o valor é `strftime(<coluna de data>, '%Y-%m-%d')`, e cada valor é uma partição, a
  unidade de ingestão, auditoria, publicação e substituição. A biblioteca não fixa nome nem
  granularidade; `mes` nos exemplos de `delta.md`, `duckdb.md`, `parquet.md` e `sqlalchemy.md` é
  uma coluna de partição ilustrativa.
- **Toda coluna numérica da base de origem é `double`, e o modelo de referência a mantém `Double`**
  (decisão de 2026-09-20): sem arredondamento nem `Numeric` de precisão fixa. O pacote suporta
  `Numeric(p, s)` pela tabela de tipos de `schema.md`, e a transição de `valor` para
  `Numeric(18, 2)`, mais adequada a dados contábeis, é uma melhoria futura, por `rewrite` da tabela
  com o `cast` que recusa o `double` fora da escala.
- **As chaves inteiras passam a `int64` na migração para o Delta** (decisão de 2026-09-20): a origem
  as tem em `int32`, com `id_lancamento` em 1.113.599.996; o modelo corrigido declara `BigInteger`
  nas chaves primárias inteiras e nas colunas que as referenciam, e a carga inicial faz o cast sem
  perda.
- **O `timestamp` em `INT96` da origem vira `INT64` na migração** (decisão de 2026-09-20): o formato
  Parquet marca o `INT96` como obsoleto (`parquet.thrift`: "deprecated, new Parquet writers should
  not write data in INT96"), e a precisão dos timestamps da origem não importa: a carga trunca a
  microssegundos, o `timestamp[us]` do contrato, e o delta-rs grava `INT64`.
- **A nulidade é a do modelo** (decisão de 2026-09-20): sete colunas de `cad_contratos` são
  anuláveis nos arquivos e `NOT NULL` no modelo, sem nulo nos dados; o `cast` da carga as recusa
  com nulo, e a regra só muda se a migração o mostrar.

As partes da biblioteca: o esquema a partir dos modelos; o SQL gerado por dialeto; a camada Delta
sobre os dois armazenamentos; a ingestão seletiva e o sandbox por execução em cada motor; a execução
com auditoria e publicação; a publicação para clientes no Redshift; a carga inicial dos Parquet
atuais; a operação (snapshots do banco, `vacuum`, compactação, arquivo). Fora da biblioteca ficam o
ORM para cargas linha a linha, as chaves estrangeiras `DEFERRABLE`, o `Identity`, os manifestos
próprios e o Alembic; o SQLGlot fica opcional, como teste de compatibilidade.

## A troca de dados com o código cliente

Os dados cruzam a fronteira da biblioteca em lotes `pyarrow.RecordBatch`, nos dois sentidos, e a
`pyarrow.Table` é a forma conveniente do que cabe na memória, redirecionada para a mesma API de
lotes (decisão do usuário de 2026-09-20). O cliente trabalha no lote atual enquanto a biblioteca lê
o seguinte ou grava o anterior, cada primitiva numa thread auxiliar com uma fila limitada. O pandas
com backend pyarrow é o formato dos pipelines (declaração do usuário de 2026-09-20), e a regra
apoia-se na conversão barata, medida na subseção "A conversão para o pandas": 2,3 ms sem cópia para
300.000 linhas, 1,4 ms para um lote de 100.000; o pandas não entra nas dependências de execução. Por motor: no DuckDB, a saída por
`to_arrow_reader()` e a entrada por um `INSERT ... BY NAME` por lote numa transação; no Redshift, a
saída pelas tuplas de `fetchmany` ou por `UNLOAD` e a entrada por Parquet no S3 mais
`COPY ... MANIFEST`.

### A API

O pipeline lê com `run.sandbox.stream(statement_ou_texto, params, batch_size, prefetch)`, que
devolve um `BatchStream` (iterável de `RecordBatch` com `schema`, `read_next_batch`, `read_all`,
`close`, gerenciador de contexto e `__arrow_c_stream__`), ou com `run.sandbox.query(statement)` e
`run.sandbox.execute(texto, params)`, que devolvem a `pa.Table` de `stream(...).read_all()`; grava
com `with run.sandbox.loader(Modelo) as loader: loader.write(lote)`, ou com
`run.sandbox.load(Modelo, data)`, que aceita `pa.Table`, `RecordBatch`, `RecordBatchReader` ou
iterável de lotes e os passa ao mesmo `loader`. `query` compila o statement pelo dialeto e o executa
na conexão crua, sem `Session`, e `select(Lancamento)` é aceito como statement Core. A forma por
tabela serve ao que cabe na memória e à lógica que precisa de todas as linhas: `query(statement)`
devolve a `pa.Table`, `to_pandas(types_mapper=pd.ArrowDtype)` a leva ao pandas, e
`load(Modelo, pa.Table.from_pandas(frame, preserve_index=False))` grava. O caminho de um resultado
até o Delta é `loader` ou `load`, `audit` e `publish`. Nenhuma primitiva pública recebe ou devolve
um DataFrame, uma lista de linhas ou uma instância ORM; a mensagem que recusa um DataFrame aponta
`pa.Table.from_pandas` e `pa.RecordBatch.from_pandas`. Dentro da biblioteca, o que não cabe na
memória corre por `RecordBatchReader` (`export_partition` para `publish_partition`, `rewrite`, a
carga inicial), cada um no seu cursor. O exemplo de uso, `stream` e `loader` dentro de uma
execução, está na seção "Pipeline de atualização mensal".

### O que a sondagem fixou em cada primitiva

A sondagem de 2026-09-20 (macOS arm64, DuckDB 1.5.5 com `threads = 2`, PyArrow 25.0.1, pandas
3.0.6; as asserções em `test_duckdb.py`, `test_pyarrow.py` e `test_parallel.py`, e a leitura em
[`POC.md`](POC.md)) fixou em cada primitiva:

- **`stream` roda a consulta num cursor próprio**, e o cursor da thread do cliente fica livre: o
  leitor preso a um cursor entregou o snapshot da sua consulta (1.000.000 de linhas) enquanto outro
  cursor inseria dez linhas na mesma tabela, criava, alterava e apagava tabelas; no mesmo cursor, o
  comando seguinte o esvazia sem erro. Fechar o cursor no meio não interrompeu o leitor, e cem
  `cursor()` mais `close()` levaram 0,4 ms.
- **Uma thread auxiliar por stream pré-busca `prefetch` lotes** (padrão 2) numa fila limitada,
  puxando do leitor do DuckDB enquanto o cliente trabalha no lote atual; `prefetch=0` dispensa a
  thread. Toda espera na fila tem prazo e confere o encerramento, a thread não referencia o stream,
  e `close` (ou o fim do `with`) a interrompe, esvazia a fila e fecha o cursor; um stream abandonado
  é coletado e a thread termina. O erro da consulta chega ao cliente como `duckdb.Error` na
  construção ou como `OSError` com a mensagem do DuckDB na leitura de um lote, nunca em silêncio.
- **O streaming limita a memória do lado Python, não a do DuckDB.** 20.000.000 de linhas em três
  colunas: `to_arrow_table` em 0,59 s com o processo em 567 MB; `to_arrow_reader(100_000)` em 0,54 s
  com o processo em 83 MB, e com a pré-busca de dois lotes em 0,47 s e 89 MB. Sem `ORDER BY` o
  primeiro lote chegou em 3 ms; com `ORDER BY`, o `execute` levou 2,4 s (a ordenação inteira, dentro
  do DuckDB, sob `memory_limit` e `temp_directory`) e o primeiro lote veio em seguida. A ordem dos
  lotes é a da consulta: sem `ORDER BY`, com `preserve_insertion_order = false`, é arbitrária.
- **O ganho do encadeamento é o trabalho do cliente escondido atrás da leitura**, limitado pelo
  estágio mais lento: 6.000.000 de linhas em lotes de 200.000, com o trabalho por lote em pandas
  (`to_pandas(types_mapper=pd.ArrowDtype)`, duas colunas calculadas, `RecordBatch.from_pandas`),
  levaram 0,201 s em sequência e 0,160 s encadeados (1,25x; só a leitura, 0,157 s); com um laço
  Python puro sobre 20.000 valores por lote, 0,250 s contra 0,162 s (1,55x). Puxar um lote é uma
  chamada nativa longa, e a thread auxiliar paga no máximo um intervalo de troca do GIL por lote ao
  lado do laço Python do cliente.
- **`loader` insere cada lote por `INSERT ... BY NAME` numa transação explícita, num cursor próprio
  e numa thread auxiliar**, enquanto o cliente prepara o lote seguinte: `write` faz o `cast` do lote
  na thread do cliente, para o erro aparecer com o lote em mãos, e bloqueia quando a fila está
  cheia; nada é visível a outro cursor antes do `commit` (a contagem ficou em 0 durante vinte
  lotes); uma exceção dentro do `with`, um lote recusado pelo `cast` ou um erro do `INSERT` desfazem
  a transação, e a tabela fica como estava; um loader abandonado sem `close` também desfaz. Cada
  comando custa cerca de 3,5 ms: 6.000.000 de linhas em lotes de 100.000 levaram 0,41 s, contra
  0,20 s de um único `INSERT` sobre um leitor da fila. O pipeline de três estágios (`stream`,
  trabalho em pandas, `loader`) sobre 3.000.000 de linhas levou 0,127 s, contra 0,178 s lote a lote
  sem threads e 0,131 s pela tabela inteira.
- **O `INSERT` único sobre um leitor alimentado por gerador Python não é o caminho**, embora seja um
  comando só e atômico (a falha do gerador no lote 20 deixou a tabela como estava). O `arrow_scan`
  do DuckDB puxa o fluxo por uma thread de leitura antecipada do Arrow (`BackgroundGenerator`, lida
  na pilha nativa), que chama o gerador em outra thread: ela tinha puxado de 5 a 15 lotes quando o
  comando falhou no primeiro, chegou a 10 ou 20 depois da falha e parou ali. Esse buffer a fila da
  biblioteca não controla, e essa thread, ainda chamando Python na saída do processo, pendurou o
  processo no destrutor do pool de threads do Arrow. O mesmo vale para qualquer gerador Python
  entregue ao `register` do DuckDB, e é por isso que `BatchStream.__arrow_c_stream__` serve ao
  `write_deltalake` e ao `RecordBatchReader.from_stream`, não ao `register`.
- **O `cast` por lote é barreira de segurança, não só de contrato.** `RecordBatchReader.from_batches`
  não confere cada lote contra o esquema declarado: `read_next_batch` devolve o lote como veio, e só
  `read_all` acusa `Schema at index 0 was different`. O `arrow_scan` do DuckDB lê os buffers pelo
  esquema declarado, então um lote com as colunas em outra ordem entrou sem erro com os bytes
  trocados (`(1, 1.0)` lido como `(4607182418800017408, 5e-324)`); uma coluna a mais ou a menos falha
  com `ArrowArray struct has 3 children, expected 2`. O DuckDB não confere a nulidade do esquema
  Arrow; a coluna `NOT NULL` do DuckDB ele confere (`ConstraintException`). `RecordBatch.cast` recusa
  o que `Table.cast` recusa: nulo em campo `nullable=False`, nomes fora de ordem e escala perdida.
- **As conversões do lote não copiam**: `to_batches(max_chunksize=100_000)` de 300.000 linhas em
  0,04 ms e `Table.from_batches` em 0,003 ms sobre os mesmos buffers;
  `RecordBatch.to_pandas(types_mapper=pd.ArrowDtype)` de 100.000 linhas em 1,4 ms e
  `RecordBatch.from_pandas` em 0,5 ms, com os buffers compartilhados e os tipos do contrato, como na
  tabela inteira.
- **A lógica do cliente por lote é a lógica por linha.** Uma agregação, um `merge`, uma ordenação ou
  uma janela precisam de todas as linhas: vão para SQL no sandbox, onde o DuckDB usa todos os
  núcleos, ou para a `pa.Table` de `query`. `run.next_ids(table, batch.num_rows)` dá a cada lote a
  sua faixa.
- **No Redshift**, `stream` monta cada lote de `cursor.fetchmany(batch_size)` por
  `RecordBatch.from_pylist` com o esquema do statement, numa thread auxiliar que compete pelo GIL com
  o cliente porque o `redshift_connector` é Python puro, e `loader` grava um row group por lote com
  `ParquetWriter.write_batch` em `staging/<execution_id>/` e faz o `COPY` no `close`, então nada
  entra antes dele. Se o `redshift_connector` materializa o resultado no `execute` ou o lê do socket
  no `fetchmany` decide se `stream` limita a memória sem `UNLOAD`
  ([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)).

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
  ausentes para o `BY NAME`. Um inteiro numa coluna `Numeric(18, 2)` passa por `decimal128(21, 2)`,
  porque o cast direto pede precisão 21.
- `safe=True` não acusa duas perdas: `double` para `decimal128(18, 2)` arredonda o valor binário
  exato (`1.236` vira `1.24` e `2.675` vira `2.67`, como o `round` do Python) e `timestamp` para
  `date32` descarta a hora. `cast` aceita as duas conversões só quando nada se perde: um `double` é
  representável na escala quando `pc.round(x, 2)` o devolve igual (os 300.000 valores `k/100.0`
  passaram em 1,1 ms, e os 270.000 de `k/1000.0` com terceira casa foram apontados), e um
  `timestamp` cabe em `date` quando a ida e volta o devolve igual; fora disso, recusa com a
  mensagem que pede o arredondamento explícito no cliente. `pc.round` e o cast discordam em
  `2.675` (2,68 contra 2,67), então a carga inicial (etapa 7) arredonda os `Double` dos modelos
  atuais por `pc.round(x, 2)` antes do cast e registra a regra no relatório.
- Um documento JSON é `string` no contrato Arrow, sem a extensão `arrow.json` (decisão do usuário de
  2026-09-20): no pandas com backend pyarrow, o dtype da extensão não tem os kernels de `.str`
  (`utf8_length` e `match_substring_regex` falham com `ArrowNotImplementedError`), nenhum motor a
  devolve, e o DuckDB valida o texto ao carregar numa coluna `JSON` (`Malformed JSON`). O documento
  entra já serializado: uma coluna de `dict` vira `struct` com a união das chaves (`{"k": 1}` volta
  como `{"k": 1, "x": null}`), dicts heterogêneos falham na inferência e `pa.array(dicts,
  pa.string())` falha; `json.dumps` de 300.000 documentos levou 204 ms. `cast` recusa `struct`,
  `list` e `map` numa coluna JSON.
- `large_string` vira `string`; `timestamp[ns]` vira `[us]` quando a parte perdida é zero e é
  recusado quando não é.
- Uma coluna calculada no pandas em `float64` chega como `double`: `loader.write` e `load` a recusam
  numa coluna `Numeric` enquanto houver valor fora da escala, até o pipeline arredondar; nas colunas
  `Double` do modelo de referência (decisão de 2026-09-20) ela entra como chega. O DataFrame que a
  lógica do cliente devolve mantém os dtypes `ArrowDtype` do lote, e `from_pandas` os devolve ao
  Arrow sem cópia.

### O paralelismo do cliente e as threads da biblioteca

- A API é síncrona: toda primitiva bloqueia até o efeito estar visível para a chamada seguinte, de
  qualquer thread, com autocommit por comando nos dois motores, e nenhuma é `async`, porque nenhum
  dos drivers (`duckdb`, `deltalake`, `redshift_connector`, `boto3`) tem API assíncrona em Python. O
  paralelismo é do código cliente, com `concurrent.futures`, e `Future.result()` expressa a
  dependência entre um `load` e a leitura que o segue; a biblioteca não tem scheduler nem grafo de
  tarefas, e as suas threads são os pools de `ingest`, `publish` e `publish_redshift` e a auxiliar de
  cada `stream` e de cada `loader`, com fila limitada e encerrada no `close`. O DuckDB, o delta-rs e
  o PyArrow liberam o GIL no trabalho nativo, então threads bastam, e uma extensão em Rust não entra
  por paralelismo (`test_concurrency.py`, `serialize-db.md`, seção "Paralelismo").
- Uma chamada nativa que solta e retoma o GIL ao lado de uma thread em Python puro espera o
  intervalo de troca a cada retomada: 200 `os.stat` levaram 0,3 s contra 0,2 ms, e o
  `import pyarrow.dataset` que `pq.read_table` faz na primeira chamada levou 15 s contra 0,19 s. A
  biblioteca importa seus módulos na abertura, e o cliente não roda laços Python puros ao lado das
  threads da biblioteca que fazem chamadas curtas, como o `redshift_connector` lendo pelo socket e o
  `boto3`; `sys.setswitchinterval` é o ajuste (`test_concurrency.py`).

## Regras que as etapas obedecem

Cada regra vem de um comportamento verificado, registrado no documento citado.

- A coluna de partição (`data_str` no modelo de referência) vive na ação `add`, não no arquivo de
  dados: ela deriva de uma coluna de data do arquivo por `strftime('%Y-%m-%d')`, e o Redshift a
  recebe por uma staging sem ela e `INSERT ... SELECT *, '<valor>'`, ou por lista de colunas no
  `COPY` se a prova de conceito a confirmar (`delta.md`).
- `DECIMAL(18, 2)` sai como `INT64` do delta-rs e do DuckDB; o `COPY` desse tipo físico é o primeiro
  item da prova de conceito no Redshift (`parquet.md`).
- O delta-rs não impõe duas regras de evolução: `add_columns` aceita coluna `NOT NULL` em tabela
  com dados e a deixa nula, e o `append` converte os dados para o tipo da tabela em vez de acusar a
  diferença. `reconcile` recusa a primeira, e `cast` aplica os tipos antes de gravar (`delta.md`).
- O log é limpo no checkpoint com `delta.logRetentionDuration` de 30 dias por padrão (`delta.md`;
  uma sondagem de 2026-09-19 com `interval 0 days`, seis commits e um checkpoint manteve a versão 0
  legível, então a limpeza não vira asserção); a tabela nasce com `interval 3650 days`,
  `delta.deletedFileRetentionDuration` fica em `interval 400 days`, e `keep_versions` protege os
  snapshots do banco.
- Dois `overwrite` da mesma partição conflitam (`CommitFailedError`); partições diferentes e
  `append` entram. Uma execução por ambiente por vez, e o conflito é o sinal de que houve duas; a ação `txn`
  não impede repetição, e a idempotência é do `overwrite` por partição (`delta.md`).
- A biblioteca escreve por um único caminho, delta-rs ou `COPY ... (RETURN_STATS)` mais
  `create_write_transaction`: o `INSERT INTO` do DuckDB numa tabela Delta grava a coluna de
  partição dentro do arquivo e quebraria o `COPY` posicional (`delta.md`).
- As regras que mantêm o `COPY` do Redshift lendo os arquivos e a saída do Delta aberta: sem vetores
  de exclusão, sem column mapping, sem `Identity`, caminhos relativos no log e nunca um arquivo
  registrado por URI absoluta (`delta.md`, `estrategia.md`).
- Ler no lugar custa o mesmo que ler Parquet solto; cada `delta_scan` relê o log, e toda tabela
  consultada mais de uma vez é materializada no DuckDB (`delta.md`).
- O delta-rs não lê a região de `~/.aws/config`: com `region = us-west-2` no perfil `default` e
  sem `AWS_REGION` nem `AWS_DEFAULT_REGION`, foi a `us-east-1` (2026-09-20); a cadeia de credenciais
  consulta o perfil (`credential_source = EcsContainer`, aviso `aws_config::profile::credentials`)
  mas encontra o contêiner sem ele (`HOME` vazio e `AWS_REGION` bastaram). A região precisa estar
  em `AWS_REGION` ou `AWS_DEFAULT_REGION`; as credenciais vêm do ambiente, do contêiner, do IMDS ou
  de `storage_options`. `storage_options` leva `max_retries` e `retry_timeout` para uma rede morta
  falhar em 10 s em vez de 59 s (`README.md`, `delta.md`).
- O cliente HTTP do delta-rs lê `HTTP_PROXY` e `HTTPS_PROXY` nas duas grafias e `NO_PROXY` antes de
  `no_proxy`; vazia, `NO_PROXY` anula as exceções e a chamada ao endpoint de credenciais vai pelo
  proxy (403). `prepare_environment` exporta `NO_PROXY` de `no_proxy` quando a maiúscula está
  ausente ou vazia, antes da primeira abertura de tabela; exportar depois do `import deltalake`
  basta, porque a suíte importa na coleta e exporta na fixture da sessão (`delta.md`).
- O DuckDB carrega extensões só da pasta configurada, com `autoinstall_known_extensions` e
  `autoload_known_extensions` desligados: o `LOAD` de uma extensão conhecida baixaria a extensão
  para `~/.duckdb` sem aviso, e o destino não tem internet (`README.md`).
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
- O motor guarda uma conexão por thread: `duckdb` e `redshift_connector` declaram `threadsafety` 1,
  uma conexão DuckDB compartilhada entrega a uma thread o resultado da outra sem erro, um banco em
  memória só é compartilhado por `cursor()` da conexão que o abriu, um segundo `connect(arquivo)`
  com outra configuração ou `read_only` é recusado, e `threads` é da instância. Cada thread recebe
  um `cursor()` do DuckDB ou uma conexão Redshift num `threading.local`, criados no primeiro uso e
  fechados em `cleanup`, e cada `stream` e cada `loader` abre um cursor próprio, fechado no `close`,
  que deixa o da thread do cliente livre; `run.sandbox.connection` expõe a conexão crua da thread; o
  estado mutável de `Execution` fica sob lock; o cliente não cria conexão para o sandbox, e a
  biblioteca não cria `Engine` do SQLAlchemy (`test_concurrency.py`, `test_parallel.py`).
- As chaves inteiras vêm de `run.next_ids(table, n)`: faixas contíguas sob lock, a partir de
  `max_key + 1` na versão fixada, lido de `max.<coluna>` das ações `add` e pela varredura da coluna
  quando um arquivo não tem a estatística; a tabela vazia começa em 1. Os ids de uma reexecução
  diferem, e a unicidade continua na auditoria; `publish` confere que a versão da tabela ainda é a
  fixada e aborta com `ExecutionConflict`, para que duas execuções abertas na mesma versão não
  publiquem a mesma faixa (`test_parallel.py`).

## Organização do pacote

| Módulo | Etapa | Conteúdo |
| --- | --- | --- |
| `serialize_db.schema` | 1 | O esquema a partir dos modelos: Arrow, Delta, DDL por dialeto, opções físicas, cast seguro, arquivos gerados. |
| `serialize_db.sql` | 2 | O texto SQL por dialeto a partir de statements Core: parâmetro, prefixo, renderização, arquivos gerados. |
| `serialize_db.storage` | 3 | Os dois armazenamentos atrás de uma interface: URIs, leitura e escrita condicional, cópia, listagem, `storage_options` e o secret do DuckDB. |
| `serialize_db.delta` | 3 | A camada Delta: criação, publicação por partição, registro de arquivos, reconciliação, reescrita, manifesto, diferença de versões, snapshots, `vacuum`, compactação, cópia profunda, exportação. |
| `serialize_db.audit` | 4 | As verificações derivadas do contrato: chaves, nulos, limites de tipo, JSON e totais; o texto SQL por dialeto e o `AuditReport`. |
| `serialize_db.engine` | 4 e 5 | O protocolo `Engine` e os motores `duckdb` e `redshift`, com a mesma interface. |
| `serialize_db.execution` | 6 | `Database` e `Execution`, o ciclo de uma execução. |
| `serialize_db.load` | 7 | A carga inicial dos Parquet atuais. |
| `serialize_db.cli` | 6 a 9 | `serialize-db run`, `schema`, `sql`, `audit`, `load`, `publish`, `snapshot`, `vacuum`, `compact`, `archive`, `export` e `history`. |

Dependências: `pyproject.toml` passa a declarar as de execução, `sqlalchemy`, `deltalake`, `duckdb`,
`pyarrow` e `boto3`, nas versões fixadas pelos documentos, mais `duckdb-engine` e
`sqlalchemy-redshift`, que saem do grupo `dev` das suítes de estudo para as de execução enquanto
houver compilação em tempo de execução; `redshift-connector` entra no extra `redshift`, e `sqlglot`
no grupo `dev`; o pandas fica no grupo `dev`, para o teste do ciclo com `ArrowDtype`, porque a
biblioteca não o importa. `prepare_offline.sh` passa a instalar os extras (`--all-extras`) e é rodado
de novo a cada mudança.

Configuração: argumentos explícitos de `Database` e da linha de comando, com as variáveis
`SERIALIZE_DB_ROOT`, `SERIALIZE_DB_ENVIRONMENT`, `SERIALIZE_DB_ENGINE`,
`SERIALIZE_DB_DUCKDB_EXTENSIONS` e `SERIALIZE_DB_REDSHIFT_*` (as de `probes/redshift.py`) como
padrão; nenhum arquivo de configuração.

Testes: `tests/` na raiz testa o pacote, um módulo de teste por módulo do pacote;
`tests/proof_of_concept/` guarda as provas de conceito e os testes das bibliotecas externas,
comentados passo a passo porque também são o material de estudo das APIs; `tests/model/` é o modelo
de referência, que faz o papel da biblioteca cliente: os testes do pacote o entregam à API como um
pipeline entregaria os seus modelos. Um teste que não grava (esquema, renderização, DuckDB em
memória) roda sem variável. Um teste que grava usa a fixture `local_location`, sob
`SERIALIZE_DB_TEST_LOCAL_ROOT`, e é pulado sem ela; o fim da sessão imprime, sem erro, o comando que
autoriza cada suíte pulada e o que ela grava. O marcador `s3` repete no bucket os testes que dependem
do armazenamento, sob `SERIALIZE_DB_TEST_S3_ROOT`; o marcador `redshift` roda só com
`SERIALIZE_DB_TEST_REDSHIFT_SCHEMA`, o esquema onde a suíte pode criar tabelas
`serialize_db_test_<id>_*`, e conecta pelas variáveis `SERIALIZE_DB_REDSHIFT_*`. Os arquivos gerados
do modelo de referência (`tests/model/schema/` e `tests/model/sql/`, o que um pipeline versionaria na
raiz do seu repositório) são comparados por teste com uma geração nova, sem gravar.

## Etapas

Cada etapa entrega um módulo com testes. As etapas 1 a 4 e 6 rodam em pastas locais, sem AWS; a
etapa 5 e a parte Redshift da etapa 0 exigem a conexão; a etapa 7 exige os Parquet de origem.

| Etapa | Entrega | Critério de aceite |
| --- | --- | --- |
| 0. Prova de conceito na AWS | `tests/proof_of_concept/`: S3 verificado; no Redshift, a conexão e a escrita no datashare provadas por `examples/`, e a suíte pendente. | Cada item respondido em `delta.md` e `redshift.md`; nenhum bloqueio sem alternativa. |
| 1. `schema` | Modelo de referência corrigido; esquema Arrow, Delta e DDL; cast; os arquivos `schema/` do modelo de referência. | `create_all` no DuckDB em memória passa; o teste de diff falha quando um modelo muda sem regenerar; `cast` recusa perda de precisão, `double` fora da escala, texto longo e nulo em `NOT NULL`. |
| 2. `sql` | `param`, `prefixed`, `render`, `bind`, `write_sql_files`. | O texto de um statement com parâmetro, `%` em literal e prefixo roda no DuckDB com `$nome`; o teste de diff dos arquivos `sql/`. |
| 3. `storage` e `delta` | Os dois armazenamentos; a camada Delta inteira. | Testes locais de substituição da partição, conflito, reconciliação aditiva e destrutiva, reescrita num commit, `keep_versions`, exportação por partição e realocação; os mesmos no bucket com `-m s3`. |
| 4. `audit` e motor DuckDB | As verificações do contrato e seu texto por dialeto; conexão, ingestão, consulta, execução de texto, carga, auditoria, exportação da partição. | O pipeline de exemplo roda em memória sobre um Delta local; a auditoria reprova a chave repetida entre a partição nova e uma já publicada. |
| 5. Motor Redshift | O mesmo protocolo com sandbox `exec_<id>_`, `COPY ... MANIFEST` e `UNLOAD`. | SQL gerado coberto por testes sem cluster; integração com amostra, marcador `redshift`. |
| 6. Execução e linha de comando | `Database`, `Execution`, `serialize-db run`. | Reexecução idempotente; auditoria reprovada não altera o Delta; conflito abortado com mensagem. |
| 7. Carga inicial | Migração dos Parquet atuais por tabela e por partição, com relatório. | Contagens e somas por partição iguais entre origem e Delta. |
| 8. Publicação para clientes | Tabelas `<ambiente>_*` no Redshift, `version_diff`, transação única, `serialize_db_publications`. | Uma partição alterada recarrega só essa partição. |
| 9. Operação | Snapshots, `vacuum`, compactação, arquivo, exportação, `history`, runbook, `pdoc`. | Runbook escrito e testes de manutenção passando. |

O plano de cada etapa, com as primitivas do módulo, os testes e as provas de conceito, está num
arquivo próprio:

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
| 1. Abertura | Lê `_serialize_db/snapshots.json` e `serialize_db_publications`; abre cada tabela de entrada e registra a versão. | `versions = {cad_lancamentos: 143, cad_contratos: 88, ...}` gravado no log da execução. |
| 2. Ingestão | DuckDB: views com os nomes dos modelos sobre `delta_scan(uri, version := 143)`; `cad_lancamentos` materializada com `WHERE data_base_str BETWEEN '2025-09-30' AND '2026-08-31'`; dimensões como views. Redshift: `COPY ... MANIFEST` dos arquivos dessas partições em `exec_2026_09_05_cad_lancamentos`, via staging. | Sandbox em `/tmp/exec-2026-09-05.duckdb` ou tabelas com prefixo no esquema único. |
| 3. Execução | O pipeline roda statements Core, texto gerado e lógica Python sobre o sandbox; o que sai para o Python sai em lotes por `stream`, ou como `pa.Table` por `query` ou `execute`, e volta por `loader` ou `load`; intermediários ficam no sandbox. | Tabela `cad_lancamentos_projetados` no sandbox, partição 2026-08-31. |
| 4. Auditoria | Contagem, nulos, unicidade da chave contra as demais partições da versão 57, `data_base_str = strftime(data_base, '%Y-%m-%d')`, limites de tipo, `json_valid`, totais de controle. | Relatório com o SQL de cada verificação no log da execução; reprovação encerra sem tocar o Delta. |
| 5. Publicação no Delta | `reconcile` e `publish_partition(uri, "2026-08-31", data, commit_metadata(...))`; do Redshift, `UNLOAD ... PARTITION BY (data_base_str)` mais `register_files`. | `cad_lancamentos` projetado passa da versão 57 para 58; um arquivo em `data_base_str=2026-08-31/`. |
| 6. Publicação no Redshift | `version_diff(57, 58)` aponta a partição 2026-08-31; `DELETE` da partição, `COPY ... MANIFEST` na staging, `INSERT ... SELECT *, '2026-08-31'`; controle atualizado. | `prod_cad_lancamentos_projetados` com a partição nova; `serialize_db_publications` em 58. |
| 7. Snapshot do banco | Só na execução marcada, por exemplo a do fim do trimestre: `serialize_db_snapshot = "2026T3"` nos commits e a entrada em `_serialize_db/snapshots.json`. | Versões do snapshot protegidas por `keep_versions`. |
| 8. Encerramento | Sandbox descartado, staging apagado, resumo no log. | Execução idempotente: repetir os passos 5 e 6 reproduz o mesmo estado. |

A API da etapa 6:

```python
import pandas as pd
import pyarrow as pa
from sqlalchemy import select

from serialize_db import Database, Execution
from pipeline import compute_in_sandbox, project
from pipeline.models import Contrato, Lancamento, LancamentoProjetado, Operacao, RelContratoOperacao

db = Database("s3://bucket/projeto/delta", environment="prod")
with Execution(db, engine="duckdb", partition="2026-08-31", execution_id="exec-2026-09-05") as run:
    run.ingest(Lancamento, partitions=run.previous_partitions(Lancamento, 12), materialize=True)
    run.ingest(Contrato, Operacao, RelContratoOperacao)              # views sobre a versão fixada
    compute_in_sandbox(run.sandbox)                                  # statements Core e texto gerado, por query e execute
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
`register_files`, sem passar pela máquina local.

## Ordem do trabalho

1. Etapas 1 e 2, em pastas locais, com o modelo de referência corrigido e os seus arquivos
   `schema/` e `sql/` versionados em `tests/model/`.
2. Etapa 3, depois 4 e 6: um pipeline completo em disco local, o critério de aceite da etapa 6
   sobre o motor DuckDB.
3. Em paralelo, no ambiente alvo: os probes, que rodaram no laboratório em 2026-09-20 com as suítes
   local e S3, e dos quais `redshift.py` rodou no ambiente alvo no mesmo dia ([`POC.md`](POC.md)); a
   manutenção da suíte S3 se confirmada; `tests/proof_of_concept/` e os testes `-m s3` das etapas 3
   e 4 no bucket.
4. Com a conexão mostrada por `probes/redshift.py` e por `examples/` em 2026-09-20: o
   `test_redshift.py` da etapa 0 no ambiente alvo, depois as etapas 5 e 8.
5. Etapa 7 quando os Parquet de origem estiverem acessíveis; etapa 9 por último, com o runbook.
