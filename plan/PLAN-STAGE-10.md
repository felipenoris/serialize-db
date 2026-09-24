# Etapa 10: acesso de leitura

A entrega e o critério de aceite desta etapa estão na tabela de etapas de [`PLAN.md`](PLAN.md), que
também fixa as decisões, as regras que toda etapa obedece e a ordem do trabalho.

A etapa dá ao código cliente que tem o modelo a leitura da base por statements Core, com o
resultado em Arrow, em duas origens: a base Delta, por um DuckDB no processo com uma view por
tabela, e a base publicada no Redshift, pelas tabelas `<ambiente>_<tabela>` da
[etapa 8](PLAN-STAGE-8.md). O módulo é `serialize_db.reader`. O time que tem o `Database` entra por
`db.open_delta()` e `db.open_redshift(config)`; o cliente que só enxerga o Redshift entra por
`serialize_db.reader.open_redshift`, sem `Database`. O mesmo statement roda nas duas origens.

## As decisões do usuário

Tomadas em 2026-09-24, na conversa que propôs a etapa:

- O leitor é um objeto próprio, aberto a partir do `Database`, com a implementação em
  `serialize_db.reader` e os nomes `open_delta` e `open_redshift`. Ele não é `Execution`, que fixa
  partição e `execution_id`, grava snapshot e publica. Ele também não é uma `Connection` do
  SQLAlchemy, que devolve linhas e não troca `cad_lancamentos` por `prod_cad_lancamentos`.
- O resultado é Arrow, como nos motores: `query` devolve a `pa.Table`, e
  `to_pandas(types_mapper=pd.ArrowDtype)` a leva ao pandas, pela regra da troca de dados de
  2026-09-20 ([`PLAN.md`](PLAN.md), seção "A troca de dados com o código cliente").
- As duas origens servem a times diferentes. Os clientes com acesso só de leitura à base publicada
  no Redshift informam um destino próprio para o `UNLOAD`.
- A versão padrão do leitor Delta é o último snapshot do ambiente, e outro modo lê a versão atual
  de cada tabela.
- A materialização de parte das partições é permitida: o nome do modelo passa a ter só essas
  partições.

## Interface

As assinaturas em aberto estão em "Decisões pendentes": a forma do modo da versão atual (B) e a
entrada do cliente do Redshift (C).

```python
# serialize_db/execution.py
class Database:
    def open_delta(self, snapshot: str | None = None, current: bool = False,
                   config: DuckDBConfig | None = None) -> DeltaReader: ...
    def open_redshift(self, config: RedshiftConfig,
                      unload_to: str | None = None) -> RedshiftReader: ...


# serialize_db/reader.py
def open_redshift(metadata: sa.MetaData, environment: str, config: RedshiftConfig,
                  unload_to: str) -> RedshiftReader: ...


class DeltaReader:
    snapshot: str | None                          # o snapshot lido; None no modo da versão atual
    versions: dict[str, int]                      # a versão de cada tabela com view
    materialized: dict[str, list[str] | None]     # as tabelas locais e as partições de cada uma

    def materialize(self, *tables: sa.Table, partitions: list[str] | None = None) -> None: ...
    def query(self, statement_or_sql: sa.sql.ClauseElement | str,
              params: Mapping[str, object] | None = None) -> pa.Table: ...
    def stream(self, statement_or_sql: sa.sql.ClauseElement | str,
               params: Mapping[str, object] | None = None,
               batch_size: int = 100_000) -> BatchStream: ...
    def session(self) -> contextlib.AbstractContextManager[duckdb.DuckDBPyConnection]: ...
    def close(self) -> None: ...                  # e __enter__, __exit__


class RedshiftReader:
    environment: str

    def query(...) -> pa.Table: ...               # as assinaturas do DeltaReader
    def stream(...) -> BatchStream: ...
    def session(self) -> contextlib.AbstractContextManager[object]: ...
    def close(self) -> None: ...                  # e __enter__, __exit__
```

O uso, com o `stmt` de um `select` Core ou ORM do modelo:

```python
import pandas as pd

from serialize_db import Database
from serialize_db.engine.redshift import RedshiftConfig
from serialize_db.reader import open_redshift

db = Database("s3://bucket/projeto/delta", "prod", Base.metadata)

with db.open_delta() as reader:                     # o último snapshot
    reader.materialize(Conta.__table__)
    reader.materialize(Lancamento.__table__, partitions=["2026-08-31"])
    frame = reader.query(stmt).to_pandas(types_mapper=pd.ArrowDtype)
    reader.versions                                 # {"cad_lancamentos": 143, ...}

reader = db.open_delta(current=True)                # a versão atual, sem with, num caderno
reader.close()

with db.open_redshift(RedshiftConfig.from_environment()) as reader:   # o time
    frame = reader.query(stmt).to_pandas(types_mapper=pd.ArrowDtype)

config = RedshiftConfig.from_environment()          # o cliente, sem acesso à raiz Delta
with open_redshift(Base.metadata, "prod", config, "s3://bucket-do-cliente/tmp") as reader:
    with reader.stream(stmt) as batches:            # resultado grande, pelo UNLOAD
        for batch in batches:
            work(batch)
```

## Estratégia de implementação

### O leitor Delta

- **As versões** são lidas na abertura, em três modos:
  - Sem argumento, o leitor usa o último snapshot do arquivo de controle
    `<ambiente>/_serialize_db/snapshots.json` (`delta.read_snapshots`). A escolha depende da data
    que `delta.snapshot` passa a gravar (decisão pendente A). O ambiente sem snapshot é
    `ContractError`, e a mensagem aponta o modo da versão atual.
  - Com `snapshot="2026T3"`, o leitor usa a entrada com esse nome, e o nome ausente é
    `ContractError`. O nome em `archived` depende da decisão pendente D: o `vacuum` deixa de
    preservar as versões do snapshot arquivado, e a cópia dele fica em
    `arquivo/<nome>/<tabela>`.
  - Com `current=True`, o leitor usa a versão atual de cada tabela do modelo que existe no ambiente,
    como `Execution` na abertura; `snapshot` e `current` juntos são `ContractError`.
- **O motor** é um `DuckDBEngine` com o `DuckDBConfig` recebido. O padrão é o da execução: os
  limites lidos do ambiente e uma pasta temporária nova. O identificador é
  `reader-<AAAA-MM-DD>-<uuid8>`, e o `storage` é o do `Database`.
- **As views** são uma por tabela do modelo presente nas versões, pelo `ingest` do motor
  (`CREATE VIEW "<tabela>" AS SELECT * FROM delta_scan(uri, version := v)`). As tabelas correm em
  paralelo, uma sessão a mais por tabela, como `run.ingest`. A criação da view lê o log da tabela,
  e a versão inexistente falha nela, antes da primeira consulta (sonda de 2026-09-24,
  [`POC.md`](POC.md)). A tabela do modelo ausente das versões fica sem view: ela foi criada depois do
  snapshot ou ainda não existe. A view expõe as colunas da versão lida, e a coluna que o modelo
  ganhou depois dela é `BinderException` do DuckDB na consulta que a cita.
- **`materialize(*tables, partitions=None)`** troca, por tabela e numa transação, a view (ou a
  tabela de uma materialização anterior) por
  `CREATE TABLE "<tabela>" AS SELECT * FROM delta_scan(...)`. O filtro das partições é o do `ingest`:
  o intervalo ao lado do `IN`. A falha devolve o objeto anterior pelo `ROLLBACK`. As tabelas correm
  em paralelo, uma sessão a mais por tabela; a sonda de 2026-09-24 trocou duas em cursores paralelos
  sem conflito. Com `partitions`, o nome passa a ter só essas partições (decisão do usuário), e
  `materialized` e o log registram quais. Uma chamada seguinte troca a tabela de novo. O `ingest` do
  motor recusa o nome ocupado e confere o nome num cursor à parte, que não vê o `DROP` ainda não
  confirmado. Por isso o leitor monta o `CREATE TABLE` com o texto do `delta_scan` e o filtro das
  partições do motor, que passam de privados a protegidos.
- **`query`, `stream` e `session()`** são os do motor, depois da regra de leitura comum.
- **`close()`** chama o `cleanup` do motor, que apaga o banco e a pasta de transbordo; a segunda
  chamada não faz nada. O DuckDB só devolve a memória no `close` (instrução do usuário de
  2026-09-24), e a docstring avisa o usuário de caderno.

### O leitor Redshift

- **`open_redshift(metadata, environment, config, unload_to)`** abre um `RedshiftEngine` sobre a
  conexão de `config`, com o prefixo `<ambiente>_` no lugar de `exec_<id>_`, o `Storage` de
  `unload_to` e a pasta `<id do leitor>` sob ele. O motor ganha o argumento `prefix`, que tem
  `sandbox_prefix(execution_id)` por padrão. `Database.open_redshift(config, unload_to=None)` chama
  a função com `db.metadata`, `db.environment` e, sem `unload_to`, `<raiz>/<ambiente>/staging`.
- **`query`** vai pelo caminho do cursor do motor (`compiled_for_cursor` com o prefixo). O driver
  materializa o resultado no `execute`, e o motor monta a `pa.Table` a partir de objetos Python:
  o caminho serve a resultados pequenos.
- **`stream`** é o `UNLOAD ... PARALLEL OFF` do motor para
  `<unload_to>/<id do leitor>/stream/<uuid>/`, lido pelo `Storage`. O `close` do stream apaga os
  arquivos dele, e o `close` do leitor apaga `<unload_to>/<id do leitor>/`; nada fora dessa pasta é
  apagado.
- **O leitor não expõe** `ingest`, `loader`, `load`, `published`, `audit` nem `export_partition`,
  porque eles gravariam no esquema com o prefixo das tabelas publicadas.
- **A consistência entre tabelas** não é garantida. Cada tabela é publicada numa transação própria
  ([etapa 8](PLAN-STAGE-8.md)), e uma consulta que junta duas tabelas durante uma publicação pode
  ler versões diferentes. A leitura consistente entre tabelas é a do leitor Delta sobre um snapshot.

### A regra de leitura comum

- O statement Core que não é `Select` nem `CompoundSelect` é `ContractError`, e o motor não é
  chamado. O texto pronto roda como está; o comando que não é consulta vai por `session()`.
- No leitor Delta, a tabela do modelo que o statement Core cita (`sql.referenced_tables`) e que não
  tem view é `ContractError`, com o nome dela e a origem das versões; o texto pronto recebe o erro
  de catálogo do DuckDB. No leitor Redshift, o servidor responde a tabela não publicada com `XX000`
  e `Relation <nome> does not exist in the database.`.
- O `delta_scan` abre todos os arquivos num `IN` de mais de um valor na coluna de partição, também
  pela view (sonda de 2026-09-24). O `=`, o `BETWEEN` e o `IN` ao lado do intervalo podam. A
  documentação do leitor recomenda essas formas; o leitor não reescreve o filtro do cliente.

## Pré-requisitos e pós-condições

- **O leitor Delta** precisa de leitura na raiz do banco (as tabelas e o arquivo de controle) e da
  extensão `delta` do DuckDB. Ele grava só na pasta temporária do motor, apagada no `close`, e nada
  na raiz.
- **O leitor Redshift** precisa de `SELECT` nas tabelas publicadas do esquema. Para `stream`,
  precisa também do `UNLOAD` com o papel de `config.iam_role` ou as credenciais da sessão `boto3`,
  e de gravação, leitura e exclusão em `unload_to`. Nada é criado no esquema.

## Testes por caso

`tests/test_reader.py`, sob a raiz local (`local`):

- O último snapshot pela data, entre dois snapshots gravados; um único snapshot sem data; mais de
  um snapshot sem data e nenhum com data, `ContractError`; o ambiente sem snapshot,
  `ContractError`.
- O snapshot nomeado; o nome ausente, `ContractError`; o nome arquivado, pela decisão D.
- A versão atual; `snapshot` com `current`, `ContractError`.
- Um commit depois da abertura não aparece na leitura.
- A tabela criada depois do snapshot fica sem view, e a consulta que a cita é `ContractError`.
- `materialize` da tabela inteira e de parte das partições: as linhas, `materialized`, a troca
  numa segunda chamada, duas tabelas em paralelo, e a falha provocada (um arquivo de dados apagado)
  que devolve a view.
- `query` e `stream` com os tipos Arrow do contrato nas colunas do modelo, e o ciclo com
  `to_pandas(types_mapper=pd.ArrowDtype)`.
- O `delete`, o `update` e o `insert` Core recusados com `ContractError`.
- A poda pela view no log `FileSystem` do DuckDB: o `=` e o `BETWEEN` abrem só as pastas das
  partições, e o `IN` de dois valores abre todas.
- O `close` apaga o banco, e a segunda chamada não faz nada.
- `delta.snapshot` grava a data, em `tests/test_delta.py`.

`tests/test_reader.py`, sem conexão: o texto compilado com o prefixo `<ambiente>_` e a regra de
leitura comum no leitor Redshift, sobre a conexão de mentira de `tests/test_engine_redshift.py`.

`tests/test_reader.py`, com `redshift`, `s3` e `local`, no substituto local e no ambiente alvo: a
publicação de `Lancamento` num ambiente `poc<id>`, como `tests/test_publication.py`, e o leitor de
`open_redshift` com `unload_to` no bucket da suíte. `query` devolve o mesmo resultado do leitor
Delta para o mesmo statement, e `stream` o mesmo de `query`. Depois do `close`, nada fica sob
`<unload_to>/<id do leitor>/`.

## As execuções no ambiente alvo

- **O tempo de abertura do leitor Delta** sobre as 12 tabelas da raiz carregada, com as views em
  paralelo. Só a pasta local foi medida: 8,7 ms por view (sonda de 2026-09-24).
- **O `UNLOAD` do cliente** para um bucket próprio com um usuário do Redshift só de leitura. A
  execução mostra se o `UNLOAD` é aceito para quem só tem `SELECT` e qual caminho de credencial
  serve, o `iam_role` do cliente ou as credenciais da sessão. Ela precisa de um papel de cliente no
  ambiente alvo.

## Decisões pendentes

- **A. A data do snapshot.** O arquivo de controle guarda `{nome: {tabela: versão}}` sem data, e
  `_write_control` grava as chaves em ordem alfabética, então nada nele diz qual snapshot é o
  último. A proposta é que `delta.snapshot` grave o instante UTC numa chave irmã `created_at`
  (`{"created_at": {"2026T3": "2026-09-24T14:16:00+00:00"}}`), como `archived`. O último snapshot
  seria o de `created_at` mais recente em `snapshots`, e uma entrada sem data (gravada antes da
  mudança) seria mais antiga que qualquer outra com data. Mais de uma entrada sem data e nenhuma
  com data seria `ContractError`, que pede o nome. A alternativa é um ponteiro `latest` que
  `delta.snapshot` atualiza. No ambiente alvo, o único snapshot, `carga-2026-09-24`, foi para
  `archived` na bateria das 16:51 ([`POC.md`](POC.md)): sem snapshot novo, o leitor sem
  argumento é `ContractError` lá, e só o modo da versão atual lê a raiz.
- **B. A forma do modo da versão atual.** A proposta é `db.open_delta(current=True)`: um método para
  a origem Delta, com os três modos na mesma docstring. A alternativa, pela regra do repositório que
  prefere funções separadas a flags booleanas, é `db.open_delta_current()`.
- **C. A entrada do cliente do Redshift.** A proposta é
  `serialize_db.reader.open_redshift(metadata, environment, config, unload_to)`, com `unload_to`
  obrigatório, porque o cliente não tem a raiz Delta que o `Database` exige, e
  `Database.open_redshift(config, unload_to=None)` para o time, com o padrão
  `<raiz>/<ambiente>/staging`. Ela espera a confirmação do usuário.
- **D. O snapshot arquivado.** O `archive` copia cada tabela do snapshot para
  `arquivo/<nome>/<tabela>` e move a entrada para `archived`, e o `vacuum` deixa de preservar
  as versões dela na tabela viva. A proposta é que `open_delta(snapshot=<nome arquivado>)` crie
  as views sobre a cópia, na versão atual de cada cópia, que não muda depois do `archive`; o
  último snapshot do modo sem argumento continua a ser procurado só em `snapshots`. A
  alternativa é recusar o nome arquivado com `ContractError`.
