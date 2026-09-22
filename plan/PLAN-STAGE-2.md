# Etapa 2: `sql`

A entrega e o critério de aceite desta etapa estão na tabela de etapas de [`PLAN.md`](PLAN.md), que
também fixa as decisões, as regras que toda etapa obedece e a ordem do trabalho.

O módulo `serialize_db.sql` gera o texto SQL de cada dialeto a partir de um statement Core, para a
substituição gradual da compilação em tempo de execução descrita em `sqlalchemy.md`.

| Primitiva | O que faz |
| --- | --- |
| `param(name, type_=None)` | `literal_column(":nome", type_)`: o parâmetro de execução, que atravessa `literal_binds` e chega ao texto como `:nome`. |
| `prefixed(statement, metadata, prefix="{prefix}")` | A cópia do statement com cada tabela do contrato trocada pela cópia com o prefixo, por `replacement_traverse`; todo nome da cópia vai citado (`quoted_name(quote=True)`), o sentinela dentro das aspas como no DDL. |
| `render(statement, dialect, metadata, prefix="{prefix}")` | O texto de `duckdb` ou `redshift` com as constantes embutidas e os parâmetros como `:nome`, compilado por um dialeto com `paramstyle="named"`, que não dobra o `%` dos literais; um `bindparam` sem valor é erro, porque o compilador o renderia como `NULL`; nenhuma linha termina em espaço, para o arquivo versionado sobreviver a um editor que apara o fim das linhas. |
| `bind(sql, params, style)` | O texto com `:nome` reescrito para o estilo do motor (`$nome` no DuckDB; inalterado no `redshift_connector` com `paramstyle = "named"`) e o dicionário conferido: parâmetro faltante ou sobrando é erro, e um texto que ainda traz `{prefix}` também. Toda região citada passa intacta, entre aspas simples ou duplas. |
| `referenced_tables(statement_or_sql)` | As tabelas do contrato que um statement Core (`find_tables`) ou um texto gerado (o sentinela) cita, para o log da execução. |
| `sql_files(statements, metadata)` | `{"<nome>.duckdb.sql": ..., "<nome>.redshift.sql": ...}` de um dicionário `{nome: statement}`. |
| `write_sql_files(statements, metadata, directory)` | Grava `sql_files`; `serialize-db sql write` e `serialize-db sql check`. |
| `read_sql(directory, name, dialect, prefix)` | O texto versionado com o sentinela `{prefix}` trocado pelo `prefix` informado, para o `execute` dos motores. |

Testes: `tests/test_sql.py`, sem gravar fora do caso `local`: o statement de `sqlalchemy.md` (parâmetro, `%` em literal,
prefixo) renderizado nos dois dialetos e executado no DuckDB em memória com `$mes`; `bindparam` sem
valor e parâmetro faltante como erros; o diff de `tests/client_model/sql/`, gerado dos statements do
pipeline fictício em `tests/client_model/statements.py` (`STATEMENTS`, o dicionário `{nome: statement}`
que `serialize-db sql` recebe por `--statements`). `sqlglot.parse_one(texto, dialect="redshift")` sobre o texto do Redshift de cada statement, o
arquivo versionado com o sentinela inclusive, como teste de que o texto gerado para o Redshift
analisa (decisão do usuário de 2026-09-22; `sqlglot==30.18.0` no grupo `dev`, a versão do ensaio
de 2026-09-21; o sentinela dentro das aspas de um identificador analisa, leitura de 2026-09-22 em
[`POC.md`](POC.md)). Provas de conceito:
`test_sqlalchemy.py` (`test_generated_sql_text_per_dialect`, `test_redshift_dialect_compiles_dml`) e
`test_stdlib.py::test_generated_files_diff`.

## Interface

```python
"""Assinaturas de serialize_db.sql; os corpos estão no rascunho abaixo."""
import sqlalchemy as sa

from serialize_db.errors import SqlError   # ValueError: statement sem texto executável, ou parâmetros que não fecham
from serialize_db.schema import Dialect    # Literal["duckdb", "redshift"], definido uma vez na etapa 1


def param(name: str, type_: sa.types.TypeEngine | None = None) -> sa.ColumnElement: ...
def prefixed(statement: sa.sql.ClauseElement, metadata: sa.MetaData, prefix: str = "{prefix}") -> sa.sql.ClauseElement: ...
def render(statement: sa.sql.ClauseElement, dialect: Dialect, metadata: sa.MetaData, prefix: str = "{prefix}") -> str: ...
def bind(sql: str, params: dict[str, object], style: Dialect) -> tuple[str, dict[str, object]]: ...
def referenced_tables(statement_or_sql: sa.sql.ClauseElement | str) -> set[str]: ...
def sql_files(statements: dict[str, sa.sql.ClauseElement], metadata: sa.MetaData) -> dict[str, str]: ...
def write_sql_files(statements: dict[str, sa.sql.ClauseElement], metadata: sa.MetaData, directory: str) -> list[str]: ...
def check_sql_files(statements: dict[str, sa.sql.ClauseElement], metadata: sa.MetaData, directory: str) -> list[str]: ...
def read_sql(directory: str, name: str, dialect: Dialect, prefix: str) -> str: ...
```

`referenced_tables` entra aqui, e não na [etapa 6](PLAN-STAGE-6.md): as tabelas de um statement
Core saem de `find_tables(include_crud=True)`, as de um texto gerado saem do sentinela `{prefix}`,
e o log da execução as registra por comando. É o esboço de `test_parallel.py`, e a barreira por
tabela de [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md) a usaria.

O módulo segue a seção "Python Code Style" de `CLAUDE.md`, como o da [etapa 1](PLAN-STAGE-1.md):
uma função por responsabilidade, anotação de tipo em toda assinatura, docstring com um exemplo em
cada função pública, laços explícitos no lugar de comprehensions com dois `for`, nenhum `lambda`,
nenhum decorador próprio e nenhum estado global do processo mudado para controlar o fluxo. As
assinaturas acima são o `__all__`. `SENTINEL` (`"{prefix}"`) fica protegido, sem prefixo e fora do
`__all__`, porque as etapas [4](PLAN-STAGE-4.md) e [5](PLAN-STAGE-5.md) o leem; `_DIALECTS`,
`_MARKERS`, `_PARAMETER_NAME`, `_QUOTED_OR_PLACEHOLDER`, `_SENTINEL_TABLE`, `_prefixed_copy`,
`_parameters_without_value`, `_placeholders` e `_versioned_text` são privados. `SqlError` entra em
`serialize_db.errors` ao lado de `ContractError`, e `Dialect` é o de `serialize_db.schema`. O
rascunho abaixo tem essa forma e é a referência do módulo; ele define `Dialect` e `SqlError`
localmente só para rodar sozinho.

## Estratégia de implementação

- **`param`** valida o nome (`[a-z_][a-z0-9_]*`) e devolve `literal_column(":nome", type_)`; o
  tipo serve à compilação de comparações com colunas tipadas.
- **`prefixed`** monta, para cada tabela do contrato, uma cópia só com o que a compilação de um DML
  usa (`_prefixed_copy`): o nome com o prefixo e cada coluna com nome e tipo, todos em
  `quoted_name(..., quote=True)`; chaves e índices ficam de fora, porque um `SELECT` ou um
  `INSERT ... SELECT` não os compila. Com todo nome citado, o DML cumpre a regra de
  [`PLAN.md`](PLAN.md) que o DDL da etapa 1 já cumpre — `"{prefix}cad_contas"."numero"`, com o
  sentinela dentro das aspas como no DDL — sem depender da lista de palavras reservadas de nenhum
  dialeto (decisão do usuário de 2026-09-21, medição em [`POC.md`](POC.md): com `quote=False` o
  texto citava só `"to"` nos dois dialetos e `"timestamp"` só no Redshift). Os rótulos e o resto
  do statement continuam citados como o dialeto exige, porque são do cliente. `replacement_traverse` chama uma função em cada nó do
  statement e deixa o nó como está quando ela devolve `None`; a função troca cada `Table` do
  contrato pela cópia e cada `Column` pela coluna de mesmo nome na cópia. O statement original não
  muda.
- **`render`** compila a cópia prefixada duas vezes com um dialeto avulso de `paramstyle="named"`,
  que não dobra o `%` dos literais. A primeira, sem `literal_binds`, lê em `compiled.binds` os
  `bindparam` com `required=True`, os sem valor, que a segunda renderizaria como `NULL`; havendo
  algum, `SqlError` os nomeia e manda usar `param`. A segunda, com `literal_binds=True`, é o texto,
  sem o espaço que o compilador deixa antes de cada quebra de linha, para o arquivo versionado
  sobreviver a um editor que apara o fim das linhas (2026-09-22, [`POC.md`](POC.md)).
  O rascunho anterior transformava o `SAWarning` dessa renderização em exceção com
  `warnings.catch_warnings`, que troca o filtro de avisos do processo inteiro: a documentação do
  módulo `warnings` o declara inseguro num programa com threads abaixo do Python 3.14, e o projeto
  roda 3.13 com os motores das etapas [4](PLAN-STAGE-4.md) e [5](PLAN-STAGE-5.md) chamando `render`
  em threads ([`POC.md`](POC.md)). Os dialetos são `duckdb_engine.Dialect` e
  `RedshiftDialect_redshift_connector` (decisão do usuário de 2026-09-21): como os motores chamam
  `render` em tempo de execução, `duckdb-engine` e `sqlalchemy-redshift` saem do grupo `dev` e
  entram nas dependências de execução de `pyproject.toml` no commit que escrever o módulo, com
  `prepare_offline.sh` rodado de novo. A alternativa medida em 2026-09-21 — o dialeto `postgresql`
  do próprio SQLAlchemy, que compila o mesmo texto fora a citação de `"timestamp"`
  ([`POC.md`](POC.md)) — fica registrada e não adotada.
- **`bind`** recusa um texto que ainda traga o sentinela `{prefix}`, porque sem a troca ele chegaria
  ao motor como erro de sintaxe (decisão do usuário de 2026-09-21; `read_sql` o preenche). Depois
  lê os nomes dos marcadores com `_placeholders`, que percorre o texto com uma expressão que
  consome primeiro as regiões citadas — o literal entre aspas simples (com `''` escapado) e o
  identificador entre aspas duplas (com `""` escapado) — e só depois reconhece `:nome` fora de
  `::cast`. Aspas simples e duplas delimitam conteúdo literal, e `render` põe o marcador só onde vai
  um valor: pular toda região citada não perde marcador algum (decisão do usuário de 2026-09-21).
  Sem a alternativa das aspas duplas, uma coluna chamada `taxa :base` saía como `"taxa $base"` com
  o parâmetro `base` inventado, e uma chamada `preco d'agua` abria uma região falsa de literal que
  ia até a aspa simples seguinte do texto e engolia o `:nome` do meio (medições de 2026-09-21). O
  conjunto de nomes precisa ser igual ao do dicionário: faltante e sobrando são `SqlError`. Só então
  a mesma expressão reescreve cada marcador, `$nome` no estilo `duckdb` e `:nome` no `redshift`,
  que o `redshift_connector` lê com `cursor.paramstyle = "named"`. Ler os nomes e reescrever são
  dois passos com a mesma expressão, no lugar de uma função de substituição que acumula nomes num
  conjunto de fora.
- **`referenced_tables`** é o esboço de `test_parallel.py`: `find_tables(include_crud=True)` num
  statement Core, o sentinela seguido do nome num texto gerado; só nomes de `sa.Table`.
- **`sql_files`** renderiza cada statement nos dois dialetos, um laço com uma linha por arquivo,
  sempre com o sentinela `{prefix}` e com `\n` final, porque o arquivo versionado serve a qualquer
  alvo; `write_sql_files` e `check_sql_files` repetem o padrão da [etapa 1](PLAN-STAGE-1.md).
- **`read_sql`** abre `<pasta>/<nome>.<dialeto>.sql` com `encoding="utf-8"`, como a etapa 1, e troca
  `{prefix}` pelo `prefix` informado, argumento obrigatório (decisão do usuário de 2026-09-21): quem
  chama sabe o alvo — a string vazia para as tabelas do contrato, `exec_<id>_` para o sandbox da
  execução —, e a obrigação de informar sobe para os chamadores, os motores das etapas
  [4](PLAN-STAGE-4.md) e [5](PLAN-STAGE-5.md) e o pipeline. Nenhuma outra primitiva preenche o
  sentinela.
- **`serialize-db sql write|check`** recebe `--metadata modulo:atributo`, `--statements
  modulo:atributo` (o dicionário `{nome: statement}` do pipeline, resolvido pelo mesmo caminho de
  `--metadata`) e a pasta; `check` imprime o diff e sai com 1 quando há diferença, como
  `schema check`.

## Pré-requisitos e pós-condições

| Primitiva | Pré-requisitos | Pós-condições |
| --- | --- | --- |
| `param` | Nome válido como identificador. | Um `literal_column` que atravessa `literal_binds` como `:nome`. |
| `prefixed` | Toda tabela do statement pertence ao `metadata` informado. | Uma cópia com cada tabela e coluna trocadas; o original intacto. |
| `render` | Statement sem `bindparam` sem valor. | Texto com as constantes embutidas, `%` dos literais simples e toda tabela e coluna do contrato entre aspas, o `{prefix}` dentro delas; o mesmo texto nos dois dialetos para o SQL portável; nenhuma linha termina em espaço. |
| `referenced_tables` | Statement Core, ou texto gerado com o sentinela. | Os nomes das tabelas do contrato que ele cita. |
| `bind` | Texto com `:nome` fora das regiões citadas e sem `{prefix}`. | O texto no estilo do motor e o dicionário conferido, com literais e identificadores citados intactos; `SqlError` quando os nomes não fecham. |
| `write_sql_files` | Pasta gravável. | Dois arquivos por statement; `check_sql_files` vazio quando nada mudou. |
| `read_sql` | Arquivo gravado por `write_sql_files`; `prefix` informado. | O texto sem o sentinela, pronto para `bind`. |

## Testes por caso

`tests/test_sql.py`, sem gravar fora do caso `local`, com o statement de
[`sqlalchemy.md`](sqlalchemy.md), os quatro de `tests/client_model/statements.py` e os do rascunho.

| Caso | Teste | O que confere |
| --- | --- | --- |
| Texto por dialeto | `test_render_embeds_constants_and_keeps_parameters` | Constantes embutidas, `:nome` preservado, `%` simples em `LIKE`, toda tabela e coluna do contrato entre aspas nos dois dialetos com o sentinela dentro delas (`"{prefix}cad_contas"."numero"`, `"to"`, `"timestamp"`); DuckDB e Redshift iguais para o statement portável. |
| Execução | `test_rendered_text_runs_in_duckdb` | O texto com `prefix=""` e com `prefix="exec_42_"` passa por `bind` e roda num DuckDB em memória com `$nome`, sobre o DDL da etapa 1; os quatro statements de `STATEMENTS` rodam sobre as 12 tabelas do modelo cliente, e `veiculos_novos` insere o veículo do lançamento uma vez só. |
| Literais com dois-pontos | `test_bind_leaves_quoted_literals_and_casts_alone` | `'TI:%'`, `'12:30'` e `valor::DECIMAL(18, 2)` intactos; só `:nome` do dicionário muda. |
| Identificadores citados | `test_bind_leaves_quoted_identifiers_alone` | As colunas `taxa :base`, `:base` e `preco d'agua` saem intactas entre aspas duplas, e o `:nome` fora das aspas é o único trocado. |
| Parâmetros | `test_bind_refuses_missing_and_extra_parameters` | Faltante e sobrando são `SqlError` com os dois conjuntos na mensagem. |
| `bindparam` sem valor | `test_render_refuses_bindparam_without_value` | `SqlError` em vez de `NULL` silencioso. |
| Prefixo | `test_prefixed_replaces_every_contract_table` | Tabelas e colunas trocadas em `select`, `insert ... from_select` e `join`; o statement original intacto. |
| Tabelas referenciadas | `test_referenced_tables_from_core_and_text` | `find_tables` e o sentinela dão o mesmo conjunto para o mesmo comando. |
| Arquivos gerados | `test_sql_files_match_versioned`, `test_check_sql_files_reports_a_changed_statement` | Diff vazio contra `tests/client_model/sql/`; uma coluna nova no statement aparece no diff. |
| Prefixo do arquivo | `test_read_sql_fills_the_sentinel` | `read_sql(..., prefix="")` dá `cad_lancamentos` e `prefix="exec_42_"` dá `exec_42_cad_lancamentos`; um `{prefix}` que sobra no texto é `SqlError` no `bind`. |
| Gravação | `test_write_sql_files` (`local`) | Os oito arquivos gravados sob a raiz local, o `check` vazio depois e `read_sql` sobre eles. |
| Linha de comando | `test_cli_sql_check_reads_the_versioned_files` | `serialize-db sql check --metadata client_model:Base.metadata --statements client_model.statements:STATEMENTS tests/client_model/sql` sai com 0 sem gravar; um statement mudado sai com 1 e imprime o diff; sem `--statements` sai com 2. |
| Redshift analisável | `test_redshift_text_parses_with_sqlglot` | `sqlglot.parse_one(texto, dialect="redshift")` aceita o texto do Redshift de cada statement de `STATEMENTS`, o arquivo versionado com o sentinela e o texto com o prefixo vazio (o sentinela fica dentro das aspas de um identificador, leitura de 2026-09-22), e recusa uma aspa desbalanceada com `TokenError`; o teste não diz o que o Redshift suporta nem vê um identificador estragado como `"taxa $base"` (ensaio de 2026-09-21, [`POC.md`](POC.md)). |

## Rascunhos executados

Rodou em 2026-09-21 com as versões fixadas.

```python
"""Etapa 2: param, prefixed, render, bind, referenced_tables, sql_files e read_sql sobre um
statement Core, com o texto executado no DuckDB com $nome."""
import os
import re
import tempfile
from typing import Literal

import duckdb
import duckdb_engine
import sqlalchemy as sa
from sqlalchemy.sql import quoted_name
from sqlalchemy.sql.util import find_tables
from sqlalchemy.sql.visitors import replacement_traverse
from sqlalchemy_redshift.dialect import RedshiftDialect_redshift_connector

Dialect = Literal["duckdb", "redshift"]      # o módulo importa o de serialize_db.schema


class SqlError(ValueError):                 # o módulo importa o de serialize_db.errors
    """Um statement que não vira texto executável, ou um texto cujos parâmetros não fecham."""


SENTINEL = "{prefix}"
# O compilador de cada dialeto, com paramstyle "named" para o % dos literais não sair dobrado.
_DIALECTS = {
    "duckdb": duckdb_engine.Dialect(paramstyle="named"),
    "redshift": RedshiftDialect_redshift_connector(paramstyle="named"),
}
# O marcador de parâmetro de cada motor: $nome no DuckDB, :nome no redshift_connector com
# cursor.paramstyle = "named".
_MARKERS = {"duckdb": "$", "redshift": ":"}
_PARAMETER_NAME = re.compile(r"[a-z_][a-z0-9_]*")
# Uma região citada ('...' ou "...", com a aspa dobrada como escape), ou :nome fora de ::cast. As
# regiões vêm primeiro para um : dentro delas nunca ser lido como marcador.
_QUOTED_OR_PLACEHOLDER = re.compile(
    r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"|(?<![:\w]):(?P<name>[a-z_][a-z0-9_]*)")
_SENTINEL_TABLE = re.compile(r"\{prefix\}(\w+)")


def param(name: str, type_: sa.types.TypeEngine | None = None) -> sa.ColumnElement:
    """O parâmetro de execução, que atravessa literal_binds e chega ao texto como :nome."""
    if not _PARAMETER_NAME.fullmatch(name):
        raise SqlError(f"nome de parâmetro inválido: {name!r}")
    return sa.literal_column(f":{name}", type_=type_)


def _prefixed_copy(table: sa.Table, prefix: str) -> sa.Table:
    """A cópia da tabela com o prefixo no nome e só nomes e tipos, o que um DML compilado usa.

    Todo nome vai citado, como no DDL da etapa 1: o texto não depende da lista de palavras
    reservadas do dialeto, e o sentinela fica dentro das aspas.
    """
    columns = []
    for column in table.columns:
        columns.append(sa.Column(quoted_name(column.name, quote=True), column.type))
    name = quoted_name(f"{prefix}{table.name}", quote=True)
    return sa.Table(name, sa.MetaData(), *columns)


def prefixed(statement: sa.sql.ClauseElement, metadata: sa.MetaData,
             prefix: str = SENTINEL) -> sa.sql.ClauseElement:
    """O statement com cada tabela do contrato trocada pela cópia prefixada; o original não muda."""
    copies = {}
    for table in metadata.tables.values():
        copies[table] = _prefixed_copy(table, prefix)

    def replace(element: sa.sql.ClauseElement) -> sa.sql.ClauseElement | None:
        # replacement_traverse chama replace em cada nó do statement; None deixa o nó como está.
        if isinstance(element, sa.Table):
            return copies.get(element)
        if isinstance(element, sa.Column) and element.table in copies:
            return copies[element.table].c[element.name]
        return None

    return replacement_traverse(statement, {}, replace)


def _parameters_without_value(statement: sa.sql.ClauseElement, dialect) -> list[str]:
    """Os bindparam sem valor do statement, que literal_binds renderizaria como NULL."""
    names = []
    for parameter in statement.compile(dialect=dialect).binds.values():
        if parameter.required:
            names.append(parameter.key)
    return names


def render(statement: sa.sql.ClauseElement, dialect: Dialect, metadata: sa.MetaData,
           prefix: str = SENTINEL) -> str:
    """O texto do dialeto com as constantes embutidas e os parâmetros como :nome."""
    copy = prefixed(statement, metadata, prefix)
    missing = _parameters_without_value(copy, _DIALECTS[dialect])
    if missing:
        raise SqlError(f"parâmetro sem valor no statement: {missing}; use param('nome')")
    compiled = copy.compile(dialect=_DIALECTS[dialect], compile_kwargs={"literal_binds": True})
    return str(compiled)


def _placeholders(sql: str) -> set[str]:
    """Os nomes dos marcadores :nome do texto, fora das regiões citadas."""
    names = set()
    for match in _QUOTED_OR_PLACEHOLDER.finditer(sql):
        if match.group("name") is not None:
            names.add(match.group("name"))
    return names


def bind(sql: str, params: dict[str, object], style: Dialect) -> tuple[str, dict[str, object]]:
    """O texto com :nome reescrito para o marcador do motor e o dicionário conferido."""
    if SENTINEL in sql:
        raise SqlError(
            f"o texto ainda traz o sentinela {SENTINEL}; leia-o por read_sql(..., prefix=...)")
    names = _placeholders(sql)
    if names != set(params):
        raise SqlError(
            f"parâmetros do texto {sorted(names)} e do dicionário {sorted(params)} não fecham")
    marker = _MARKERS[style]

    def rewrite(match: re.Match) -> str:
        if match.group("name") is None:
            return match.group(0)                # região citada, intacta
        return marker + match.group("name")

    return _QUOTED_OR_PLACEHOLDER.sub(rewrite, sql), dict(params)


def referenced_tables(statement_or_sql: sa.sql.ClauseElement | str) -> set[str]:
    """As tabelas de um statement Core (find_tables) ou de um texto gerado (o sentinela)."""
    if isinstance(statement_or_sql, str):
        return set(_SENTINEL_TABLE.findall(statement_or_sql))
    tables = set()
    for table in find_tables(statement_or_sql, include_crud=True):
        if isinstance(table, sa.Table):
            tables.add(table.name)
    return tables


def sql_files(statements: dict[str, sa.sql.ClauseElement], metadata: sa.MetaData) -> dict[str, str]:
    """O conteúdo por nome de arquivo, sempre com o sentinela e com \\n final."""
    files = {}
    for name, statement in statements.items():
        files[f"{name}.duckdb.sql"] = render(statement, "duckdb", metadata) + "\n"
        files[f"{name}.redshift.sql"] = render(statement, "redshift", metadata) + "\n"
    return files


def read_sql(directory: str, name: str, dialect: Dialect, prefix: str) -> str:
    """O texto versionado com o sentinela trocado pelo prefixo informado, que quem chama decide."""
    path = os.path.join(directory, f"{name}.{dialect}.sql")
    with open(path, encoding="utf-8") as handle:
        return handle.read().replace(SENTINEL, prefix)


# ---------------------------------------------------------------- a execução

metadata = sa.MetaData()
entries = sa.Table("cad_lancamentos", metadata,
                   sa.Column("id_lancamento", sa.BigInteger), sa.Column("id_conta", sa.BigInteger),
                   sa.Column("valor", sa.Double), sa.Column("area", sa.String(50)),
                   sa.Column("data_base_str", sa.String(10)))
accounts = sa.Table("cad_contas", metadata,
                    sa.Column("id_conta", sa.BigInteger), sa.Column("numero", sa.String(30)))

statement = (
    sa.select(accounts.c.numero, sa.func.sum(entries.c.valor).label("total"))
    .join_from(entries, accounts, entries.c.id_conta == accounts.c.id_conta)
    .where(entries.c.data_base_str == param("data_base_str", sa.String(10)),
           entries.c.area.like("TI:%"), accounts.c.numero != "1:2")
    .group_by(accounts.c.numero)
    .order_by(accounts.c.numero)
)
texts = {"duckdb": render(statement, "duckdb", metadata),
         "redshift": render(statement, "redshift", metadata)}
print(texts["duckdb"])
print("redshift igual ao duckdb:", texts["redshift"] == texts["duckdb"])
print("tabelas do statement e do texto:", sorted(referenced_tables(statement)),
      sorted(referenced_tables(texts["duckdb"])))

# O texto com o prefixo vazio e :nome em $nome roda no DuckDB; 'TI:%' e '1:2' ficam intactos.
sql, values = bind(render(statement, "duckdb", metadata, prefix=""),
                   {"data_base_str": "2026-08-31"}, "duckdb")
print(sql.splitlines()[-1])
con = duckdb.connect()
con.execute("CREATE TABLE cad_lancamentos (id_lancamento BIGINT, id_conta BIGINT, valor DOUBLE, "
            "area VARCHAR, data_base_str VARCHAR)")
con.execute("CREATE TABLE cad_contas (id_conta BIGINT, numero VARCHAR)")
con.execute("INSERT INTO cad_lancamentos VALUES (1, 7, 150, 'TI:infra', '2026-08-31'), "
            "(2, 7, 50, 'RH', '2026-08-31'), (3, 9, 200, 'TI:dados', '2026-07-31')")
con.execute("INSERT INTO cad_contas VALUES (7, '1.1'), (9, '1:2')")
print(con.execute(sql, values).fetchall())

# O alvo de um INSERT ... SELECT também é trocado.
insert = sa.insert(accounts).from_select(["id_conta", "numero"],
                                         sa.select(entries.c.id_conta, entries.c.area).distinct())
print(" ".join(render(insert, "duckdb", metadata).split()))

# As recusas, cada uma com a instrução ao cliente.
try:
    render(sa.select(entries.c.id_conta).where(entries.c.area == sa.bindparam("area")),
           "duckdb", metadata)
except SqlError as error:
    print("bindparam sem valor:", error)
try:
    bind("SELECT 1 WHERE x = :a AND y = :b", {"a": 1}, "duckdb")
except SqlError as error:
    print("parâmetro faltante:", error)
try:
    bind("SELECT 1 WHERE x = :a", {"a": 1, "b": 2}, "redshift")
except SqlError as error:
    print("parâmetro sobrando:", error)
try:
    bind(texts["duckdb"], {"data_base_str": "2026-08-31"}, "duckdb")
except SqlError as error:
    print("sentinela restante:", error)

print(sorted(sql_files({"total_por_conta": statement}, metadata)))
# ::cast e '12:30' preservados; só :k muda.
print(bind("SELECT valor::DECIMAL(18, 2), '12:30' FROM t WHERE k = :k", {"k": 1}, "redshift")[0])

# Um identificador citado passa intacto: o marcador só existe onde vai um valor.
quoted_metadata = sa.MetaData()
quoted_table = sa.Table("t", quoted_metadata, sa.Column("taxa :base", sa.String(10)),
                        sa.Column("preco d'agua", sa.Double), sa.Column("data_str", sa.String(10)))
query = (sa.select(quoted_table.c["taxa :base"], quoted_table.c["preco d'agua"])
         .where(quoted_table.c.data_str == param("data_str")))
quoted_sql, _ = bind(render(query, "duckdb", quoted_metadata, prefix=""),
                     {"data_str": "2026-08-31"}, "duckdb")
print(quoted_sql.splitlines()[0])

# O arquivo versionado guarda o sentinela; quem lê informa o alvo.
with tempfile.TemporaryDirectory() as directory:
    for file_name, text in sql_files({"total_por_conta": statement}, metadata).items():
        with open(os.path.join(directory, file_name), "w", encoding="utf-8") as handle:
            handle.write(text)
    for prefix in ("", "exec_42_"):
        text = read_sql(directory, "total_por_conta", "duckdb", prefix)
        print(f"prefix={prefix!r}:", text.splitlines()[1])
```

Saída:

```
SELECT "{prefix}cad_contas"."numero", sum("{prefix}cad_lancamentos"."valor") AS total
FROM "{prefix}cad_lancamentos" JOIN "{prefix}cad_contas" ON "{prefix}cad_lancamentos"."id_conta" = "{prefix}cad_contas"."id_conta"
WHERE "{prefix}cad_lancamentos"."data_base_str" = :data_base_str AND "{prefix}cad_lancamentos"."area" LIKE 'TI:%' AND "{prefix}cad_contas"."numero" != '1:2' GROUP BY "{prefix}cad_contas"."numero" ORDER BY "{prefix}cad_contas"."numero"
redshift igual ao duckdb: True
tabelas do statement e do texto: ['cad_contas', 'cad_lancamentos'] ['cad_contas', 'cad_lancamentos']
WHERE "cad_lancamentos"."data_base_str" = $data_base_str AND "cad_lancamentos"."area" LIKE 'TI:%' AND "cad_contas"."numero" != '1:2' GROUP BY "cad_contas"."numero" ORDER BY "cad_contas"."numero"
[('1.1', 150.0)]
INSERT INTO "{prefix}cad_contas" ("id_conta", "numero") SELECT DISTINCT "{prefix}cad_lancamentos"."id_conta", "{prefix}cad_lancamentos"."area" FROM "{prefix}cad_lancamentos"
bindparam sem valor: parâmetro sem valor no statement: ['area']; use param('nome')
parâmetro faltante: parâmetros do texto ['a', 'b'] e do dicionário ['a'] não fecham
parâmetro sobrando: parâmetros do texto ['a'] e do dicionário ['a', 'b'] não fecham
sentinela restante: o texto ainda traz o sentinela {prefix}; leia-o por read_sql(..., prefix=...)
['total_por_conta.duckdb.sql', 'total_por_conta.redshift.sql']
SELECT valor::DECIMAL(18, 2), '12:30' FROM t WHERE k = :k
SELECT "t"."taxa :base", "t"."preco d'agua"
prefix='': FROM "cad_lancamentos" JOIN "cad_contas" ON "cad_lancamentos"."id_conta" = "cad_contas"."id_conta"
prefix='exec_42_': FROM "exec_42_cad_lancamentos" JOIN "exec_42_cad_contas" ON "exec_42_cad_lancamentos"."id_conta" = "exec_42_cad_contas"."id_conta"
```

## Decisões pendentes

Nenhuma: as cinco decisões da etapa foram tomadas pelo usuário em 2026-09-21 e 2026-09-22 — as
regiões citadas em `bind`, o `prefix` obrigatório de `read_sql`, os dialetos de terceiros como
compiladores de `render`, a cópia prefixada com `quote=True` e o `sqlglot` no grupo `dev` —, e cada
uma está escrita na seção que a descreve.
