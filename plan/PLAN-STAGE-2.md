# Etapa 2: `sql`

A entrega e o critério de aceite desta etapa estão na tabela de etapas de [`PLAN.md`](PLAN.md), que
também fixa as decisões, as regras que toda etapa obedece e a ordem do trabalho.

O módulo `serialize_db.sql` gera o texto SQL de cada dialeto a partir de um statement Core, a
opção de migração para fora do SQLAlchemy descrita em `sqlalchemy.md`, e a cópia prefixada que os
motores compilam. O caminho padrão é o statement Core submetido ao motor, que o compila pelo
dialeto com os parâmetros do cliente (decisão do usuário de 2026-09-22); o texto versionado é
opcional.

| Primitiva | O que faz |
| --- | --- |
| `prefixed(statement, metadata, prefix="{prefix}")` | A cópia do statement com cada tabela do contrato trocada pela cópia com o prefixo, por `replacement_traverse`; todo nome da cópia vai citado (`quoted_name(quote=True)`), o sentinela dentro das aspas como no DDL. |
| `render(statement, dialect, metadata, prefix="{prefix}")` | O texto de `duckdb` ou `redshift` com as constantes embutidas e os parâmetros como `:nome`, compilado por um dialeto com `paramstyle="named"`, que não dobra o `%` dos literais; cada `bindparam` sem valor sai como `:nome` por `replacement_traverse`, o com valor como constante, e um nome fora de `[a-z_][a-z0-9_]*` é erro (decisão do usuário de 2026-09-22); nenhuma linha termina em espaço, para o arquivo versionado sobreviver a um editor que apara o fim das linhas. |
| `bind(sql, params, dialect)` | O texto com `:nome` reescrito para o estilo do motor (`$nome` no DuckDB; inalterado no `redshift_connector` com `paramstyle = "named"`) e o dicionário conferido: parâmetro faltante ou sobrando é erro, e um texto que ainda traz `{prefix}` também. Toda região citada passa intacta, entre aspas simples ou duplas. |
| `referenced_tables(statement_or_sql)` | As tabelas do contrato que um statement Core (`find_tables`) ou um texto gerado (o sentinela) cita, para o log da execução. |
| `sql_files(statements, metadata)` | `{"<nome>.duckdb.sql": ..., "<nome>.redshift.sql": ...}` de um dicionário `{nome: statement}`. |
| `write_sql_files(statements, metadata, directory)` | Grava `sql_files`; `serialize-db sql write` e `serialize-db sql check`. |
| `read_sql(directory, name, dialect, prefix)` | O texto versionado com o sentinela `{prefix}` trocado pelo `prefix` informado, para a `query` e o `stream` dos motores. |

Testes: `tests/test_sql.py`, sem gravar fora do caso `local`: o statement de `sqlalchemy.md` (parâmetro, `%` em literal,
prefixo) renderizado nos dois dialetos e executado no DuckDB em memória com `$mes`; `bindparam` sem
valor como `:nome`, o statement num `Connection` criado fora da biblioteca, e parâmetro faltante
como erro; o diff de `tests/client_model/sql/`, gerado dos statements do
pipeline fictício em `tests/client_model/statements.py` (`STATEMENTS`, o dicionário `{nome: statement}`
que `serialize-db sql` recebe por `--statements`). `sqlglot.parse_one(texto, dialect="redshift")` sobre o texto do Redshift de cada statement, o
arquivo versionado com o sentinela inclusive, como teste de que o texto gerado para o Redshift
analisa (decisão do usuário de 2026-09-22; `sqlglot==30.18.0` no grupo `dev`, a versão do ensaio
de 2026-09-21; o sentinela dentro das aspas de um identificador analisa, leitura de 2026-09-22 em
[`POC.md`](POC.md)). Provas de conceito:
`test_sqlalchemy.py` (os comportamentos do compilador que dão forma a `render`:
`test_literal_binds_renders_a_bindparam_without_value_as_null`,
`test_compiled_binds_marks_the_bindparam_without_value_as_required`,
`test_default_paramstyle_doubles_the_percent_in_literals` e
`test_dialects_quote_only_their_reserved_words`; e `test_redshift_dialect_compiles_dml`) e
`test_stdlib.py::test_generated_files_diff`.

## Estratégia de implementação

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
- **`render`** troca na cópia prefixada cada `bindparam` sem valor (`required=True`) por
  `literal_column(":nome", type_)`, por `replacement_traverse` (`_parameters_as_placeholders`), e
  compila o resultado com `literal_binds=True` num dialeto avulso de `paramstyle="named"`, que não
  dobra o `%` dos literais: o `literal_column` atravessa a compilação como texto, o `bindparam` com
  valor sai como constante, e um nome fora de `[a-z_][a-z0-9_]*` é `SqlError`, porque `bind` não o
  leria. É a decisão do usuário de 2026-09-22: o statement escrito com `bindparam` serve ao
  `Connection` do cliente, à `query` e ao `stream` dos motores e aos arquivos, e `param`
  (`literal_column(":nome")`, que chegava ao driver como texto e falhava no `Connection` com
  `Parser Error: syntax error at or near ":"`) saiu do módulo ([`POC.md`](POC.md)). O texto sai sem
  o espaço que o compilador deixa antes de cada quebra de linha, para o arquivo versionado
  sobreviver a um editor que apara o fim das linhas (2026-09-22, [`POC.md`](POC.md)).
  O rascunho anterior transformava o `SAWarning` dessa renderização em exceção com
  `warnings.catch_warnings`, que troca o filtro de avisos do processo inteiro: a documentação do
  módulo `warnings` o declara inseguro num programa com threads abaixo do Python 3.14, e o projeto
  roda 3.13 com os motores das etapas [4](PLAN-STAGE-4.md) e [5](PLAN-STAGE-5.md) chamando `render`
  em threads ([`POC.md`](POC.md)). O aviso também não serve de guarda: ele sai só numa comparação
  por `=`, e no `LIKE`, no `coalesce`, no `VALUES` de um `INSERT` e no `text()` o `bindparam` sem
  valor vira `NULL` calado, enquanto `required` o marca nas sete formas medidas (leitura de
  2026-09-22, [`POC.md`](POC.md)). Os dialetos são `duckdb_engine.Dialect` e
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
- **`referenced_tables`** lê `find_tables(include_crud=True)` num statement Core e o sentinela
  seguido do nome num texto gerado; só nomes de `sa.Table`.
- **`sql_files`** renderiza cada statement nos dois dialetos, um laço com uma linha por arquivo,
  sempre com o sentinela `{prefix}` e com `\n` final, porque o arquivo versionado serve a qualquer
  alvo; `write_sql_files` e `check_sql_files` repetem o padrão da [etapa 1](PLAN-STAGE-1.md).
- **`read_sql`** abre `<pasta>/<nome>.<dialeto>.sql` com `encoding="utf-8"`, como a etapa 1, e troca
  `{prefix}` pelo `prefix` informado, argumento obrigatório (decisão do usuário de 2026-09-21): quem
  chama sabe o alvo — a string vazia para as tabelas do contrato, `exec_<id>_` para o sandbox da
  execução —, e a obrigação de informar sobe para o pipeline que lê o arquivo e entrega o texto à
  `query` e ao `stream` dos motores das etapas [4](PLAN-STAGE-4.md) e [5](PLAN-STAGE-5.md). Nenhuma outra
  primitiva preenche o sentinela.
- **`serialize-db sql write|check`** recebe `--metadata modulo:atributo`, `--statements
  modulo:atributo` (o dicionário `{nome: statement}` do pipeline, resolvido pelo mesmo caminho de
  `--metadata`) e a pasta; `check` imprime o diff e sai com 1 quando há diferença, como
  `schema check`.

## Pré-requisitos e pós-condições

| Primitiva | Pré-requisitos | Pós-condições |
| --- | --- | --- |
| `prefixed` | Toda tabela do statement pertence ao `metadata` informado. | Uma cópia com cada tabela e coluna trocadas; o original intacto. |
| `render` | Statement Core sobre tabelas do `metadata`; o nome de cada `bindparam` sem valor válido como identificador. | Texto com as constantes embutidas, cada `bindparam` sem valor como `:nome`, `%` dos literais simples e toda tabela e coluna do contrato entre aspas, o `{prefix}` dentro delas; o mesmo texto nos dois dialetos para o SQL portável; nenhuma linha termina em espaço. |
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
| `bindparam` sem valor | `test_render_writes_a_bindparam_without_value_as_placeholder` | `:nome` no texto, num `text()` inclusive, com o statement original intacto; o `bindparam` com valor como constante; o nome fora de `[a-z_][a-z0-9_]*` como `SqlError`. |
| `Connection` do cliente | `test_statement_with_bindparam_runs_on_a_client_connection` | O statement com `bindparam` roda num `sqlalchemy.Connection` do `duckdb-engine` criado fora da biblioteca, com o dicionário de parâmetros. |
| Prefixo | `test_prefixed_replaces_every_contract_table` | Tabelas e colunas trocadas em `select`, `insert ... from_select` e `join`; o statement original intacto. |
| Tabelas referenciadas | `test_referenced_tables_from_core_and_text` | `find_tables` e o sentinela dão o mesmo conjunto para o mesmo comando. |
| Arquivos gerados | `test_sql_files_match_versioned`, `test_check_sql_files_reports_a_changed_statement` | Diff vazio contra `tests/client_model/sql/`; uma coluna nova no statement aparece no diff. |
| Prefixo do arquivo | `test_read_sql_fills_the_sentinel` | `read_sql(..., prefix="")` dá `cad_lancamentos` e `prefix="exec_42_"` dá `exec_42_cad_lancamentos`; um `{prefix}` que sobra no texto é `SqlError` no `bind`. |
| Gravação | `test_write_sql_files` (`local`) | Os oito arquivos gravados sob a raiz local, o `check` vazio depois e `read_sql` sobre eles. |
| Linha de comando | `test_cli_sql_check_reads_the_versioned_files` | `serialize-db sql check --metadata client_model:Base.metadata --statements client_model.statements:STATEMENTS tests/client_model/sql` sai com 0 sem gravar; um statement mudado sai com 1 e imprime o diff; sem `--statements` sai com 2. |
| Redshift analisável | `test_redshift_text_parses_with_sqlglot` | `sqlglot.parse_one(texto, dialect="redshift")` aceita o texto do Redshift de cada statement de `STATEMENTS`, o arquivo versionado com o sentinela e o texto com o prefixo vazio (o sentinela fica dentro das aspas de um identificador, leitura de 2026-09-22), e recusa uma aspa desbalanceada com `TokenError`; o teste não diz o que o Redshift suporta nem vê um identificador estragado como `"taxa $base"` (ensaio de 2026-09-21, [`POC.md`](POC.md)). |

## A implementação

O módulo `serialize_db.sql` e os casos de `tests/test_sql.py` substituem a interface e o rascunho
executado em 2026-09-21: as assinaturas e as docstrings estão no código e na documentação do
`pdoc`, e o que o rascunho, a revisão e a implementação mostraram está em [`POC.md`](POC.md),
seções "O que os rascunhos das etapas mostraram", "O que a revisão da etapa 2 mostrou" e "O que a
implementação da etapa 2 mostrou".

## Decisões pendentes

As decisões da etapa foram tomadas pelo usuário em 2026-09-21 e 2026-09-22 — as regiões citadas
em `bind`, o `prefix` obrigatório de `read_sql`, os dialetos de terceiros como compiladores de
`render`, a cópia prefixada com `quote=True`, o `sqlglot` no grupo `dev`, e cada `bindparam` sem
valor como `:nome` em `render`, com `param` fora do módulo —, e cada uma está escrita na seção que
a descreve. Nenhuma decisão da etapa espera o usuário.
