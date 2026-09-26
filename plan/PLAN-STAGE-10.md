# Etapa 10: acesso de leitura e canal do snapshot

A entrega e o critério de aceite desta etapa estão na tabela de etapas de [`PLAN.md`](PLAN.md), que
também fixa as decisões, as regras que toda etapa obedece e a ordem do trabalho.

A etapa dá ao código cliente que tem o modelo a leitura da base por statements Core, com o
resultado em Arrow, em duas origens: a base Delta, por um DuckDB no processo com uma view por
tabela, e a base publicada no Redshift, pelas tabelas `<ambiente>_<tabela>` da
[etapa 8](PLAN-STAGE-8.md). O módulo é `serialize_db.reader`. O time que tem o `Database` entra por
`db.open_delta()` e `db.open_redshift()`; o cliente que só enxerga o Redshift entra por
`serialize_db.reader.open_redshift`, sem `Database`. O mesmo statement roda nas duas origens.

A etapa também dá nome ao snapshot padrão: o canal `default` do ambiente, que um comando próprio
move. A publicação no Redshift passa a escolher o snapshot pelo nome ou pelo canal.

## As decisões do usuário

Tomadas em 2026-09-24, na conversa que propôs a etapa:

- O leitor é um objeto próprio, aberto a partir do `Database`, com a implementação em
  `serialize_db.reader` e os nomes `open_delta` e `open_redshift`. Ele não é `Execution`, que fixa
  partição e `execution_id`, grava snapshot e publica. Ele também não é uma `Connection` do
  SQLAlchemy, que devolve linhas e não troca `cad_lancamentos` por `prd_cad_lancamentos`.
- O resultado é Arrow, como nos motores: `query` devolve a `pa.Table`, e
  `to_pandas(types_mapper=pd.ArrowDtype)` a leva ao pandas, pela regra da troca de dados de
  2026-09-20 ([`PLAN.md`](PLAN.md), seção "A troca de dados com o código cliente").
- As duas origens servem a times diferentes. Os clientes com acesso só de leitura à base publicada
  no Redshift informam um destino próprio para o `UNLOAD`.
- A versão padrão do leitor Delta é a do snapshot padrão do ambiente, e outro modo lê a versão
  atual de cada tabela.
- A materialização de parte das partições é permitida: o nome do modelo passa a ter só essas
  partições.

Tomadas no mesmo dia, nas respostas ao modelo dos snapshots:

- O nome do snapshot continua livre dentro da regra da partição, imutável e único.
- O snapshot padrão é o que o canal `default` do ambiente aponta. Só um comando próprio move o
  canal, sem vínculo com a publicação, e a etapa começa só com o `default`.
- O Redshift não tem canal. `serialize-db publish` recebe `--snapshot <nome>`, que escolhe o
  snapshot explicitamente, ou `--channel <nome>` (`--channel default`), que publica o snapshot
  do canal.
- O exercício que diverge nos dados é um ambiente (dsv, prd), e nenhuma operação que crie um
  ambiente a partir de um snapshot entra agora.
- A versão atual de cada tabela, sem snapshot, é o canal reservado `current`, no leitor
  (`open_delta(channel="current")`) e na publicação (`--channel current`); o comando do canal
  não o move.
- `run.publish_redshift` e `serialize-db run --redshift` saem: a publicação no Redshift é sempre
  `serialize-db publish`, depois da execução.
- O leitor Delta lê o snapshot arquivado pela cópia em `arquivo/<nome>/<tabela>`.
- O comando que move o canal é `serialize-db channel --name default --snapshot <nome>`, com o
  nome do canal explícito desde já.
- O leitor Redshift tem duas entradas: `serialize_db.reader.open_redshift` para o cliente sem a
  raiz Delta e `db.open_redshift` para o time.

Tomadas no mesmo dia, na revisão da interface pela simplicidade de uso:

- `config` e `unload_to` do leitor Redshift aceitam `None`. Com `config=None`, o leitor lê
  `RedshiftConfig.from_environment()`. Sem `unload_to`, o cliente sem S3 roda `query`, e `stream`
  é `ContractError`.
- O leitor Delta que não foi fechado apaga a pasta temporária do motor quando é coletado ou quando
  o interpretador termina normalmente.
- Ficam como estão o `metadata` de `open_redshift`, o par `snapshot` e `channel` de `open_delta` e
  a materialização parcial por `materialize(..., partitions=...)`.

## A implementação

O módulo `serialize_db.reader` (`DeltaReader`, `RedshiftReader` e `open_redshift`),
`Database.open_delta` e `Database.open_redshift` em `serialize_db.execution`, `set_channel`,
`channel_snapshot`, `snapshot_versions` e a protegida `channels_pointing` em `serialize_db.delta`,
o subcomando `serialize-db channel`, o `serialize-db publish` por `--snapshot` ou `--channel`, e
os casos de `tests/test_reader.py`, `tests/test_delta.py`, `tests/test_operation.py`,
`tests/test_execution.py` e `tests/test_publication.py` substituem a interface planejada em
2026-09-24: as assinaturas, o uso e as docstrings estão no código, em `docs/index.md` (seção
"Ler a base com o modelo") e na documentação do `pdoc`. `Execution.publish_redshift` e
`serialize-db run --redshift` saíram em 2026-09-25. O que a implementação mostrou está em
[`POC.md`](POC.md), seção "O que a implementação da etapa 10 mostrou"; as leituras que só o
ambiente alvo dá ficam na seção "As execuções no ambiente alvo".

O que a implementação fixou além do texto das seções abaixo:

- **O motor sem armazenamento** guarda a recusa do `stream`: `RedshiftEngine.stream` levanta
  `ContractError`, que cita `unload_to`, quando `storage` é `None`, antes de qualquer comando, e o
  `cleanup` não lista arquivos; o leitor só repassa. `RedshiftEngine` ganhou `prefix`, e
  `storage` e `staging_prefix` aceitam `None` juntos.
- **`materialize` confere toda tabela antes da primeira troca**: a view ausente, `partitions`
  numa tabela sem partição e o valor fora da regra são `ContractError` sem que tabela alguma
  troque; as trocas correm em paralelo, uma sessão a mais por tabela, e `materialized` guarda a
  lista de partições como o cliente a passou.
- **A regra de leitura no leitor Delta** confere só as tabelas do statement Core que estão em
  `db.metadata.tables` (`sql.referenced_tables`); um texto pronto e uma tabela temporária de
  `session()` passam ao motor como estão.
- **As versões do snapshot arquivado** são a versão atual de cada cópia em `arquivo/<nome>/`,
  lida na abertura; `reader.versions` as traz, e a view cita a cópia.
- **O finalizador** guarda o `cleanup` do motor, e `close()` o chama: o `cleanup` roda uma vez,
  no `close` ou na coleta, e a segunda chamada não faz nada; o motor de `DuckDBConfig()` nasce em
  `tempfile.mkdtemp(prefix="serialize_db_")`, a pasta que a coleta apaga.
- **`Database.open_redshift` sem `unload_to`** grava em `<raiz>/<ambiente>/staging/<id do
  leitor>/`, e `open_redshift` sem ele abre o motor sem armazenamento; o identificador é
  `reader-<AAAA-MM-DD>-<uuid8>` nos dois leitores.
- **`unload_to` aceita uma pasta local** (decisão do usuário de 2026-09-25), que serve só à
  conexão de mentira de `tests/test_engine_redshift.py`, a que grava o arquivo pelo
  armazenamento do leitor. O `UNLOAD` do Redshift grava só no S3, e o do substituto das suítes
  também, pelo moto: nele, uma pasta local falha no `ParamValidationError` do boto3 antes de
  gravar ([`POC.md`](POC.md)), e no alvo o caso não rodou. A docstring de `unload_to` diz o
  que cada conexão aceita.
- **A volta a um snapshot anterior** chama `version_diff(uri, min, max)` entre a versão publicada
  e a pedida, e a transação da publicação deixou de recusar a versão lida acima da pedida; o
  `UPDATE` da linha de controle continua condicionado à versão lida.
- **`serialize-db publish`** sai com 2, sem escrita no Redshift, na chamada sem `--snapshot` nem
  `--channel`, com `--init`, `--status` ou `--unpublish` ao lado de um deles, no canal ausente,
  no snapshot ausente ou arquivado e na tabela pedida sem versão (`PublicationError`, que
  `--tables` contorna); `serialize-db archive` recusa o snapshot de um canal antes de copiar.
- **`tests/test_reader.py`** usa a fixture `target` e `export_with_duckdb` de
  `tests/test_publication.py` no caso `redshift`, e a conexão de mentira de
  `tests/test_engine_redshift.py` nos casos sem conexão; a poda pela view é medida pelo log
  `FileSystem` do DuckDB, por caso parametrizado (`=`, `BETWEEN`, `IN` salteado).

## Estratégia de implementação

### O canal do snapshot

- **O arquivo de controle** ganha a chave irmã `channels`, ao lado de `snapshots` e `archived`:
  `{"channels": {"default": "2026T3"}}`. O mapa por nome guarda o `default` hoje e aceita outro
  canal depois sem mudar o formato.
- **O canal `current`** é reservado e não fica no arquivo: ele resolve para a versão atual de cada
  tabela do modelo que existe no ambiente, lida como `serialize-db snapshot` a lê.
- **`set_channel(storage, environment, name, snapshot)`** aponta o canal para o snapshot na
  escrita condicional do arquivo de controle e devolve o controle novo. O nome fora da regra da
  partição e o nome `current` são `ContractError`; o snapshot ausente de `snapshots` é
  `ValueError`, também o que está em `archived`, porque o `vacuum` deixa de preservar as versões
  dele; outro escritor entre a leitura e a escrita é `ConflictError`.
- **`channel_snapshot(control, name)`** devolve o snapshot do canal, e o canal ausente é
  `ContractError` com o comando que o cria; o canal `current` também, porque não aponta snapshot, e
  o leitor e `serialize-db publish` o tratam antes de chamá-la.
  **`snapshot_versions(control, name)`** devolve as versões do snapshot, e o nome ausente de
  `snapshots` é `ContractError`. O leitor e a publicação usam as duas.
- **`archive_snapshot`** recusa com `ValueError` o snapshot que um canal aponta: o canal precisa
  ser movido antes.
- **`serialize-db channel --name <canal> --snapshot <nome>`** aponta o canal e imprime o snapshot
  anterior e o novo; sem opções, imprime os canais. O comando sai com 0 ao terminar e 2 no nome
  fora da regra, no `current`, no snapshot ausente ou arquivado e no conflito de escrita, como os da
  [etapa 9](PLAN-STAGE-9.md). Nenhum outro caminho move o canal: nem `Execution.snapshot`, nem
  `serialize-db snapshot`, nem a publicação.

### A publicação por snapshot

- **`serialize-db publish`** publica só com `--snapshot <nome>` ou `--channel <nome>`, que o
  `argparse` torna excludentes, e um dos dois é obrigatório; `--init`, `--status` e `--unpublish`
  não os recebem. As versões são as de `snapshot_versions`, que `publication.publish_redshift`
  já recebe em `versions`; `--channel current` passa as versões atuais.
- **O snapshot arquivado** é recusado, e a tabela do modelo ausente do snapshot é
  `PublicationError` com o nome do snapshot.
- **A volta a um snapshot anterior ao publicado** troca as partições alteradas entre as duas
  versões. Hoje `version_diff` recusa a versão publicada posterior à pedida (`ValueError`); a
  publicação passa a chamá-lo com a menor e a maior das duas. A partição que só a versão
  publicada tem sai pelo `DELETE`, sem `COPY`, como já acontece com a partição removida.
- **A publicação dentro da execução sai**: `Execution.publish_redshift` e `serialize-db run
  --redshift` deixam o pacote, e o argumento `redshift` de `Execution` fica só para o motor
  `"redshift"`. A execução grava no Delta e marca o snapshot; a publicação vem depois, pela
  linha de comando. As [etapas 6](PLAN-STAGE-6.md) e [8](PLAN-STAGE-8.md), `docs/`, os testes e
  `SUITE.md` mudam com a implementação desta etapa.

### O leitor Delta

- **As versões** são lidas na abertura, em três modos:
  - Sem argumento, o leitor usa o snapshot do canal `default` do arquivo de controle
    `<ambiente>/_serialize_db/snapshots.json`, por `channel_snapshot` e `snapshot_versions`. O
    ambiente sem o canal é `ContractError`, e a mensagem aponta `serialize-db channel` e o canal
    `current`.
  - Com `snapshot="2026T3"`, o leitor usa a entrada com esse nome, e o nome ausente é
    `ContractError`. O nome em `archived` abre as views sobre a cópia em
    `arquivo/<nome>/<tabela>`, na versão atual de cada cópia, que não muda depois do `archive`
    (decisão do usuário): o `vacuum` deixa de preservar as versões do snapshot arquivado na
    tabela viva.
  - Com `channel="default"`, o leitor usa o snapshot do canal, como sem argumento; com
    `channel="current"`, a versão atual de cada tabela do modelo que existe no ambiente, como
    `Execution` na abertura. `snapshot` e `channel` juntos são `ContractError`.
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
- **O leitor não fechado** tem o mesmo `cleanup` num `weakref.finalize`, que roda quando o objeto é
  coletado ou quando o interpretador termina normalmente; sem ele, a pasta `serialize_db_*` do
  motor, com o `.duckdb` e as tabelas materializadas, ficaria no disco depois do processo. O
  finalizador guarda o motor, não o leitor, para não impedir a coleta, e o `close` o chama, então o
  `cleanup` roda uma vez só. O processo encerrado por sinal não o roda.

### O leitor Redshift

- **`open_redshift(metadata, environment, config=None, unload_to=None)`** abre um
  `RedshiftEngine` sobre a conexão de `config`, com o prefixo `<ambiente>_` no lugar de
  `exec_<id>_`, o `Storage` de `unload_to` e a pasta `<id do leitor>` sob ele. O motor ganha o
  argumento `prefix`, que tem `sandbox_prefix(execution_id)` por padrão.
- **`config=None`** lê `RedshiftConfig.from_environment()`, as variáveis `SERIALIZE_DB_REDSHIFT_*`,
  como o motor `"redshift"` de `Execution` e a linha de comando.
- **Sem `unload_to`**, o cliente sem S3 roda `query`, e `stream` é `ContractError`, que cita
  `unload_to`, antes de qualquer comando no servidor. O motor abre sem armazenamento: `storage` e
  `staging_prefix` aceitam `None`, que só o leitor passa, e o `cleanup` não toca arquivos.
- **`Database.open_redshift(config=None, unload_to=None)`** chama a função com `db.metadata`,
  `db.environment`, o mesmo `config` e, sem `unload_to`, `<raiz>/<ambiente>/staging`.
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
- **`serialize-db channel`** precisa de leitura e gravação do arquivo de controle, e grava só ele.
- **A publicação por snapshot** lê as versões do snapshot, que o `vacuum` preserva enquanto a
  entrada está em `snapshots`.
- **O leitor Redshift** precisa de `SELECT` nas tabelas publicadas do esquema. Para `stream`,
  precisa também do `UNLOAD` com o papel de `config.iam_role` ou as credenciais da sessão `boto3`,
  e de gravação, leitura e exclusão em `unload_to`; sem `unload_to`, o leitor não toca o S3. Nada
  é criado no esquema.

## Testes por caso

`tests/test_reader.py`, sob a raiz local (`local`):

- O snapshot do canal `default`, depois de o canal passar de um snapshot a outro; o ambiente sem o
  canal, `ContractError`.
- O snapshot nomeado; o nome ausente, `ContractError`; o nome arquivado, lido da cópia em
  `arquivo/<nome>/`, com as mesmas linhas do snapshot antes do `archive`.
- O canal `current` lê a versão atual; `snapshot` com `channel`, `ContractError`.
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
- O leitor sem `close`: `del` e `gc.collect()` apagam a pasta temporária do motor; depois do
  `close`, o finalizador não roda de novo.

`tests/test_delta.py`, nas duas raízes: `set_channel` aponta e move o canal e recusa o nome fora
da regra, o nome `current`, o snapshot ausente e o arquivado; a escrita concorrente é
`ConflictError`; `channel_snapshot` e `snapshot_versions` recusam o que não existe, e
`channel_snapshot` o canal `current`, que não fica no arquivo; e `archive_snapshot` recusa o
snapshot de um canal.

`tests/test_operation.py`, sob a raiz local: `serialize-db channel --name default --snapshot`
move o canal, imprime o anterior e o novo e mostra os canais; o snapshot ausente e o nome
`current` saem com 2.

`tests/test_publication.py`, com `redshift`, `s3` e `local`: a publicação por `--channel default`,
por `--snapshot` e por `--channel current`; a volta a um snapshot anterior troca só as partições
alteradas entre as duas versões, com a partição que só a versão publicada tinha apagada; o
snapshot arquivado é recusado sem escrita no Redshift; e a publicação sem nenhum dos dois, ou com
os dois, é erro de uso.

`tests/test_reader.py`, sem conexão: o texto compilado com o prefixo `<ambiente>_` e a regra de
leitura comum no leitor Redshift, sobre a conexão de mentira de `tests/test_engine_redshift.py`;
`config=None` com as variáveis `SERIALIZE_DB_REDSHIFT_*` do `monkeypatch`; e, sem `unload_to`,
`query` roda e `stream` é `ContractError` sem comando no servidor.

`tests/test_reader.py`, com `redshift`, `s3` e `local`, no substituto local e no ambiente alvo: a
publicação de `Lancamento` num ambiente `poc<id>`, como `tests/test_publication.py`, e o leitor de
`open_redshift` com `unload_to` no bucket da suíte. `query` devolve o mesmo resultado do leitor
Delta para o mesmo statement, e `stream` o mesmo de `query`. Depois do `close`, nada fica sob
`<unload_to>/<id do leitor>/`.

## As execuções no ambiente alvo

Os comandos estão em `SUITE.md`, seções "Publicação Delta -> Redshift" e "Acesso de leitura", e
as leituras em [`POC.md`](POC.md):

- **O tempo de abertura do leitor Delta** sobre as 12 tabelas da raiz carregada, com as views em
  paralelo: 0,645 s em 2026-09-25 e 0,582 s em 2026-09-26, com `cad_lancamentos` na versão 4 e na
  5, contra 8,7 ms por view na pasta local (sonda de 2026-09-24). A contagem de `cad_contas` deu o
  mesmo pelos dois leitores nos dois dias.
- **A publicação por `--channel default`** da raiz de `SUITE.md` rodou em 2026-09-26, com
  `--max-workers 4`: as tabelas sem partição de 3,5 s a 4,8 s, `cad_contratos` em 31,2 s,
  `cad_operacoes` em 53,1 s, `rel_contrato_operacao` em 56,5 s e `cad_lancamentos`, com cinco
  partições, em 295,1 s, com o pico do processo em 266 MB. A volta a um snapshot anterior segue
  sem leitura sobre a raiz: o snapshot de `SUITE.md` está na versão atual, e a volta não tem
  partição para trocar. No substituto, em 2026-09-25, e na suíte da publicação no alvo, num
  ambiente `poc<id>`, a volta trocou só as partições alteradas entre as duas versões.
- **O `UNLOAD` do cliente** para um bucket próprio com um usuário do Redshift só de leitura,
  pendente. A execução mostra se o `UNLOAD` é aceito para quem só tem `SELECT` e qual caminho de
  credencial serve, o `iam_role` do cliente ou as credenciais da sessão. Ela precisa de um papel de
  cliente no ambiente alvo.

## Decisões pendentes

Nenhuma. As decisões do usuário de 2026-09-24 sobre o canal, a versão atual, a publicação fora
da execução, o snapshot arquivado, o comando do canal, as entradas do leitor Redshift e a revisão
da interface estão escritas nas seções que as descrevem.
