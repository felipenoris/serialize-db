"""[public] As exceções da biblioteca.

O módulo não depende de nenhum outro, e por isso os outros módulos o importam sem ciclo. Cada
exceção pertence a uma parte da biblioteca:

- ``ContractError``: os dados, o modelo ou a configuração fora do contrato de ``schema``.
- ``SqlError``: o texto SQL de ``sql``.
- ``ConflictError``, ``RegistrationRefused``, ``SchemaDiffRefused`` e ``LogUnavailable``: a
  camada de tabela, ``storage`` e ``delta``. A execução as deixa chegar ao cliente.
- ``ExecutionConflict``: o commit concorrente, em ``delta``, em ``execution`` e em
  ``publication``.
- ``SandboxError``: o sandbox, nos motores de ``engine`` e nos limites de ``resources``.
- ``AuditFailed``: a auditoria reprovada, em ``execution``.
- ``PublicationError``: a publicação no Redshift, em ``publication``.

A linha de comando traduz as exceções em códigos de saída. Ela sai com 1 em ``AuditFailed`` e no
``ContractError`` da carga de ``serialize-db import``. Sai com 2 em ``ContractError``,
``ConflictError``, ``ExecutionConflict`` e ``PublicationError`` nos demais casos. As outras
exceções sobem com o traceback. A lista dos casos de cada código está em ``serialize_db.cli``.
"""

__all__ = [
    "AuditFailed",
    "ConflictError",
    "ContractError",
    "ExecutionConflict",
    "LogUnavailable",
    "PublicationError",
    "RegistrationRefused",
    "SandboxError",
    "SchemaDiffRefused",
    "SqlError",
]


class ContractError(ValueError):
    """[public] Dados, modelo ou configuração fora do contrato.

    O contrato é o modelo SQLAlchemy do cliente, de que ``schema`` deriva o esquema e as
    conferências. Nos dados e no modelo, a mensagem nomeia a tabela e a coluna e diz o que o
    cliente faz antes de chamar de novo: arredondar, truncar, serializar ou declarar
    ``String(n)``. Na configuração, a mensagem nomeia o que falta ou sobra: o ``RedshiftConfig``
    sem conexão, ou o ``execution_id`` longo demais para o prefixo do sandbox do Redshift.

    Exemplo:

    .. code-block:: python

        try:
            schema.cast(batch, Operacao.__table__)
        except ContractError as error:
            print(error)   # "cad_operacoes.data: timestamp com hora numa coluna Date; ..."
    """


class SqlError(ValueError):
    """[public] Um statement que não vira texto executável, ou um texto cujos parâmetros não fecham
    com o dicionário de valores.

    A mensagem nomeia a causa, e cada causa tem a sua correção pelo cliente:

    - um parâmetro de nome inválido: o cliente renomeia o ``bindparam``;
    - os parâmetros em falta ou sobrando: o cliente completa o dicionário;
    - o sentinela que ficou no texto, o marcador ``{prefix}`` do prefixo do sandbox no nome de
      cada tabela: o cliente lê o texto por ``read_sql``. Só a mensagem do sentinela indica a
      correção.

    Exemplo:

    .. code-block:: python

        sql.bind("SELECT :valor", {}, "duckdb")
        # SqlError: parâmetros do texto ['valor'] e do dicionário [] não fecham
    """


class ConflictError(Exception):
    """[public] A escrita condicional foi recusada: outro escritor criou ou mudou o arquivo desde a
    leitura.

    Uma escrita condicional só grava quando o arquivo ainda não existe (``Storage.create_text``)
    ou quando ele continua igual ao lido (``Storage.write_text`` com ``if_match``). A recusa vem
    dos dois armazenamentos:

    - no S3, do 412 do ``IfNoneMatch`` ou do ``IfMatch``, ou do 409 de outra operação no objeto
      durante a gravação;
    - na pasta local, do arquivo já existente ou da impressão digital diferente.

    Nada foi gravado. Quem chama lê o arquivo de novo e decide o que fazer.

    Exemplo:

    .. code-block:: python

        storage.create_text("prd/_serialize_db/snapshots.json", "{}")
        storage.create_text("prd/_serialize_db/snapshots.json", "{}")   # ConflictError
    """


class ExecutionConflict(Exception):
    """[public] Outra execução gravou a mesma partição, ou avançou a tabela com dados desde a
    abertura.

    A exceção cobre os conflitos entre escritores concorrentes:

    - o ``CommitFailedError`` do delta-rs num ``overwrite`` ou num registro de arquivos;
    - a versão fixada que ficou para trás, isto é, a versão que a execução leu na abertura já não
      é a atual da tabela;
    - na publicação no Redshift, a linha de controle que mudou desde a leitura, o erro ``1023`` e
      a tabela publicada que outra primeira publicação criou.

    Nenhum commit foi feito pela chamada que falhou.

    Exemplo:

    .. code-block:: python

        try:
            run.publish_delta(Projetado.__table__, partitions=["2026-08-31"])
        except ExecutionConflict:
            raise   # nada desta chamada foi gravado; uma execução nova parte da versão atual
    """


class RegistrationRefused(Exception):
    """[public] Uma conferência de ``register_files`` reprovou antes do commit, ou a releitura
    reprovou depois dele e ``restore`` voltou a versão anterior.

    ``register_files`` registra no log da tabela Delta os arquivos que outro escritor gravou na
    pasta dela, depois de conferir cada um. A mensagem nomeia a conferência reprovada e, numa
    conferência de arquivo, o arquivo. Os arquivos recusados ficam órfãos na pasta da tabela até
    um ``vacuum_keeping_snapshots(..., apply=True, full=True)`` depois da retenção
    (``serialize-db vacuum --full --apply``).

    ``deep_copy`` também a levanta:

    - no destino que registra um arquivo fora da versão copiada;
    - no esquema da cópia diferente do da versão copiada, quando nenhuma partição copiada o
      trocou;
    - na contagem da cópia diferente da soma das ações.

    Exemplo:

    .. code-block:: python

        delta.register_files(uri, Operacao.__table__, [file], "2026-08-31", metadata, storage,
                             expected_rows=999)
        # RegistrationRefused: 1000 linhas nos arquivos, 999 na fonte
    """


class SchemaDiffRefused(Exception):
    """[public] O diff entre o modelo e a tabela Delta é destrutivo, e só ``rewrite`` o resolve.

    Um diff destrutivo tem uma destas diferenças:

    - uma coluna removida; uma renomeação é uma remoção e uma coluna nova;
    - uma mudança de tipo;
    - ``NOT NULL`` numa coluna anulável;
    - uma coluna ``NOT NULL`` nova numa tabela com dados.

    A mensagem lista cada diferença e aponta ``rewrite``.

    Exemplo:

    .. code-block:: python

        delta.reconcile(uri, Operacao.__table__, storage)
        # SchemaDiffRefused: cad_operacoes: diff destrutivo, só por rewrite(...): valor: tipo ...
    """


class LogUnavailable(Exception):
    """[public] Um arquivo do log da tabela Delta entre duas versões não existe.

    Sem esse arquivo, as partições alteradas entre as duas versões não podem ser lidas do log. A
    mensagem manda publicar a tabela inteira.

    Exemplo:

    .. code-block:: python

        delta.version_diff(uri, 10, 58, Operacao.__table__, storage)
        # LogUnavailable: cad_operacoes: o log da versão 11 não existe; ... publique a tabela
    """


class SandboxError(ValueError):
    """[public] O sandbox não pode atender ao que foi pedido.

    O sandbox é o banco de trabalho da execução, no DuckDB ou no Redshift. A exceção cobre estes
    casos, e a mensagem nomeia o objeto de cada um:

    - Um nome já ocupado no sandbox, ou um objeto do sandbox que não serve ao que foi pedido. A
      mensagem nomeia o nome que ``create_table`` achou ocupado, a tabela que o ``appender`` não
      achou no sandbox, ou a view do ``ingest``, que não recebe lotes. O cliente lê a versão fixada
      por ``run.pinned_delta(table)`` sem ocupar nome.
    - Uma tabela sem versão fixada no ``ingest``, porque não existe no ambiente.
    - Uma coluna do resultado do motor Redshift num tipo fora do contrato. No resultado de
      ``query`` e de ``stream``, a mensagem nomeia a coluna de outro OID, ou de ``NUMERIC`` sem
      modificador, que o contrato não tem.
    - O motor Redshift sem as credenciais que o ``COPY`` e o ``UNLOAD`` pedem. Sem ``iam_role`` na
      configuração, as credenciais vêm da sessão ``boto3``, e a mensagem diz onde ela procurou.
    - O motor Redshift com a sessão numa transação que o cliente abriu. O ``append``, o
      ``appender``, o ``ingest`` e o ``pinned_delta`` recusam essa sessão, porque o ``COMMIT``
      deles confirmaria junto o trabalho da transação. A mensagem pede o ``COMMIT`` ou o
      ``ROLLBACK`` dela antes.
    - A memória lida do ambiente que não dá 1 MiB de ``memory_limit`` ao DuckDB. A memória é a que
      o processo ainda pode usar, de ``serialize_db.resources``, lida quando a configuração do
      DuckDB não informa o ``memory_limit``; abaixo de 2 MiB, ou negativa, ela é recusada, e a
      mensagem traz a leitura.

    Exemplo:

    .. code-block:: python

        engine.ingest(Lancamento.__table__, uri, 143)
        engine.ingest(Lancamento.__table__, uri, 143)
        # SandboxError: cad_lancamentos: o nome já está ocupado no sandbox
    """


class AuditFailed(Exception):
    """[public] A auditoria reprovou, ou ``publish_delta`` foi chamado sem a auditoria aprovada das
    partições.

    A auditoria é a conferência do contrato sobre uma tabela do sandbox, antes de publicar. A
    execução encerra sem tocar o Delta. A mensagem nomeia a tabela, as partições e as verificações
    reprovadas. O relatório, com o SQL e a amostra, vai ao log ``serialize_db.execution``.

    Exemplo:

    .. code-block:: python

        run.audit(Projetado.__table__, ["2026-08-31"])
        # AuditFailed: cad_lancamentos_projetados em ['2026-08-31']: reprovada em ['linhas']; ...
    """


class PublicationError(Exception):
    """[public] A publicação no Redshift não pode começar.

    A publicação copia as tabelas Delta de um snapshot para as tabelas do esquema do Redshift. Ela
    não começa nestes casos, e a mensagem diz o que o operador faz em cada um:

    - A tabela de controle ``serialize_db_publications`` não existe no esquema.
      ``serialize-db publish_redshift --init`` a cria, uma vez.
    - Uma tabela do modelo não tem versão a publicar, porque não está no snapshot pedido ou não
      existe no ambiente. No ``serialize-db publish_redshift``, ``--tables`` deixa essa tabela de
      fora.

    Exemplo:

    .. code-block:: python

        publication.publish_redshift(db, config, [Lancamento.__table__], "exec-42")
        # PublicationError: a tabela de controle ... não existe; crie-a uma vez com
        # serialize-db publish_redshift --init (create_publications_table)
    """
