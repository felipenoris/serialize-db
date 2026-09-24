"""As exceções da biblioteca, num módulo sem dependências.

Cada etapa acrescenta as suas: ``ContractError`` é a da etapa 1 (``schema``), ``SqlError`` a da
etapa 2 (``sql``), ``ConflictError``, ``ExecutionConflict``, ``RegistrationRefused``,
``SchemaDiffRefused`` e ``LogUnavailable`` as da etapa 3 (``storage`` e ``delta``), que
``serialize_db.delta`` levanta e a execução captura, ``SandboxError`` a da etapa 4 (os motores),
``AuditFailed`` a da etapa 6 (a execução) e ``PublicationError`` a da etapa 8 (a publicação).
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
    """Dados ou modelo fora do contrato.

    A mensagem nomeia a tabela e a coluna e diz o que o cliente faz antes de chamar de novo:
    arredondar, truncar, serializar, declarar ``String(n)``.

    Exemplo:

    .. code-block:: python

        try:
            schema.cast(batch, Operacao.__table__)
        except ContractError as error:
            print(error)   # "cad_operacoes.data: timestamp com hora numa coluna Date; ..."
    """


class SqlError(ValueError):
    """Um statement que não vira texto executável, ou um texto cujos parâmetros não fecham.

    A mensagem nomeia o parâmetro de nome inválido, os parâmetros em falta ou sobrando ou o
    sentinela que ficou no texto, e diz o que o cliente faz: renomear o ``bindparam``, completar o
    dicionário, ler o texto por ``read_sql``.

    Exemplo:

    .. code-block:: python

        sql.bind("SELECT :valor", {}, "duckdb")
        # SqlError: parâmetros do texto ['valor'] e do dicionário [] não fecham
    """


class ConflictError(Exception):
    """A escrita condicional perdeu: outro escritor mudou o objeto entre a leitura e a escrita.

    É o 412 do S3 no ``IfMatch`` ou no ``IfNoneMatch``, e a impressão digital diferente, ou o
    arquivo já existente, na pasta local. Nada foi gravado; quem chama lê de novo e decide.

    Exemplo:

    .. code-block:: python

        storage.create_text("prod/_serialize_db/snapshots.json", "{}")
        storage.create_text("prod/_serialize_db/snapshots.json", "{}")   # ConflictError
    """


class ExecutionConflict(Exception):
    """Outra execução gravou a mesma partição, ou avançou a tabela com dados desde a abertura.

    É o ``CommitFailedError`` do delta-rs num ``overwrite`` ou num registro de arquivos, e a
    versão fixada que ficou para trás. Nenhum commit foi feito pela chamada que falhou.

    Exemplo:

    .. code-block:: python

        try:
            run.publish(Projetado.__table__, partitions=["2026-08-31"])
        except ExecutionConflict:
            raise   # nada desta chamada foi gravado; uma execução nova parte da versão atual
    """


class RegistrationRefused(Exception):
    """Uma conferência de ``register_files`` reprovou antes do commit, ou a releitura reprovou
    depois dele e ``restore`` voltou a versão anterior.

    A mensagem nomeia o arquivo e a conferência; o arquivo fica órfão na pasta da tabela até um
    ``vacuum(full=True)``.

    Exemplo:

    .. code-block:: python

        delta.register_files(uri, Operacao.__table__, [file], "2026-08-31", metadata, storage,
                             expected_rows=999)
        # RegistrationRefused: 1000 linhas nos arquivos, 999 na fonte
    """


class SchemaDiffRefused(Exception):
    """O diff entre o modelo e a tabela Delta é destrutivo: renomeação, remoção, mudança de tipo
    ou coluna ``NOT NULL`` nova numa tabela com dados. A mensagem lista cada diferença e aponta
    ``rewrite``.

    Exemplo:

    .. code-block:: python

        delta.reconcile(uri, Operacao.__table__, storage)
        # SchemaDiffRefused: cad_operacoes: diff destrutivo, só por rewrite(...): valor: tipo ...
    """


class LogUnavailable(Exception):
    """Um arquivo do log entre duas versões não existe; a mensagem manda publicar a tabela
    inteira, porque as partições alteradas não podem ser lidas do log.

    Exemplo:

    .. code-block:: python

        delta.version_diff(uri, 10, 58, Operacao.__table__, storage)
        # LogUnavailable: cad_operacoes: o log da versão 11 não existe; ... publique a tabela
    """


class SandboxError(ValueError):
    """Um nome já ocupado no sandbox, ou um objeto do sandbox que não serve ao que foi pedido.

    A mensagem nomeia o objeto e diz o que o cliente faz: ler a versão publicada por
    ``run.published(table)`` em vez de gravar no nome que o ``ingest`` ocupou, ou abrir um
    ``loader`` só por tabela.

    Exemplo:

    .. code-block:: python

        engine.ingest(Lancamento.__table__, uri, 143)
        engine.ingest(Lancamento.__table__, uri, 143)   # SandboxError: o nome está ocupado
    """


class AuditFailed(Exception):
    """A auditoria reprovou, ou ``publish`` foi chamado sem a auditoria aprovada das partições.

    A execução encerra sem tocar o Delta; a mensagem nomeia a tabela, as partições e as
    verificações reprovadas, e o relatório, com o SQL e a amostra, vai para o log.

    Exemplo:

    .. code-block:: python

        run.audit(Projetado.__table__, ["2026-08-31"])
        # AuditFailed: cad_lancamentos_projetados em ['2026-08-31']: reprovada em ['linhas']; ...
    """


class PublicationError(Exception):
    """A publicação no Redshift não pode começar: a tabela de controle ``serialize_db_publications``
    não existe no esquema, ou a execução não recebeu a configuração do Redshift.

    A mensagem diz o que o operador faz: ``serialize-db publish --init`` cria a tabela de controle
    uma vez, e ``Execution(..., redshift=RedshiftConfig(...))`` dá a configuração.

    Exemplo:

    .. code-block:: python

        publication.publish_redshift(db, config, [Lancamento.__table__], "exec-42")
        # PublicationError: a tabela de controle ... não existe; crie-a uma vez com
        # serialize-db publish --init (create_publications_table)
    """
