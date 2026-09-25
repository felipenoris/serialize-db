"""As exceções da biblioteca, num módulo sem dependências, que os outros importam sem ciclo.

Cada etapa acrescenta as suas: ``ContractError`` é a da etapa 1 (``schema``), ``SqlError`` a da
etapa 2 (``sql``), ``ConflictError``, ``ExecutionConflict``, ``RegistrationRefused``,
``SchemaDiffRefused`` e ``LogUnavailable`` as da etapa 3 (``storage`` e ``delta``), que a
execução deixa chegar ao cliente, ``SandboxError`` a da etapa 4 (os motores), ``AuditFailed`` a
da etapa 6 (a execução) e ``PublicationError`` a da etapa 8 (a publicação). A linha de comando
captura ``ConflictError`` e ``ExecutionConflict`` e sai com 2.
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
    """Dados, modelo ou configuração fora do contrato.

    A mensagem nomeia a tabela e a coluna e diz o que o cliente faz antes de chamar de novo:
    arredondar, truncar, serializar, declarar ``String(n)``. Na configuração, nomeia o que falta
    ou sobra: o ``RedshiftConfig`` sem conexão, o ``execution_id`` longo demais para o prefixo do
    sandbox do Redshift.

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
    sentinela que ficou no texto; o cliente renomeia o ``bindparam``, completa o dicionário ou lê
    o texto por ``read_sql``, que só a mensagem do sentinela indica.

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

        storage.create_text("prd/_serialize_db/snapshots.json", "{}")
        storage.create_text("prd/_serialize_db/snapshots.json", "{}")   # ConflictError
    """


class ExecutionConflict(Exception):
    """Outra execução gravou a mesma partição, ou avançou a tabela com dados desde a abertura.

    É o ``CommitFailedError`` do delta-rs num ``overwrite`` ou num registro de arquivos, a versão
    fixada que ficou para trás e, na publicação no Redshift, a linha de controle que mudou desde a
    leitura, o ``1023`` e a tabela publicada que outra primeira publicação criou. Nenhum commit foi
    feito pela chamada que falhou.

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

    A mensagem nomeia a conferência e, nas de cada arquivo, o arquivo; os arquivos ficam órfãos na
    pasta da tabela até um ``vacuum(full=True)``. ``deep_copy`` também a levanta, no destino que
    registra um arquivo fora da versão copiada e na contagem da cópia diferente da soma das ações.

    Exemplo:

    .. code-block:: python

        delta.register_files(uri, Operacao.__table__, [file], "2026-08-31", metadata, storage,
                             expected_rows=999)
        # RegistrationRefused: 1000 linhas nos arquivos, 999 na fonte
    """


class SchemaDiffRefused(Exception):
    """O diff entre o modelo e a tabela Delta é destrutivo: renomeação, remoção, mudança de tipo,
    ``NOT NULL`` numa coluna anulável ou coluna ``NOT NULL`` nova numa tabela com dados. A mensagem
    lista cada diferença e aponta ``rewrite``.

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
    """Um nome já ocupado no sandbox, um objeto do sandbox que não serve ao que foi pedido, ou o
    motor Redshift sem as credenciais que o ``COPY`` e o ``UNLOAD`` pedem.

    A mensagem nomeia o objeto; o cliente lê a versão publicada por ``run.published(table)`` em
    vez de gravar no nome que o ``ingest`` ocupou, como a mensagem do ``loader`` indica, ou abre
    um ``loader`` só por tabela. Sem ``iam_role`` na configuração, as credenciais vêm da sessão
    ``boto3``, e a mensagem diz onde ela procurou.

    Exemplo:

    .. code-block:: python

        engine.ingest(Lancamento.__table__, uri, 143)
        engine.ingest(Lancamento.__table__, uri, 143)
        # SandboxError: cad_lancamentos: o nome já está ocupado no sandbox
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
    não existe no esquema, ou uma tabela do modelo não tem versão a publicar, porque não está no
    snapshot pedido ou não existe no ambiente.

    A mensagem diz o que o operador faz: ``serialize-db publish --init`` cria a tabela de controle
    uma vez, e, no ``serialize-db publish``, ``--tables`` deixa de fora a tabela sem versão.

    Exemplo:

    .. code-block:: python

        publication.publish_redshift(db, config, [Lancamento.__table__], "exec-42")
        # PublicationError: a tabela de controle ... não existe; crie-a uma vez com
        # serialize-db publish --init (create_publications_table)
    """
