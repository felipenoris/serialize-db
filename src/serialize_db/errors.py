"""As exceções da biblioteca, num módulo sem dependências.

Cada etapa acrescenta as suas: ``ContractError`` é a da etapa 1 (``schema``), ``SqlError`` a da
etapa 2 (``sql``), e ``ConflictError``, ``ExecutionConflict``, ``RegistrationRefused``,
``SchemaDiffRefused`` e ``LogUnavailable`` as da etapa 3 (``storage`` e ``delta``), que
``serialize_db.delta`` levanta e a execução captura.
"""

__all__ = [
    "ConflictError",
    "ContractError",
    "ExecutionConflict",
    "LogUnavailable",
    "RegistrationRefused",
    "SchemaDiffRefused",
    "SqlError",
]


class ContractError(ValueError):
    """Dados ou modelo fora do contrato.

    A mensagem nomeia a tabela e a coluna e diz o que o cliente faz antes de chamar de novo:
    arredondar, truncar, serializar, declarar ``String(n)``.
    """


class SqlError(ValueError):
    """Um statement que não vira texto executável, ou um texto cujos parâmetros não fecham.

    A mensagem nomeia o parâmetro de nome inválido, os parâmetros em falta ou sobrando ou o
    sentinela que ficou no texto, e diz o que o cliente faz: renomear o ``bindparam``, completar o
    dicionário, ler o texto por ``read_sql``.
    """


class ConflictError(Exception):
    """A escrita condicional perdeu: outro escritor mudou o objeto entre a leitura e a escrita.

    É o 412 do S3 no ``IfMatch`` ou no ``IfNoneMatch``, e a impressão digital diferente, ou o
    arquivo já existente, na pasta local. Nada foi gravado; quem chama lê de novo e decide.
    """


class ExecutionConflict(Exception):
    """Outra execução gravou a mesma partição, ou avançou a tabela com dados desde a abertura.

    É o ``CommitFailedError`` do delta-rs num ``overwrite`` ou num registro de arquivos, e a
    versão fixada que ficou para trás. Nenhum commit foi feito pela chamada que falhou.
    """


class RegistrationRefused(Exception):
    """Uma conferência de ``register_files`` reprovou antes do commit, ou a releitura reprovou
    depois dele e ``restore`` voltou a versão anterior.

    A mensagem nomeia o arquivo e a conferência; o arquivo fica órfão na pasta da tabela até um
    ``vacuum(full=True)``.
    """


class SchemaDiffRefused(Exception):
    """O diff entre o modelo e a tabela Delta é destrutivo: renomeação, remoção, mudança de tipo
    ou coluna ``NOT NULL`` nova numa tabela com dados. A mensagem lista cada diferença e aponta
    ``rewrite``."""


class LogUnavailable(Exception):
    """Um arquivo do log entre duas versões não existe; a mensagem manda publicar a tabela
    inteira, porque as partições alteradas não podem ser lidas do log."""
