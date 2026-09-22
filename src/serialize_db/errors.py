"""As exceções da biblioteca, num módulo sem dependências.

Cada etapa acrescenta as suas: ``ContractError`` é a da etapa 1 (``schema``) e ``SqlError`` a da
etapa 2 (``sql``).
"""

__all__ = ["ContractError", "SqlError"]


class ContractError(ValueError):
    """Dados ou modelo fora do contrato.

    A mensagem nomeia a tabela e a coluna e diz o que o cliente faz antes de chamar de novo:
    arredondar, truncar, serializar, declarar ``String(n)``.
    """


class SqlError(ValueError):
    """Um statement que não vira texto executável, ou um texto cujos parâmetros não fecham.

    A mensagem nomeia os parâmetros em falta ou sobrando, o ``bindparam`` sem valor ou o sentinela
    que ficou no texto, e diz o que o cliente faz: usar ``param``, completar o dicionário, ler o
    texto por ``read_sql``.
    """
