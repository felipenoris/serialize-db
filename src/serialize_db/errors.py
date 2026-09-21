"""As exceções da biblioteca, num módulo sem dependências.

Cada etapa acrescenta as suas; ``ContractError`` é a da etapa 1 (``schema``).
"""


class ContractError(ValueError):
    """Dados ou modelo fora do contrato.

    A mensagem nomeia a tabela e a coluna e diz o que o cliente faz antes de chamar de novo:
    arredondar, truncar, serializar, declarar ``String(n)``.
    """
