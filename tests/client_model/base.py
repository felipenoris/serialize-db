"""A ``Base`` declarativa do modelo cliente: todas as tabelas entram no mesmo ``Base.metadata``."""

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """A base declarativa única do modelo; o pacote recebe ``Base.metadata``."""
