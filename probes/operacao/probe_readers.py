"""Sonda dos leitores da base publicada: o Delta, do snapshot do canal ``default``, e o Redshift.

A sonda roda depois da publicação da base inteira pelo canal ``default`` e recebe a raiz da base, a
do ``--root`` da publicação; o leitor Redshift lê as tabelas publicadas pelas variáveis
``SERIALIZE_DB_REDSHIFT_*``. Ela:

1. abre o leitor Delta, com uma view por tabela do modelo, e imprime o tempo da abertura e as
   versões lidas;
2. conta as linhas de ``cad_contas`` pelos dois leitores, com o mesmo statement.

Checagem: a contagem de ``cad_contas`` igual nos dois leitores.

A sonda só lê a base e as tabelas publicadas, sem raiz de trabalho. O relatório sai no terminal e
em ``probes/output/operacao_leitores_<data-hora>.txt``.

Exemplo:

.. code-block:: shell

    export SERIALIZE_DB_REDSHIFT_WORKGROUP=workgroup SERIALIZE_DB_REDSHIFT_SCHEMA=esquema
    .venv/bin/python probes/operacao/probe_readers.py s3://bucket/prefixo
"""

from __future__ import annotations

import argparse
import time

import operation_lib as lib
import sqlalchemy as sa

from serialize_db.execution import Database

TABLE = "cad_contas"


def parse_arguments() -> argparse.Namespace:
    """A linha de comando: ``root``, a raiz da base publicada; o uso errado sai com o código 2."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("root", metavar="raiz", help="a raiz da base, a do --root da publicação")
    return parser.parse_args()


def main() -> None:
    """Os dois leitores sobre a base publicada, com o relatório no terminal e em
    ``probes/output/``."""
    options = parse_arguments()
    output = lib.open_report("leitores")
    db = Database(options.root, lib.ENVIRONMENT, lib.Base.metadata)
    print(f"base: {db.storage.uri}; ambiente {db.environment}; saída: {output}")
    table = lib.Base.metadata.tables[TABLE]
    statement = sa.select(sa.func.count()).select_from(table)

    # O leitor Delta: o tempo da abertura, que cria uma view por tabela, e a contagem.
    started = time.perf_counter()
    with db.open_delta() as reader:
        seconds = time.perf_counter() - started
        print(f"leitor Delta aberto em {seconds:.3f} s: {reader.versions}")
        delta_count = reader.query(statement).column(0)[0].as_py()
    print(f"delta: {delta_count} linhas em {TABLE}")

    # O leitor Redshift sobre as tabelas publicadas, com o mesmo statement.
    with db.open_redshift() as reader:
        redshift_count = reader.query(statement).column(0)[0].as_py()
    print(f"redshift: {redshift_count} linhas em {TABLE}")

    problems = []
    if delta_count != redshift_count:
        problems.append(f"{delta_count} linhas no Delta, {redshift_count} no Redshift")
    lib.check(f"a contagem de {TABLE} igual nos dois leitores", problems)
    lib.finish_on_base()


if __name__ == "__main__":
    lib.run(main)
