"""[private] A gravação dos arquivos gerados numa pasta e o diff deles contra a pasta.

Os arquivos gerados são os que o pipeline versiona no repositório dele: o esquema de cada tabela e
o texto SQL de cada statement. ``serialize_db.schema`` e ``serialize_db.sql`` geram o conteúdo em
memória, um texto por nome de arquivo, e chamam ``write_files`` para gravá-lo e ``diff_files``
para compará-lo com o que está versionado.
"""

from __future__ import annotations

import difflib
import os

__all__: list[str] = []

# O aviso depois da linha que termina o arquivo sem \n, no lugar do "\ No newline at end of file"
# do diff.
_NO_FINAL_NEWLINE = "\\ Sem quebra de linha no fim do arquivo"


def _versioned_text(
    path: str,
) -> str:
    """[private] O conteúdo do arquivo versionado em ``path``, lido em UTF-8; vazio quando ele não
    existe."""
    if not os.path.exists(path):
        return ""
    with open(path, encoding="utf-8") as handle:
        return handle.read()


def _stale_names(
    files: dict[str, str],
    directory: str,
    suffixes: tuple[str, ...],
) -> list[str]:
    """[private] Os nomes dos arquivos de ``directory`` que parecem gerados, pelo nome terminado num
    de ``suffixes``, mas que a geração não produz mais, como os de uma tabela que saiu do modelo;
    para ``diff_files``. Vazio numa pasta que não existe."""
    if not os.path.isdir(directory):
        return []
    stale = []
    for name in os.listdir(directory):
        path = os.path.join(directory, name)
        looks_generated = name.endswith(suffixes) and os.path.isfile(path)
        if looks_generated and name not in files:
            stale.append(name)
    return stale


def _file_diff(
    versioned: str,
    generated: str,
    path: str,
) -> list[str]:
    """[private] O diff unificado entre o texto versionado e o gerado de um arquivo, uma linha do
    diff por item da lista, sem o ``\\n`` final de cada uma; para ``diff_files``.

    As linhas são comparadas com o ``\\n`` de cada uma. Por isso o arquivo sem ``\\n`` no fim difere
    do gerado, e a linha sem ``\\n`` sai seguida do aviso ``_NO_FINAL_NEWLINE``.
    """
    lines = []
    diff = difflib.unified_diff(
        versioned.splitlines(keepends=True),
        generated.splitlines(keepends=True),
        fromfile=path,
        tofile=f"{path} (gerado)",
    )
    for line in diff:
        if line.endswith("\n"):
            lines.append(line.removesuffix("\n"))
            continue
        lines.append(line)
        lines.append(_NO_FINAL_NEWLINE)
    return lines


def write_files(
    files: dict[str, str],
    directory: str,
) -> list[str]:
    """[protected] Grava cada texto de ``files`` em ``directory/<nome>`` e devolve os caminhos
    gravados, em ordem de nome; para ``serialize_db.schema`` e ``serialize_db.sql``.

    A pasta é criada se preciso. Um arquivo da pasta que a geração não produz fica como está: a
    função não apaga nada."""
    os.makedirs(directory, exist_ok=True)
    written = []
    for name, content in sorted(files.items()):
        path = os.path.join(directory, name)
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(content)
        written.append(path)
    return written


def diff_files(
    files: dict[str, str],
    directory: str,
    suffixes: tuple[str, ...],
) -> list[str]:
    """[protected] O diff unificado dos arquivos de ``directory`` contra os textos gerados em
    ``files``, em ordem de nome; para ``serialize_db.schema`` e ``serialize_db.sql``.

    A lista sai vazia quando nada mudou. O texto é comparado exato, o ``\\n`` do fim inclusive. Um
    arquivo ausente da pasta aparece inteiro como acrescentado. Um arquivo da pasta com nome
    terminado num de ``suffixes`` que a geração não produz aparece inteiro como removido. Nada é
    gravado.
    """
    expected = dict(files)
    for name in _stale_names(files, directory, suffixes):
        expected[name] = ""
    diff = []
    for name, content in sorted(expected.items()):
        path = os.path.join(directory, name)
        diff.extend(_file_diff(_versioned_text(path), content, path))
    return diff
