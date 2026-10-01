"""Os arquivos gerados que o pipeline versiona: a gravação numa pasta e o diff contra a pasta.

``serialize_db.schema`` e ``serialize_db.sql`` geram o conteúdo em memória, um texto por nome de
arquivo, e usam as duas funções deste módulo para gravá-lo e para compará-lo com o que está
versionado.
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
    """O conteúdo do arquivo versionado, ou vazio quando ele não existe."""
    if not os.path.exists(path):
        return ""
    with open(path, encoding="utf-8") as handle:
        return handle.read()


def _stale_names(
    files: dict[str, str],
    directory: str,
    suffixes: tuple[str, ...],
) -> list[str]:
    """Os arquivos de ``directory`` com nome terminado num de ``suffixes`` que a geração não
    produz, como os de uma tabela que saiu do modelo; vazio numa pasta que não existe."""
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
    """O diff unificado de um arquivo, uma linha do diff por item, sem o ``\\n``.

    As linhas são comparadas com o ``\\n`` de cada uma, então o arquivo sem ``\\n`` no fim difere
    do gerado; a linha sem ``\\n`` sai seguida de ``_NO_FINAL_NEWLINE``.
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
    """Grava cada texto em ``directory/<nome>``, criando a pasta se preciso, e devolve os caminhos
    gravados em ordem de nome; um arquivo da pasta que a geração não produz fica como está."""
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
    """O diff unificado dos arquivos de ``directory`` contra os textos gerados, em ordem de nome.

    Vazio quando nada mudou. O texto é comparado exato, o ``\\n`` do fim inclusive; um arquivo
    ausente aparece inteiro como acrescentado, e um arquivo da pasta com nome terminado num de
    ``suffixes`` que a geração não produz aparece inteiro como removido. Nada é gravado.
    """
    expected = dict(files)
    for name in _stale_names(files, directory, suffixes):
        expected[name] = ""
    diff = []
    for name, content in sorted(expected.items()):
        path = os.path.join(directory, name)
        diff.extend(_file_diff(_versioned_text(path), content, path))
    return diff
