"""Os arquivos gerados que o pipeline versiona: a gravação numa pasta e o diff contra a pasta.

``serialize_db.schema`` e ``serialize_db.sql`` geram o conteúdo em memória, um texto por nome de
arquivo, e usam as duas funções deste módulo para gravá-lo e para compará-lo com o que está
versionado.
"""

from __future__ import annotations

import difflib
import os

__all__: list[str] = []


def _versioned_text(path: str) -> str:
    """O conteúdo do arquivo versionado, ou vazio quando ele não existe."""
    if not os.path.exists(path):
        return ""
    with open(path, encoding="utf-8") as handle:
        return handle.read()


def write_files(files: dict[str, str], directory: str) -> list[str]:
    """Grava cada texto em ``directory/<nome>``, criando a pasta se preciso, e devolve os caminhos
    gravados em ordem de nome."""
    os.makedirs(directory, exist_ok=True)
    written = []
    for name, content in sorted(files.items()):
        path = os.path.join(directory, name)
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(content)
        written.append(path)
    return written


def diff_files(files: dict[str, str], directory: str) -> list[str]:
    """O diff unificado dos arquivos de ``directory`` contra os textos gerados, em ordem de nome.

    Vazio quando nada mudou; um arquivo ausente aparece inteiro como acrescentado. Nada é gravado.
    """
    diff = []
    for name, content in sorted(files.items()):
        path = os.path.join(directory, name)
        versioned_lines = _versioned_text(path).splitlines()
        generated_lines = content.splitlines()
        diff.extend(difflib.unified_diff(versioned_lines, generated_lines, fromfile=path,
                                         tofile=f"{path} (gerado)", lineterm=""))
    return diff
