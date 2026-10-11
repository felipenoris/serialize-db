"""``serialize_db``: os módulos públicos que a documentação do ``pdoc`` alcança, e a etiqueta de
nível na abertura de cada docstring.

O ``pdoc`` só documenta os submódulos que o ``__all__`` do pacote lista. Cada docstring de
``src/serialize_db/`` abre com ``[public]``, ``[protected]`` ou ``[private]``, o nível do nome que
documenta, e a marca ``[uso do cliente]``, a do nome que a avaliação da intenção de uso tornou
público, só aparece logo depois de ``[public]``. O teste não grava arquivo.
"""

from __future__ import annotations

import ast
import importlib
import pathlib
import pkgutil
from types import ModuleType

import pytest

import serialize_db

SOURCE_ROOT = pathlib.Path(serialize_db.__file__).parent
TAGS = {"public": "[public]", "protected": "[protected]", "private": "[private]"}
CLIENT_USE = "[uso do cliente]"


def packages() -> list[ModuleType]:
    """O pacote e cada subpacote dele, importados."""
    found = [serialize_db]
    for module in pkgutil.walk_packages(serialize_db.__path__, "serialize_db."):
        if module.ispkg:
            found.append(importlib.import_module(module.name))
    return found


@pytest.mark.parametrize("package", packages(), ids=lambda package: package.__name__)
def test_every_public_submodule_is_in_the_package_all(
    package: ModuleType,
) -> None:
    """Cada submódulo sem ``_`` no nome está no ``__all__`` do pacote que o contém, e nenhum nome
    com ``_`` está lá."""
    public = [
        module.name
        for module in pkgutil.iter_modules(package.__path__)
        if not module.name.startswith("_")
    ]
    assert public
    missing = [name for name in public if name not in package.__all__]
    assert missing == []
    assert [name for name in package.__all__ if name.startswith("_")] == []


def source_files() -> list[pathlib.Path]:
    """Cada arquivo ``.py`` de ``src/serialize_db/``, em ordem de caminho."""
    return sorted(path for path in SOURCE_ROOT.rglob("*.py") if "__pycache__" not in path.parts)


def exported_names(
    tree: ast.Module,
) -> list[str] | None:
    """Os nomes do ``__all__`` do módulo; ``None`` quando ele não declara um."""
    for statement in tree.body:
        targets: list[ast.expr] = []
        if isinstance(statement, ast.Assign):
            targets = statement.targets
        elif isinstance(statement, ast.AnnAssign):
            targets = [statement.target]
        for target in targets:
            if isinstance(target, ast.Name) and target.id == "__all__":
                return [element.value for element in statement.value.elts]
    return None


def is_dunder(
    name: str,
) -> bool:
    """Se o nome é ``__assim__``."""
    return name.startswith("__") and name.endswith("__")


def module_name_level(
    name: str,
    exported: list[str] | None,
) -> str:
    """O nível de um nome definido no módulo: ``_`` é privado, o ``__all__`` é público e o resto
    protegido."""
    if name.startswith("_") and not is_dunder(name):
        return "private"
    if exported is not None and name in exported:
        return "public"
    return "protected"


def member_level(
    name: str,
    owner_level: str,
) -> str:
    """O nível de um membro de classe ou de um atributo: um dunder tem o nível da classe, um nome
    com ``_`` ou numa classe privada é privado, e o resto tem o nível da classe."""
    if is_dunder(name):
        return owner_level
    if name.startswith("_") or owner_level == "private":
        return "private"
    return owner_level


def is_docstring(
    statement: ast.AST,
) -> bool:
    """Se o nó é uma string solta: a docstring de um módulo, de uma classe, de uma função ou de um
    atributo."""
    return (
        isinstance(statement, ast.Expr)
        and isinstance(statement.value, ast.Constant)
        and isinstance(statement.value.value, str)
    )


def assigned_name(
    statement: ast.stmt,
) -> str | None:
    """O nome que o comando atribui (``x = ...``, ``x: T = ...`` ou ``self.x = ...``); ``None`` em
    outro comando."""
    target = None
    if isinstance(statement, ast.AnnAssign):
        target = statement.target
    elif isinstance(statement, ast.Assign) and len(statement.targets) == 1:
        target = statement.targets[0]
    if isinstance(target, ast.Name):
        return target.id
    if isinstance(target, ast.Attribute):
        return target.attr
    return None


def docstring_levels(
    node: ast.AST,
    level: str,
    exported: list[str] | None,
    found: list[tuple[ast.Expr, str]],
) -> None:
    """Junta em ``found`` cada docstring do nó e do que ele contém, com o nível do nome que ela
    documenta."""
    body = getattr(node, "body", None)
    if body is None:
        return
    if body and is_docstring(body[0]):
        found.append((body[0], level))
    # A string logo depois de uma atribuição documenta o atributo atribuído.
    for previous, statement in zip(body, body[1:]):
        name = assigned_name(previous)
        if name is None or not is_docstring(statement):
            continue
        if isinstance(node, ast.Module):
            found.append((statement, module_name_level(name, exported)))
        else:
            found.append((statement, member_level(name, level)))
    for child in body:
        if not isinstance(child, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if isinstance(node, ast.Module):
            child_level = module_name_level(child.name, exported)
        elif isinstance(node, ast.ClassDef):
            child_level = member_level(child.name, level)
        else:
            child_level = "private"  # uma função aninhada em outra
        docstring_levels(child, child_level, exported, found)


def module_level(
    path: pathlib.Path,
) -> str:
    """O nível do módulo: ``__init__`` é público, e os outros seguem o ``__all__`` do pacote que os
    contém e o prefixo ``_``."""
    if path.stem == "__init__":
        return "public"
    package_tree = ast.parse((path.parent / "__init__.py").read_text(encoding="utf-8"))
    return module_name_level(path.stem, exported_names(package_tree))


@pytest.mark.parametrize(
    "path", source_files(), ids=lambda path: str(path.relative_to(SOURCE_ROOT))
)
def test_every_docstring_opens_with_the_level_of_its_name(
    path: pathlib.Path,
) -> None:
    """Cada docstring do arquivo abre com a etiqueta do nível do nome que documenta, e nenhuma
    docstring fica fora do percurso."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found: list[tuple[ast.Expr, str]] = []
    docstring_levels(tree, module_level(path), exported_names(tree), found)
    wrong = []
    for docstring, level in found:
        if not docstring.value.value.startswith(TAGS[level]):
            wrong.append(f"linha {docstring.lineno}: {TAGS[level]}")
    assert wrong == []
    assert len(found) == sum(1 for node in ast.walk(tree) if is_docstring(node))


@pytest.mark.parametrize(
    "path", source_files(), ids=lambda path: str(path.relative_to(SOURCE_ROOT))
)
def test_the_client_use_mark_follows_only_the_public_tag(
    path: pathlib.Path,
) -> None:
    """A marca ``[uso do cliente]`` aparece só na abertura ``[public] [uso do cliente]``: nunca
    depois de outra etiqueta nem no meio do texto."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    opening = f"{TAGS['public']} {CLIENT_USE}"
    wrong = []
    for node in ast.walk(tree):
        if not is_docstring(node):
            continue
        text = node.value.value
        if CLIENT_USE in text and not text.startswith(opening):
            wrong.append(f"linha {node.lineno}")
    assert wrong == []
