"""``serialize_db``: os módulos públicos que a documentação do ``pdoc`` alcança.

O ``pdoc`` só documenta os submódulos que o ``__all__`` do pacote lista. O teste não grava arquivo.
"""

from __future__ import annotations

import importlib
import pkgutil
from types import ModuleType

import pytest

import serialize_db


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
