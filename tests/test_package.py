"""``serialize_db``: os módulos públicos que a documentação do ``pdoc`` alcança.

O ``pdoc`` só documenta os submódulos que o ``__all__`` do pacote lista. O teste não grava arquivo.
"""

from __future__ import annotations

import pkgutil
from types import ModuleType

import pytest

import serialize_db
from serialize_db import engine


@pytest.mark.parametrize("package", [serialize_db, engine], ids=lambda package: package.__name__)
def test_every_public_submodule_is_in_the_package_all(package: ModuleType) -> None:
    """Cada submódulo sem ``_`` no nome está no ``__all__`` do pacote que o contém."""
    public = [info.name for info in pkgutil.iter_modules(package.__path__)
              if not info.name.startswith("_")]
    assert public
    missing = [name for name in public if name not in package.__all__]
    assert missing == []
