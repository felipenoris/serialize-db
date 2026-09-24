"""``serialize_db.resources``: as CPUs e a memória lidas de um ``/proc`` e de um cgroup fabricados.

Os testes gravam sob ``SERIALIZE_DB_TEST_LOCAL_ROOT`` (marcador ``local``) as pastas ``proc/`` e
``cgroup/`` que tomam o lugar de ``/proc`` e ``/sys/fs/cgroup``, e fixam em 8 as CPUs da afinidade.
Eles conferem o cgroup v2 com a folga que devolve o cache de arquivos e a cota arredondada para
cima, o ``MemAvailable`` menor que a folga, o ancestral mais apertado, o cgroup v1 com a pasta do
processo e o contêiner que monta o próprio cgroup como raiz, e a máquina sem ``/proc``, que fica
com a memória física e as CPUs do Python.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest

from conftest import LocalLocation
from serialize_db import resources

pytestmark = pytest.mark.local

GIB = 2**30


def fabricate(folder: Path, files: dict[str, str]) -> None:
    """Grava cada arquivo de ``files`` pelo caminho relativo a ``folder``."""
    for relative, text in files.items():
        path = folder / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)


def meminfo(available: int) -> str:
    """O ``/proc/meminfo`` com ``MemAvailable`` de ``available`` bytes."""
    return f"MemTotal:       16000000 kB\nMemAvailable:   {available // 1024} kB\n"


@pytest.fixture
def machine(local_location: LocalLocation, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Uma pasta nova com ``proc/`` e ``cgroup/`` no lugar das do sistema e 8 CPUs na afinidade."""
    folder = Path(local_location.child(f"recursos/{uuid.uuid4().hex[:8]}"))
    (folder / "proc").mkdir(parents=True)
    (folder / "cgroup").mkdir()
    monkeypatch.setattr(resources, "_PROC", folder / "proc")
    monkeypatch.setattr(resources, "_CGROUP_ROOT", folder / "cgroup")
    monkeypatch.setattr(os, "process_cpu_count", lambda: 8)
    return folder


def test_cgroup_v2_gives_back_the_file_cache_and_rounds_the_quota_up(machine: Path) -> None:
    """A folga do cgroup v2 é o limite menos o uso, com o cache de arquivos de volta; a cota de
    1,5 CPU dá 2. Um ``MemAvailable`` menor que a folga ganha dela."""
    fabricate(machine, {
        "proc/meminfo": meminfo(10 * GIB),
        "proc/self/cgroup": "0::/app\n",
        "cgroup/app/memory.max": f"{4 * GIB}\n",
        "cgroup/app/memory.current": f"{3 * GIB // 2}\n",
        "cgroup/app/memory.stat": f"anon {GIB // 2}\nfile {GIB}\n",
        "cgroup/app/cpu.max": "150000 100000\n",
    })
    assert resources.available_memory() == 7 * GIB // 2
    assert resources.available_cpus() == 2

    fabricate(machine, {"proc/meminfo": meminfo(GIB)})
    assert resources.available_memory() == GIB


def test_the_tightest_ancestor_wins(machine: Path) -> None:
    """O limite de um ancestral vale para o cgroup do processo: o pai limita a memória, e o filho,
    sem limite de memória, limita a CPU a meia, que arredonda para 1."""
    fabricate(machine, {
        "proc/meminfo": meminfo(10 * GIB),
        "proc/self/cgroup": "0::/pai/filho\n",
        "cgroup/pai/memory.max": f"{2 * GIB}\n",
        "cgroup/pai/memory.current": f"{GIB // 2}\n",
        "cgroup/pai/cpu.max": "max 100000\n",
        "cgroup/pai/filho/memory.max": "max\n",
        "cgroup/pai/filho/memory.current": f"{GIB // 4}\n",
        "cgroup/pai/filho/cpu.max": "50000 100000\n",
    })
    assert resources.available_memory() == 3 * GIB // 2
    assert resources.available_cpus() == 1


def test_cgroup_v1_with_the_process_folder_and_with_the_container_root(machine: Path) -> None:
    """No v1, a memória lê a pasta do processo e as ancestrais até a montagem, e a mais apertada é
    a montagem; a CPU não acha a pasta do processo, como no contêiner que monta o próprio cgroup, e
    lê a cota da montagem: 2,5 CPUs dão 3. A linha do v2 sem controlador não limita nada."""
    fabricate(machine, {
        "proc/meminfo": meminfo(10 * GIB),
        "proc/self/cgroup": (
            "4:memory:/docker/abc\n3:cpu,cpuacct:/docker/abc\n1:name=systemd:/docker/abc\n0::/\n"
        ),
        "cgroup/memory/docker/abc/memory.limit_in_bytes": "9223372036854771712\n",
        "cgroup/memory/docker/abc/memory.usage_in_bytes": f"{GIB}\n",
        "cgroup/memory/docker/abc/memory.stat": f"cache {GIB // 2}\ntotal_cache {GIB // 2}\n",
        "cgroup/memory/memory.limit_in_bytes": f"{3 * GIB}\n",
        "cgroup/memory/memory.usage_in_bytes": f"{GIB}\n",
        "cgroup/memory/memory.stat": f"cache {GIB // 2}\ntotal_cache {GIB // 2}\n",
        "cgroup/cpu/cpu.cfs_quota_us": "250000\n",
        "cgroup/cpu/cpu.cfs_period_us": "100000\n",
    })
    assert resources.available_memory() == 5 * GIB // 2
    assert resources.available_cpus() == 3


def test_without_proc_the_physical_memory_and_the_python_cpus(machine: Path) -> None:
    """Sem ``/proc/meminfo`` nem ``/proc/self/cgroup``, como fora do Linux, a memória é a física e
    as CPUs são as da afinidade."""
    physical = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    assert resources.available_memory() == physical
    assert resources.available_cpus() == 8
