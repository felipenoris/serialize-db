"""As CPUs e a memória que o processo pode usar, lidas do ambiente a cada chamada.

O pacote roda em máquinas de tamanhos diferentes, e os limites do DuckDB saem destas leituras, não
de um valor fixo no código (``serialize_db.engine.duckdb.environment_limits``). No Linux, as
leituras respeitam o cgroup do processo, v1 e v2, com que um contêiner limita as CPUs e a memória
abaixo das da máquina, e o menor limite no caminho do cgroup até a raiz é o que vale. Fora do Linux,
vale a memória física e as CPUs que o Python lê. ``peak_rss_mb`` lê o pico de memória residente
do próprio processo, a medida que o script de migração, os subcomandos de operação e a publicação
imprimem por tabela.

Exemplo:

.. code-block:: python

    from serialize_db.resources import available_cpus, available_memory, peak_rss_mb

    print(available_cpus(), f"{available_memory() / 2**30:.1f} GiB", f"{peak_rss_mb():.0f} MB")
"""

from __future__ import annotations

import math
import os
import re
import resource
import sys
from pathlib import Path

__all__ = ["available_cpus", "available_memory", "peak_rss_mb"]

# As raízes lidas; os testes as trocam por pastas fabricadas.
_PROC = Path("/proc")
_CGROUP_ROOT = Path("/sys/fs/cgroup")

# Por versão do cgroup: o arquivo do limite de memória, o do uso e a chave do cache de arquivos em
# memory.stat, a parte do uso que o kernel devolve antes de matar um processo.
_MEMORY_FILES = {
    1: ("memory.limit_in_bytes", "memory.usage_in_bytes", "total_cache"),
    2: ("memory.max", "memory.current", "file"),
}


def available_cpus() -> int:
    """As CPUs que o processo pode usar.

    Exemplo:

    .. code-block:: python

        available_cpus()   # 4 numa máquina de 4 vCPUs sem cota; 2 num contêiner com cota de 1,5

    :return: as CPUs da afinidade do processo (``os.process_cpu_count``, que a variável
        ``PYTHON_CPU_COUNT`` sobrepõe), limitadas pela menor cota de CPU do cgroup, arredondada
        para cima como o DuckDB arredonda; ao menos 1.
    """
    cpus = os.process_cpu_count() or 1
    quota = _cgroup_cpu_quota()
    if quota is None:
        return cpus
    return max(1, min(cpus, math.ceil(quota)))


def available_memory() -> int:
    """A memória que o processo ainda pode usar. A memória que os processos da máquina já ocupam,
    o próprio incluído, fica de fora.

    Exemplo:

    .. code-block:: python

        available_memory() / 2**30   # 14.7 numa máquina de 16 GiB com pouco em uso

    :return: em bytes, a menor entre a física, a disponível no sistema (``MemAvailable`` de
        ``/proc/meminfo``, que conta como livre o cache de arquivos que o kernel devolve) e a
        folga do cgroup (o limite menos o uso fora do cache de arquivos).
    """
    readings = [os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")]
    system = _meminfo_available()
    if system is not None:
        readings.append(system)
    room = _cgroup_memory_room()
    if room is not None:
        readings.append(room)
    return min(readings)


def peak_rss_mb() -> float:
    """O pico de memória residente do próprio processo até agora, em MB: no Linux, o ``VmHWM`` de
    ``/proc/self/status``, em KB; fora dele, o ``ru_maxrss`` do processo, em bytes no macOS e em
    KB nos outros Unix. Num processo filho, a leitura é a do filho, não a do pai.

    Exemplo:

    .. code-block:: python

        peak_rss_mb()   # 297.2 no script de migração, depois das importações
    """
    status = _PROC / "self" / "status"
    if status.exists():
        kilobytes = re.search(r"^VmHWM:\s+(\d+)", status.read_text(), re.MULTILINE).group(1)
        return int(kilobytes) / 1024
    maxrss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    unit = 1024 * 1024 if sys.platform == "darwin" else 1024
    return maxrss / unit


def _meminfo_available() -> int | None:
    """``MemAvailable`` de ``/proc/meminfo``, em bytes; ``None`` sem o arquivo, fora do Linux."""
    path = _PROC / "meminfo"
    if not path.is_file():
        return None
    found = re.search(r"^MemAvailable:\s+(\d+) kB", path.read_text(), re.MULTILINE)
    if found is None:
        return None
    return int(found.group(1)) * 1024


def _cgroup_memory_room() -> int | None:
    """A menor folga de memória nos cgroups do processo, em bytes: o limite menos o uso, com o
    cache de arquivos de volta; ``None`` quando nenhuma pasta dá limite e uso."""
    rooms = []
    for folder, version in _cgroup_folders("memory"):
        limit_file, usage_file, cache_key = _MEMORY_FILES[version]
        limit = _read_number(folder / limit_file)
        usage = _read_number(folder / usage_file)
        if limit is None or usage is None:
            continue
        cache = _stat_value(folder / "memory.stat", cache_key)
        rooms.append(limit - usage + cache)
    return min(rooms, default=None)


def _cgroup_cpu_quota() -> float | None:
    """A menor cota de CPU nos cgroups do processo, em CPUs: ``cpu.max`` no v2 e
    ``cpu.cfs_quota_us`` sobre ``cpu.cfs_period_us`` no v1; ``None`` sem cota."""
    quotas = []
    for folder, version in _cgroup_folders("cpu"):
        if version == 2:
            # "max 100000" sem cota, "150000 100000" com uma cota de 1,5 CPU.
            fields = _read_fields(folder / "cpu.max")
            if len(fields) != 2 or fields[0] == "max":
                continue
            quota, period = int(fields[0]), int(fields[1])
        else:
            quota = _read_number(folder / "cpu.cfs_quota_us")
            period = _read_number(folder / "cpu.cfs_period_us")
            # O v1 escreve -1 sem cota.
            if quota is None or period is None or quota <= 0:
                continue
        quotas.append(quota / period)
    return min(quotas, default=None)


def _cgroup_folders(controller: str) -> list[tuple[Path, int]]:
    """As pastas do cgroup do processo para o controlador, com a versão, da dele até a raiz
    montada: ``/sys/fs/cgroup/<caminho>`` no v2 e ``/sys/fs/cgroup/<controlador>/<caminho>`` no v1,
    pelas linhas de ``/proc/self/cgroup``; vazia fora do Linux."""
    path = _PROC / "self" / "cgroup"
    if not path.is_file():
        return []
    folders = []
    for line in path.read_text().splitlines():
        # "0::/caminho" no v2; "4:memory:/caminho" e "2:cpu,cpuacct:/caminho" no v1.
        _, controllers, relative = line.split(":", 2)
        if controllers == "":
            mount, version = _CGROUP_ROOT, 2
        elif controller in controllers.split(","):
            mount, version = _CGROUP_ROOT / controller, 1
        else:
            continue
        for folder in _folder_chain(mount, relative):
            folders.append((folder, version))
    return folders


def _folder_chain(mount: Path, relative: str) -> list[Path]:
    """A pasta do cgroup sob a montagem e as ancestrais dela até a montagem; só a montagem quando a
    pasta não existe, no contêiner que monta o próprio cgroup como raiz."""
    folder = mount / relative.lstrip("/")
    if not folder.is_dir():
        return [mount]
    chain = [folder]
    while chain[-1] != mount:
        chain.append(chain[-1].parent)
    return chain


def _read_fields(path: Path) -> list[str]:
    """As palavras do arquivo; vazia quando ele não existe."""
    if not path.is_file():
        return []
    return path.read_text().split()


def _read_number(path: Path) -> int | None:
    """O número do arquivo; ``None`` quando ele não existe ou diz ``max``, sem limite."""
    fields = _read_fields(path)
    if not fields or fields[0] == "max":
        return None
    return int(fields[0])


def _stat_value(path: Path, key: str) -> int:
    """O valor de ``key`` num ``memory.stat``; 0 quando o arquivo ou a chave faltam."""
    if not path.is_file():
        return 0
    for line in path.read_text().splitlines():
        name, value = line.split()
        if name == key:
            return int(value)
    return 0
