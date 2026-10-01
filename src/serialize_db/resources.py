"""As CPUs e a memória que o processo pode usar, lidas do ambiente a cada chamada.

O pacote roda em máquinas de tamanhos diferentes, e os limites do DuckDB saem destas leituras, não
de um valor fixo no código: ``environment_limits``, que ``serialize_db.engine.duckdb`` publica, os
monta para toda conexão do DuckDB do pacote, a do motor e as de ``serialize_db.delta``. No Linux, as
leituras respeitam o cgroup do processo, v1 e v2, com que um contêiner limita as CPUs e a memória
abaixo das da máquina, e o menor limite no caminho do cgroup até a raiz é o que vale. No Windows,
a memória é a disponível que a API do sistema informa, lida pelo ``ctypes``; nos outros sistemas
fora do Linux, é a física. Fora do Linux, as CPUs são as que o Python lê. ``peak_rss_mb`` lê o
pico de memória residente do próprio processo, a medida que o script de migração, os subcomandos
de operação e a publicação imprimem por tabela.

Exemplo:

.. code-block:: python

    from serialize_db.resources import available_cpus, available_memory, peak_rss_mb

    print(available_cpus(), f"{available_memory() / 2**30:.1f} GiB", f"{peak_rss_mb():.0f} MB")
"""

from __future__ import annotations

import ctypes
import math
import os
import re
import sys
from pathlib import Path

# O módulo resource só existe nos Unix; no Windows, peak_rss_mb lê o pico pela API do sistema.
if sys.platform != "win32":
    import resource

__all__ = ["available_cpus", "available_memory", "peak_rss_mb"]

# As raízes lidas; os testes as trocam por pastas fabricadas.
_PROC = Path("/proc")
_CGROUP_ROOT = Path("/sys/fs/cgroup")

# Por versão do cgroup: o arquivo do limite de memória, o do uso, e as chaves de memory.stat do
# cache de arquivos e da memória compartilhada (tmpfs, /dev/shm, mmap compartilhado) contada dentro
# dele. O kernel devolve o cache antes de matar um processo, menos a memória compartilhada, que ele
# só devolve gravando-a no swap.
_MEMORY_FILES = {
    1: ("memory.limit_in_bytes", "memory.usage_in_bytes", "total_cache", "total_shmem"),
    2: ("memory.max", "memory.current", "file", "shmem"),
}

# A fração da memória disponível que vai para o memory_limit. A documentação do DuckDB pede de 50%
# a 60% da memória quando o sistema mata o processo, porque parte das alocações foge do limite: no
# COPY ordenado, o RSS do processo passou do limite em 13% a 21% (2026-09-24). A outra metade fica
# para o PyArrow, o delta-rs e o código do cliente.
_MEMORY_FRACTION = 0.5


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

    :return: em bytes, a menor entre a física, a disponível no sistema (no Windows,
        ``ullAvailPhys`` de ``GlobalMemoryStatusEx``, que conta como livre a lista de espera, as
        páginas em cache que o sistema reaproveita; no Linux, ``MemAvailable`` de
        ``/proc/meminfo``, que conta como livre o cache de arquivos que o kernel devolve) e a
        folga do cgroup (o limite menos o uso, com o cache de arquivos de volta, menos a memória
        compartilhada, ``shmem``, que o kernel conta no cache e não devolve sem swap).
    """
    readings = [physical_memory()]
    if sys.platform == "win32":
        readings.append(_windows_memory_status().ullAvailPhys)
    system = _meminfo_available()
    if system is not None:
        readings.append(system)
    room = _cgroup_memory_room()
    if room is not None:
        readings.append(room)
    return min(readings)


def physical_memory() -> int:
    """A memória física da máquina, em bytes: ``ullTotalPhys`` de ``GlobalMemoryStatusEx`` no
    Windows e ``os.sysconf`` nos outros sistemas; protegida, para o relatório de
    ``scripts/migrate_parquet_to_delta.py``."""
    if sys.platform == "win32":
        return _windows_memory_status().ullTotalPhys
    return os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")


def environment_limits() -> dict[str, object]:
    """O ``threads`` e o ``memory_limit`` do DuckDB lidos do ambiente na chamada. O motor os aplica
    na abertura quando a configuração os omite, e ``serialize_db.delta`` em cada conexão sua.

    Exemplo:

    .. code-block:: python

        # Num contêiner com 4 CPUs e 13,2 GiB disponíveis:
        environment_limits()   # {'threads': 4, 'memory_limit': '6761MiB'}
        duckdb.connect(config=environment_limits())

    :return: as opções da conexão do DuckDB: em ``threads``, as CPUs que o processo pode usar;
        em ``memory_limit``, metade da memória que ele ainda pode usar, em MiB.
    """
    memory_limit = int(available_memory() * _MEMORY_FRACTION)
    return {"threads": available_cpus(), "memory_limit": f"{memory_limit // 2**20}MiB"}


def peak_rss_mb() -> float:
    """O pico de memória residente do próprio processo até agora. Num processo filho, a leitura é
    a do filho, não a do pai.

    Exemplo:

    .. code-block:: python

        peak_rss_mb()   # 297.2 no script de migração, depois das importações

    :return: o pico em MB. No Linux, lido do ``VmHWM`` de ``/proc/self/status``, em KB; no
        Windows, do pico do conjunto de trabalho (``PeakWorkingSetSize``) de
        ``K32GetProcessMemoryInfo``, em bytes; nos outros, do ``ru_maxrss`` do processo, em bytes
        no macOS e em KB nos outros Unix.
    """
    status = _PROC / "self" / "status"
    if status.exists():
        found = re.search(r"^VmHWM:\s+(\d+)", status.read_text(), re.MULTILINE)
        return int(found.group(1)) / 1024
    if sys.platform == "win32":
        return _windows_peak_working_set() / 2**20
    maxrss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    unit = 1024 * 1024 if sys.platform == "darwin" else 1024
    return maxrss / unit


class _MemoryStatus(ctypes.Structure):
    """O ``MEMORYSTATUSEX`` que ``GlobalMemoryStatusEx`` preenche no Windows."""

    _fields_ = [
        ("dwLength", ctypes.c_ulong),
        ("dwMemoryLoad", ctypes.c_ulong),
        ("ullTotalPhys", ctypes.c_ulonglong),
        ("ullAvailPhys", ctypes.c_ulonglong),
        ("ullTotalPageFile", ctypes.c_ulonglong),
        ("ullAvailPageFile", ctypes.c_ulonglong),
        ("ullTotalVirtual", ctypes.c_ulonglong),
        ("ullAvailVirtual", ctypes.c_ulonglong),
        ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
    ]


class _ProcessMemoryCounters(ctypes.Structure):
    """O ``PROCESS_MEMORY_COUNTERS`` que ``K32GetProcessMemoryInfo`` preenche no Windows."""

    _fields_ = [
        ("cb", ctypes.c_ulong),
        ("PageFaultCount", ctypes.c_ulong),
        ("PeakWorkingSetSize", ctypes.c_size_t),
        ("WorkingSetSize", ctypes.c_size_t),
        ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
        ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
        ("PagefileUsage", ctypes.c_size_t),
        ("PeakPagefileUsage", ctypes.c_size_t),
    ]


def _windows_memory_status() -> _MemoryStatus:
    """A memória física do Windows, a total e a disponível, de ``GlobalMemoryStatusEx``."""
    status = _MemoryStatus()
    status.dwLength = ctypes.sizeof(status)
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    if not kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
        raise ctypes.WinError(ctypes.get_last_error())
    return status


def _windows_peak_working_set() -> int:
    """O pico do conjunto de trabalho do processo no Windows, a memória residente dele, em bytes,
    de ``K32GetProcessMemoryInfo``."""
    counters = _ProcessMemoryCounters()
    counters.cb = ctypes.sizeof(counters)
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    # O GetCurrentProcess devolve o pseudo-handle do processo, um ponteiro, que o tipo de retorno
    # padrão do ctypes, int de 32 bits, cortaria.
    kernel32.GetCurrentProcess.restype = ctypes.c_void_p
    process = ctypes.c_void_p(kernel32.GetCurrentProcess())
    if not kernel32.K32GetProcessMemoryInfo(process, ctypes.byref(counters), counters.cb):
        raise ctypes.WinError(ctypes.get_last_error())
    return counters.PeakWorkingSetSize


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
    cache de arquivos de volta, menos a memória compartilhada contada nele; ``None`` quando
    nenhuma pasta dá limite e uso."""
    rooms = []
    for folder, version in _cgroup_folders("memory"):
        limit_file, usage_file, cache_key, shared_key = _MEMORY_FILES[version]
        limit = _read_number(folder / limit_file)
        usage = _read_number(folder / usage_file)
        if limit is None or usage is None:
            continue
        cache = _stat_value(folder / "memory.stat", cache_key)
        shared = _stat_value(folder / "memory.stat", shared_key)
        rooms.append(limit - usage + cache - shared)
    return min(rooms, default=None)


def _cgroup_cpu_quota() -> float | None:
    """A menor cota de CPU nos cgroups do processo, em CPUs; ``None`` sem cota."""
    quotas = []
    for folder, version in _cgroup_folders("cpu"):
        if version == 2:
            quota = _cpu_quota_v2(folder)
        else:
            quota = _cpu_quota_v1(folder)
        if quota is not None:
            quotas.append(quota)
    return min(quotas, default=None)


def _cpu_quota_v2(folder: Path) -> float | None:
    """A cota de ``cpu.max`` no cgroup v2, em CPUs: ``"max 100000"`` sem cota e
    ``"150000 100000"`` com uma cota de 1,5 CPU; ``None`` sem cota."""
    fields = _read_fields(folder / "cpu.max")
    if len(fields) != 2 or fields[0] == "max":
        return None
    return int(fields[0]) / int(fields[1])


def _cpu_quota_v1(folder: Path) -> float | None:
    """A cota do cgroup v1, ``cpu.cfs_quota_us`` sobre ``cpu.cfs_period_us``, em CPUs; ``None``
    sem cota, que o v1 escreve como -1."""
    quota = _read_number(folder / "cpu.cfs_quota_us")
    period = _read_number(folder / "cpu.cfs_period_us")
    if quota is None or period is None or quota <= 0:
        return None
    return quota / period


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
