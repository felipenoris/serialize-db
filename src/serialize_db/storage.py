"""Os dois armazenamentos da biblioteca, a pasta local e o S3, pelo sistema de arquivos do PyArrow.

``Storage`` guarda a raiz do banco: a URI que o delta-rs e o DuckDB recebem, o sistema de arquivos
do ``pyarrow.fs`` que lista, lê, copia e apaga nos dois armazenamentos, e o caminho da raiz nele.
Os métodos recebem caminhos relativos à raiz, montados com ``/`` por ``join``; ``relative`` leva
uma URI sob a raiz ao caminho relativo. Dois grupos de métodos têm um ramo por armazenamento: a
escrita condicional do arquivo de controle (``create_text`` e ``write_text``), ``put_object`` do
``boto3`` com ``IfNoneMatch`` ou ``IfMatch`` no S3, porque o ``pyarrow.fs`` não tem a condição nem
devolve a etag, e ``O_EXCL`` ou a impressão digital com ``os.replace`` na pasta local; e a cópia, a
transferência gerenciada do ``boto3`` no S3, porque o ``CopyObject`` único do ``copy_file`` do
PyArrow é abandonado pelo SDK da AWS depois de 3 segundos sem resposta num objeto grande.

``storage_options`` monta a cada chamada as opções do delta-rs, sem credencial alguma: a cadeia
padrão do delta-rs as resolve e as renova no ``DeltaTable`` que a execução segura.
``duckdb_connect`` abre uma conexão do DuckDB com as extensões da pasta configurada, e
``duckdb_setup`` carrega as extensões e cria o secret do S3 numa conexão já aberta, com a chave
da credencial do ``boto3`` naquele momento; o motor DuckDB recria o secret quando a chave troca.
``prepare_environment`` acerta as variáveis que o delta-rs lê antes da primeira abertura de tabela.

Exemplo, numa pasta local:

.. code-block:: python

    from serialize_db.storage import Storage

    storage = Storage.for_uri("/dados/delta")
    path = storage.join("prd", "_serialize_db", "snapshots.json")
    fingerprint = storage.create_text(path, "{}")
    text, fingerprint = storage.read_text(path)
    storage.write_text(path, '{"snapshots": {}}', if_match=fingerprint)
    storage.relative("/dados/delta/prd/cad_operacoes")   # "prd/cad_operacoes"
"""

from __future__ import annotations

import dataclasses
import hashlib
import os
import re
import sys
import tempfile
import urllib.parse
from collections.abc import Mapping, MutableMapping
from pathlib import Path

import boto3
import botocore.credentials
import botocore.exceptions
import duckdb
import pyarrow as pa
import pyarrow.fs as pafs

from serialize_db.errors import ConflictError
from serialize_db.schema import literal

__all__ = ["Storage", "prepare_environment"]

# O delta-rs tenta de novo por até 10 s: contra um endereço que não responde, a chamada desistiu em
# 10,3 s, contra 57,0 s no padrão, e contra uma porta fechada em 0,6 s (leitura de 2026-09-23); um
# erro passageiro do S3 ainda ganha até três novas tentativas dentro dos 10 s.
_RETRY_OPTIONS = {"max_retries": "3", "retry_timeout": "10s"}

# As variáveis de criptografia do object_store, o cliente de armazenamento do delta-rs, e a chave
# de storage_options de cada uma. Sem elas, o S3 aplica a criptografia padrão do bucket.
_SSE_VARIABLES = {
    "AWS_SERVER_SIDE_ENCRYPTION": "aws_server_side_encryption",
    "AWS_SSE_KMS_KEY_ID": "aws_sse_kms_key_id",
    "AWS_SSE_BUCKET_KEY_ENABLED": "aws_sse_bucket_key_enabled",
}

# O nome do secret do S3 que duckdb_setup cria e renew_duckdb_secret recria.
_DUCKDB_SECRET = "serialize_db_s3"


def _region() -> str | None:
    """A região de ``AWS_REGION`` ou, sem ela, de ``AWS_DEFAULT_REGION``."""
    return os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")


def _endpoint() -> str | None:
    """O endpoint de ``AWS_ENDPOINT_URL``, para um serviço compatível com o S3."""
    return os.environ.get("AWS_ENDPOINT_URL") or None


def _extension_directory() -> str | None:
    """A pasta de extensões do DuckDB: ``SERIALIZE_DB_DUCKDB_EXTENSIONS``; sem ela, ``.duckdb/``
    ao lado do ambiente virtual quando existe, a pasta que ``prepare_offline.sh`` cria; senão
    ``None``, a padrão do DuckDB."""
    configured = os.environ.get("SERIALIZE_DB_DUCKDB_EXTENSIONS")
    if configured:
        return configured
    prepared = Path(sys.prefix).parent / ".duckdb"
    return str(prepared) if prepared.is_dir() else None


def _proxy_settings(environ: Mapping[str, str]) -> dict[str, str]:
    """As configurações de proxy do DuckDB lidas de ``HTTP_PROXY``.

    O DuckDB recusa o endereço com as credenciais embutidas, e o erro atinge o acesso ao S3: o
    endereço vai sem elas em ``http_proxy``, e o usuário e a senha em ``http_proxy_username`` e
    ``http_proxy_password``, das variáveis ``username`` e ``password`` e, sem elas, do próprio
    endereço, como ``probelib.duckdb_proxy`` faz nos probes.
    """
    url = (environ.get("HTTP_PROXY") or "").strip()
    if not url:
        return {}
    # Sem "//", o urlsplit leria o endereço como caminho.
    if "//" not in url:
        url = "//" + url
    parts = urllib.parse.urlsplit(url, scheme="http")
    if not parts.hostname:
        return {}
    address = parts.hostname
    if parts.port:
        address += f":{parts.port}"
    settings = {"http_proxy": address}
    user = environ.get("username") or urllib.parse.unquote(parts.username or "")
    password = environ.get("password") or urllib.parse.unquote(parts.password or "")
    if user:
        settings["http_proxy_username"] = user
        settings["http_proxy_password"] = password
    return settings


def _duckdb_secret_options() -> list[str]:
    """As opções do secret S3 do DuckDB fora da credencial: a região e, com ``AWS_ENDPOINT_URL``,
    o endereço sem o esquema, o endereço por caminho e, num endpoint ``http``, ``USE_SSL false``.

    O DuckDB não lê ``AWS_ENDPOINT_URL``. Sem ``URL_STYLE 'path'`` o bucket vira subdomínio do
    endereço, que num IP não resolve, e sem ``USE_SSL false`` a conexão a um endpoint ``http``
    tenta TLS e falha (sonda de 2026-09-23 contra o moto, ``plan/POC.md``). O endereço por caminho
    é o que o delta-rs e o PyArrow usam com um endpoint próprio.
    """
    options = [f"REGION {literal(_region())}"]
    endpoint = _endpoint()
    if not endpoint:
        return options
    parts = urllib.parse.urlsplit(endpoint)
    options.append(f"ENDPOINT {literal(parts.netloc or endpoint)}")
    options.append("URL_STYLE 'path'")
    if parts.scheme == "http":
        options.append("USE_SSL false")
    return options


def _create_duckdb_secret(connection: duckdb.DuckDBPyConnection,
                          credentials: botocore.credentials.ReadOnlyCredentials) -> None:
    """Cria o secret do S3, ou o substitui, com a chave de ``credentials`` e as opções de
    ``_duckdb_secret_options``.

    A chave, o segredo e o token vão como parâmetros do comando, fora do texto, que o erro de
    sintaxe do DuckDB repete."""
    options = ["TYPE s3", "KEY_ID ?", "SECRET ?"]
    parameters = [credentials.access_key, credentials.secret_key]
    if credentials.token:
        options.append("SESSION_TOKEN ?")
        parameters.append(credentials.token)
    options.extend(_duckdb_secret_options())
    connection.execute(f"CREATE OR REPLACE SECRET {_DUCKDB_SECRET} ({', '.join(options)})",
                       parameters)


def _secret_key_id(connection: duckdb.DuckDBPyConnection) -> str | None:
    """A chave que o secret do S3 guarda, lida do ``secret_string`` de ``duckdb_secrets()``, que
    no DuckDB 1.5.5 mostra ``key_id`` sem redação; ``None`` sem o secret."""
    rows = connection.execute("SELECT secret_string FROM duckdb_secrets() WHERE name = ?",
                              [_DUCKDB_SECRET]).fetchall()
    if not rows:
        return None
    found = re.search(r"(?:^|;)key_id=([^;]*)", rows[0][0])
    if found is None:
        return None
    return found.group(1)


def aws_credentials() -> botocore.credentials.Credentials:
    """A credencial da cadeia padrão do ``boto3``, resolvida numa sessão nova; protegida, para o
    motor DuckDB, que a segura pela execução.

    A do contêiner é uma ``RefreshableCredentials``: o ``get_frozen_credentials`` dela pede outra
    ao endpoint quando faltam menos de 15 minutos para a expiração. A das variáveis ``AWS_*`` sem
    expiração é fixa.

    :return: a credencial da cadeia.
    :raises botocore.exceptions.NoCredentialsError: a cadeia não achou papel, variáveis ``AWS_*``
        nem perfil.
    """
    credentials = boto3.Session().get_credentials()
    if credentials is None:
        raise botocore.exceptions.NoCredentialsError()
    return credentials


def renew_duckdb_secret(connection: duckdb.DuckDBPyConnection,
                        credentials: botocore.credentials.Credentials) -> bool:
    """Recria o secret do S3 quando a chave guardada nele não é a que ``credentials`` dá agora;
    protegida, para o motor DuckDB, que a chama na entrada de cada sessão.

    O secret guarda a chave da criação, e o ``delta_scan`` não o renova: no ambiente alvo, o
    ``delta_scan`` falhou depois que a chave guardada expirou, com a credencial do ``boto3`` já em
    outra chave (2026-09-25, ``plan/POC.md``).

    :param connection: a conexão do DuckDB, com o ``httpfs`` carregado; o secret vale para a
        instância do banco, com todos os cursores dela.
    :param credentials: a credencial que o secret acompanha, a de ``aws_credentials``.
    :return: ``True`` quando o secret foi recriado.
    """
    frozen = credentials.get_frozen_credentials()
    if _secret_key_id(connection) == frozen.access_key:
        return False
    _create_duckdb_secret(connection, frozen)
    return True


def _fingerprint(content: bytes) -> str:
    """A impressão digital de um conteúdo na pasta local: o ``sha256`` dele, o papel da etag."""
    return hashlib.sha256(content).hexdigest()


@dataclasses.dataclass(frozen=True)
class Storage:
    """A raiz do banco num dos dois armazenamentos, com o sistema de arquivos que a percorre.

    Exemplo:

    .. code-block:: python

        storage = Storage.for_uri("s3://bucket/projeto/delta")
        storage.uri                      # "s3://bucket/projeto/delta"
        storage.path                     # "bucket/projeto/delta"
        storage.list_files("prd/cad_operacoes", ".parquet")
    """

    uri: str
    """A raiz como o delta-rs e o DuckDB a recebem, sem barra final: ``s3://bucket/prefixo`` ou o
    caminho absoluto da pasta local."""
    filesystem: pafs.FileSystem
    """``S3FileSystem`` ou ``LocalFileSystem``."""
    path: str
    """A raiz na forma do sistema de arquivos: ``bucket/prefixo`` ou o caminho absoluto."""

    @staticmethod
    def for_uri(uri: str) -> Storage:
        """O armazenamento de ``s3://bucket/prefixo``, de um caminho ou de ``file://``.

        No S3, o ``S3FileSystem`` leva a região de ``AWS_REGION`` ou ``AWS_DEFAULT_REGION``, a
        mesma que ``storage_options`` passa ao delta-rs, e o ``endpoint_override`` de
        ``AWS_ENDPOINT_URL``. Nada toca a rede na construção.

        Exemplo:

        .. code-block:: python

            Storage.for_uri("file:///dados/delta").uri   # "/dados/delta"

        :param uri: a raiz do banco; o caminho local é resolvido para absoluto.
        :return: o armazenamento da raiz.
        :raises ValueError: no S3 sem região, porque o PyArrow a buscaria na rede e o delta-rs
            cairia em ``us-east-1``; e em outro esquema.
        """
        if uri.startswith("s3://"):
            return _s3_storage(uri)
        if "://" in uri and not uri.startswith("file://"):
            raise ValueError(f"{uri}: esquema fora dos armazenamentos da biblioteca; "
                             "use uma pasta local, file:// ou s3://")
        return _local_storage(uri)

    @property
    def is_s3(self) -> bool:
        """Se a raiz está no S3."""
        return self.uri.startswith("s3://")

    # ------------------------------------------------------------ caminhos

    def join(self, *parts: str) -> str:
        """O caminho relativo à raiz com as partes juntadas por ``/``.

        Exemplo:

        .. code-block:: python

            storage.join("prd/", "cad_operacoes", "_delta_log")   # "prd/cad_operacoes/_delta_log"

        :param parts: os trechos do caminho; as barras nas pontas de cada um saem, e um trecho
            vazio é ignorado.
        :return: o caminho, sem barras nas pontas.
        """
        pieces = []
        for part in parts:
            stripped = part.strip("/")
            if stripped:
                pieces.append(stripped)
        return "/".join(pieces)

    def relative(self, uri: str) -> str:
        """O caminho relativo à raiz de uma URI sob ela.

        Exemplo:

        .. code-block:: python

            storage.relative(storage.uri + "/prd/cad_operacoes")   # "prd/cad_operacoes"

        :param uri: a URI sob a raiz, com ou sem barra final.
        :return: o caminho relativo; a própria raiz dá ``""``.
        :raises ValueError: a URI fora da raiz, porque as primitivas de ``serialize_db.delta``
            só tocam o que está sob a raiz do banco.
        """
        target = uri.rstrip("/")
        if target == self.uri:
            return ""
        if not target.startswith(self.uri + "/"):
            raise ValueError(f"{uri} fora da raiz {self.uri}")
        return target.removeprefix(self.uri + "/")

    def uri_of(self, path: str) -> str:
        """A URI de um caminho relativo à raiz, como o delta-rs e o DuckDB a recebem.

        Exemplo:

        .. code-block:: python

            storage.uri_of("prd/cad_operacoes")   # "s3://bucket/delta/prd/cad_operacoes"

        :param path: o caminho relativo à raiz; ``""`` é a própria raiz.
        :return: a URI, sem barra final.
        """
        return f"{self.uri}/{path}" if path else self.uri

    def _full(self, path: str) -> str:
        """O caminho no sistema de arquivos de um caminho relativo à raiz."""
        return f"{self.path}/{path}" if path else self.path

    def _local_parent(self, path: str) -> None:
        """Cria a pasta de um arquivo na pasta local; no S3 não há pasta a criar."""
        if not self.is_s3:
            Path(self._full(path)).parent.mkdir(parents=True, exist_ok=True)

    def ensure_folder(self, path: str) -> None:
        """Cria a pasta na pasta local, porque o ``COPY`` do DuckDB para um arquivo não cria a pasta
        dele; no S3 não há pasta a criar.

        Exemplo:

        .. code-block:: python

            storage.ensure_folder("prd/cad_operacoes/data_str=2026-08-31")

        :param path: a pasta, relativa à raiz; as pastas acima dela também são criadas.
        """
        if not self.is_s3:
            Path(self._full(path)).mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------ listagem, cópia e exclusão

    def exists(self, path: str) -> bool:
        """Se o arquivo ou a pasta existe.

        Exemplo:

        .. code-block:: python

            storage.exists("prd/cad_operacoes/_delta_log")   # True numa tabela criada

        :param path: o arquivo ou a pasta, relativo à raiz.
        :return: ``True`` quando existe.
        """
        info = self.filesystem.get_file_info(self._full(path))
        return info.type != pafs.FileType.NotFound

    def size(self, path: str) -> int | None:
        """O tamanho de um arquivo.

        Exemplo:

        .. code-block:: python

            storage.size("prd/cad_operacoes/data_str=2026-08-31/exec-42_ab12.parquet")
            # 4096

        :param path: o arquivo, relativo à raiz.
        :return: o tamanho em bytes, ou ``None`` quando ele não existe ou não é arquivo.
        """
        info = self.filesystem.get_file_info(self._full(path))
        if info.type != pafs.FileType.File:
            return None
        return info.size

    def list_files(self, prefix: str, suffix: str = "") -> list[str]:
        """Os arquivos sob ``prefix`` que terminam em ``suffix``.

        A listagem desce as subpastas e exclui ``_delta_log/``, onde os checkpoints também
        terminam em ``.parquet``.

        Exemplo:

        .. code-block:: python

            storage.list_files("prd/cad_operacoes", ".parquet")
            # ["prd/cad_operacoes/data_str=2026-08-31/exec-42_ab12.parquet", ...]

        :param prefix: a pasta, relativa à raiz; um prefixo ausente dá a lista vazia.
        :param suffix: o fim do nome, como ``.parquet``; vazio aceita todo arquivo.
        :return: os caminhos relativos à raiz, em ordem.
        """
        selector = pafs.FileSelector(self._full(prefix), recursive=True, allow_not_found=True)
        files = []
        for info in self.filesystem.get_file_info(selector):
            relative = info.path.removeprefix(self.path + "/")
            in_log = "_delta_log" in relative.split("/")
            if info.type == pafs.FileType.File and relative.endswith(suffix) and not in_log:
                files.append(relative)
        return sorted(files)

    def copy(self, source: str, destination: str) -> None:
        """Copia um arquivo sem passar os dados pelo Python.

        No S3, a transferência gerenciada do ``boto3``: um ``CopyObject`` até 8 MiB e, acima, um
        upload multipart de ``UploadPartCopy`` em partes de 8 MiB, em paralelo e com a repetição
        por parte do botocore. O ``copy_file`` do PyArrow é um ``CopyObject`` só, que o SDK da AWS
        abandona depois de 3 segundos sem byte de resposta (``curlCode: 28``), o que interrompeu o
        arquivo de 32.218.190 linhas de ``cad_lancamentos`` no ambiente alvo em 2026-09-24
        (``plan/POC.md``). Na pasta local, ``copy_file``, com a pasta do destino criada.

        Exemplo:

        .. code-block:: python

            storage.copy("prd/cad_operacoes/data_str=2026-08-31/exec-42_ab12.parquet",
                         "prd/arquivo/2026T3/cad_operacoes/data_str=2026-08-31/"
                         "exec-42_ab12.parquet")

        :param source: o arquivo de origem, relativo à raiz.
        :param destination: o caminho do arquivo copiado, relativo à raiz; um arquivo existente
            nele é substituído.
        """
        if self.is_s3:
            source_bucket, source_key = self._bucket_and_key(source)
            bucket, key = self._bucket_and_key(destination)
            self._s3_client().copy({"Bucket": source_bucket, "Key": source_key}, bucket, key)
            return
        self._local_parent(destination)
        self.filesystem.copy_file(self._full(source), self._full(destination))

    def delete(self, paths: list[str]) -> None:
        """Apaga os arquivos.

        Exemplo:

        .. code-block:: python

            storage.delete(storage.list_files("prd/staging/exec-42"))

        :param paths: os arquivos, relativos à raiz; um caminho ausente não é erro.
        """
        for path in paths:
            try:
                self.filesystem.delete_file(self._full(path))
            except FileNotFoundError:
                continue

    def open_input_file(self, path: str) -> pa.NativeFile:
        """O arquivo aberto para leitura aleatória, como ``pq.ParquetFile`` o recebe: no S3, o
        rodapé é lido por GET de intervalo.

        Exemplo:

        .. code-block:: python

            with storage.open_input_file(path) as source:
                rows = pq.ParquetFile(source).metadata.num_rows

        :param path: o arquivo, relativo à raiz.
        :return: o arquivo aberto do ``pyarrow.fs``, que quem chama fecha.
        """
        return self.filesystem.open_input_file(self._full(path))

    def open_output_stream(self, path: str) -> pa.NativeFile:
        """O arquivo aberto para escrita sequencial, como ``pq.ParquetWriter`` o recebe: no S3, um
        upload em partes que termina no ``close``.

        Exemplo:

        .. code-block:: python

            with storage.open_output_stream("prd/staging/exec-42/lote.parquet") as sink:
                pq.write_table(table, sink)

        :param path: o arquivo, relativo à raiz; na pasta local, a pasta do arquivo é criada.
        :return: o fluxo de saída do ``pyarrow.fs``, que quem chama fecha.
        """
        self._local_parent(path)
        return self.filesystem.open_output_stream(self._full(path))

    # ------------------------------------------------------------ a escrita condicional

    def read_text(self, path: str) -> tuple[str, str]:
        """O texto do arquivo e a impressão digital dele, a que ``write_text`` recebe em
        ``if_match``.

        Exemplo:

        .. code-block:: python

            text, fingerprint = storage.read_text("prd/_serialize_db/snapshots.json")

        :param path: o arquivo, relativo à raiz, em UTF-8.
        :return: o texto e a impressão digital: a etag no S3, o ``sha256`` na pasta local.
        :raises FileNotFoundError: o arquivo ausente, nos dois armazenamentos.
        """
        if self.is_s3:
            return self._read_s3(path)
        full = Path(self._full(path))
        content = full.read_bytes()
        return content.decode("utf-8"), _fingerprint(content)

    def create_text(self, path: str, text: str) -> str:
        """Grava o texto num arquivo que ainda não existe.

        No S3, ``put_object`` com ``IfNoneMatch="*"``, atômico no servidor; na pasta local, a
        abertura com ``O_EXCL``.

        Exemplo:

        .. code-block:: python

            path = "prd/_serialize_db/snapshots.json"
            first = storage.create_text(path, "{}")
            storage.create_text(path, "{}")   # ConflictError: o arquivo já existe

        :param path: o arquivo, relativo à raiz; na pasta local, a pasta dele é criada.
        :param text: o conteúdo, gravado em UTF-8.
        :return: a impressão digital do arquivo gravado.
        :raises ConflictError: o arquivo já existe; nada foi gravado.
        """
        if self.is_s3:
            return self._put_s3(path, text, {"IfNoneMatch": "*"})
        return self._create_local(path, text)

    def write_text(self, path: str, text: str, if_match: str | None = None) -> str:
        """Grava o texto por inteiro, substituindo o arquivo, com a condição opcional de ele não ter
        mudado desde a leitura.

        No S3, ``put_object``, com ``IfMatch=<etag>`` quando a condição vem, atômico no servidor.
        Na pasta local, a comparação da impressão digital seguida de ``os.replace`` de um arquivo
        temporário, que não é atômica entre processos e basta à pasta local, o ambiente dos testes
        e do desenvolvimento.

        Exemplo:

        .. code-block:: python

            text, fingerprint = storage.read_text(path)
            storage.write_text(path, '{"snapshots": {}}', if_match=fingerprint)

        :param path: o arquivo, relativo à raiz; na pasta local, a pasta dele é criada.
        :param text: o conteúdo, gravado em UTF-8.
        :param if_match: grava só se a impressão digital atual é a informada, a que
            ``read_text`` devolveu; ``None`` grava sem condição.
        :return: a impressão digital nova.
        :raises ConflictError: a condição falhou; nada foi gravado.
        """
        if not self.is_s3:
            return self._replace_local(path, text, if_match)
        condition = {}
        if if_match is not None:
            condition["IfMatch"] = if_match
        return self._put_s3(path, text, condition)

    def _create_local(self, path: str, text: str) -> str:
        """A criação na pasta local, pela abertura exclusiva ``O_EXCL``."""
        full = Path(self._full(path))
        full.parent.mkdir(parents=True, exist_ok=True)
        content = text.encode("utf-8")
        try:
            descriptor = os.open(full, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
        except FileExistsError:
            raise ConflictError(f"{path} já existe") from None
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
        return _fingerprint(content)

    def _replace_local(self, path: str, text: str, if_match: str | None) -> str:
        """A substituição na pasta local, com a impressão digital conferida quando ``if_match``
        vem."""
        full = Path(self._full(path))
        full.parent.mkdir(parents=True, exist_ok=True)
        if if_match is not None:
            current = _fingerprint(full.read_bytes()) if full.exists() else None
            if current != if_match:
                raise ConflictError(f"{path} mudou desde a leitura")
        content = text.encode("utf-8")
        # O arquivo temporário na mesma pasta: os.replace troca de uma vez.
        with tempfile.NamedTemporaryFile("wb", dir=full.parent, delete=False) as handle:
            handle.write(content)
            temporary = handle.name
        os.replace(temporary, full)
        return _fingerprint(content)

    def _bucket_and_key(self, path: str) -> tuple[str, str]:
        """O bucket e a chave de um caminho relativo à raiz no S3."""
        bucket, _, key = self._full(path).partition("/")
        return bucket, key

    def _s3_client(self) -> object:
        """O cliente S3 do ``boto3`` com a região e o endpoint do ambiente: o botocore não lê
        ``AWS_REGION`` e, sem região, iria ao endpoint global."""
        return boto3.client("s3", region_name=_region(), endpoint_url=_endpoint())

    def _read_s3(self, path: str) -> tuple[str, str]:
        """O texto e a etag de um objeto do S3."""
        bucket, key = self._bucket_and_key(path)
        try:
            response = self._s3_client().get_object(Bucket=bucket, Key=key)
        except botocore.exceptions.ClientError as error:
            if error.response["Error"]["Code"] in ("NoSuchKey", "404"):
                raise FileNotFoundError(self.uri_of(path)) from None
            raise
        return response["Body"].read().decode("utf-8"), response["ETag"]

    def _put_s3(self, path: str, text: str, condition: Mapping[str, str]) -> str:
        """O ``put_object`` com a condição, ``IfNoneMatch`` ou ``IfMatch``; o 412 vira
        ``ConflictError``."""
        bucket, key = self._bucket_and_key(path)
        try:
            response = self._s3_client().put_object(
                Bucket=bucket, Key=key, Body=text.encode("utf-8"), **condition)
        except botocore.exceptions.ClientError as error:
            if error.response["Error"]["Code"] in ("PreconditionFailed", "412"):
                raise ConflictError(f"{self.uri_of(path)}: {error}") from None
            raise
        return response["ETag"]

    # ------------------------------------------------------------ delta-rs e DuckDB

    def storage_options(self) -> dict[str, str]:
        """As opções do delta-rs, montadas a cada chamada.

        Nenhuma credencial: a cadeia padrão do delta-rs as resolve e as renova no ``DeltaTable``
        que a execução segura, enquanto um trio congelado expiraria em cerca de uma hora e
        circularia num dicionário que um log imprime.

        Exemplo:

        .. code-block:: python

            Storage.for_uri("s3://bucket/delta").storage_options()
            # {"AWS_REGION": "us-east-1", "max_retries": "3", "retry_timeout": "10s"}

        :return: vazias na pasta local. No S3: ``AWS_REGION``, ``AWS_ENDPOINT_URL`` quando
            presente, ``max_retries`` e ``retry_timeout`` para uma rede morta falhar em segundos,
            e as chaves de SSE das variáveis do object_store quando configuradas.
        """
        if not self.is_s3:
            return {}
        options = {"AWS_REGION": _region()}
        endpoint = _endpoint()
        if endpoint:
            options["AWS_ENDPOINT_URL"] = endpoint
        options.update(_RETRY_OPTIONS)
        for variable, key in _SSE_VARIABLES.items():
            if os.environ.get(variable):
                options[key] = os.environ[variable]
        return options

    def duckdb_setup(self, connection: duckdb.DuckDBPyConnection) -> None:
        """Carrega as extensões que a raiz pede e, no S3, cria o secret com a chave da credencial
        do ``boto3``.

        Na pasta local, só ``LOAD delta``. No S3, ``LOAD httpfs`` e ``LOAD delta``, o secret
        ``serialize_db_s3`` com a chave, o segredo e o token que a cadeia do ``boto3`` resolve
        agora, a região e o endpoint, e o proxy de ``HTTP_PROXY`` sem as credenciais no endereço.
        O secret guarda essa chave até ser recriado: uma conexão que dura mais que ela precisa
        recriá-lo, como o motor DuckDB faz. As extensões vêm da pasta configurada na conexão; nada
        é baixado.

        Exemplo:

        .. code-block:: python

            connection = duckdb.connect(config={"extension_directory": ".duckdb"})
            storage.duckdb_setup(connection)
            connection.execute(f"SELECT count(*) FROM delta_scan('{uri}')")

        :param connection: a conexão aberta do DuckDB que recebe as extensões e o secret.
        :raises botocore.exceptions.NoCredentialsError: no S3, a cadeia do ``boto3`` não achou
            credencial.
        """
        if not self.is_s3:
            connection.execute("LOAD delta")
            return
        for extension in ("httpfs", "delta"):
            connection.execute(f"LOAD {extension}")
        for name, value in _proxy_settings(os.environ).items():
            connection.execute(f"SET {name} = {literal(value)}")
        _create_duckdb_secret(connection, aws_credentials().get_frozen_credentials())

    def duckdb_connect(self, database: str = ":memory:",
                       config: Mapping[str, object] | None = None) -> duckdb.DuckDBPyConnection:
        """Uma conexão do DuckDB com as extensões da pasta configurada, sem instalação automática,
        e ``duckdb_setup`` aplicado.

        A instalação e a carga automáticas ficam desligadas: o ``LOAD`` de uma extensão conhecida
        a baixaria para ``~/.duckdb`` sem aviso, e o ambiente alvo não tem internet.

        Exemplo:

        .. code-block:: python

            connection = storage.duckdb_connect()
            uri = storage.uri_of("prd/cad_operacoes")
            connection.execute(f"SELECT count(*) FROM delta_scan('{uri}')")

        :param database: o arquivo do banco do DuckDB; o padrão, um banco em memória, é
            ``:memory:``.
        :param config: as opções da abertura (``threads``, ``temp_directory``,
            ``memory_limit``), acrescentadas às da biblioteca.
        :return: a conexão aberta, que quem chama fecha.
        :raises duckdb.Error: uma extensão ausente da pasta configurada ou o secret recusado; a
            conexão é fechada antes.
        :raises botocore.exceptions.NoCredentialsError: no S3, a cadeia do ``boto3`` não achou
            credencial; a conexão é fechada antes.
        """
        settings: dict[str, object] = {
            "autoinstall_known_extensions": False,
            "autoload_known_extensions": False,
        }
        directory = _extension_directory()
        if directory:
            settings["extension_directory"] = directory
        settings.update(config or {})
        connection = duckdb.connect(database, config=settings)
        try:
            self.duckdb_setup(connection)
        except (duckdb.Error, botocore.exceptions.BotoCoreError):
            connection.close()
            raise
        return connection


def _s3_storage(uri: str) -> Storage:
    """O armazenamento no S3, com a região e o endpoint das variáveis."""
    region = _region()
    if not region:
        raise ValueError(f"{uri}: o S3 precisa da região em AWS_REGION ou AWS_DEFAULT_REGION")
    path = uri.removeprefix("s3://").strip("/")
    options: dict[str, object] = {"region": region}
    endpoint = _endpoint()
    if endpoint:
        options["endpoint_override"] = endpoint
    return Storage(f"s3://{path}", pafs.S3FileSystem(**options), path)


def _local_storage(uri: str) -> Storage:
    """O armazenamento numa pasta local, com o caminho absoluto resolvido."""
    local_path = urllib.parse.urlsplit(uri).path if uri.startswith("file://") else uri
    resolved = str(Path(local_path).expanduser().resolve())
    return Storage(resolved, pafs.LocalFileSystem(), resolved)


def prepare_environment(environ: MutableMapping[str, str] = os.environ) -> dict[str, str]:
    """Acerta as variáveis que o delta-rs lê.

    Exporta ``NO_PROXY`` a partir de ``no_proxy`` quando a maiúscula está ausente ou vazia: o
    cliente HTTP do delta-rs lê ``NO_PROXY`` antes de ``no_proxy``, e vazia ela anula as exceções e
    manda ao proxy a chamada ao endpoint de credenciais do contêiner. Copia a região entre
    ``AWS_REGION`` e ``AWS_DEFAULT_REGION`` nos dois sentidos, porque o botocore lê só a segunda e
    o delta-rs as duas. A segunda chamada não muda nada.

    Exemplo:

    .. code-block:: python

        prepare_environment()   # {"NO_PROXY": "169.254.170.2,localhost"} na primeira chamada

    :param environ: as variáveis, alteradas no lugar; o padrão é ``os.environ``, o que o
        delta-rs lê.
    :return: as variáveis que mudaram, com o valor novo, para o log.
    """
    changes: dict[str, str] = {}
    # Vazia conta como ausente: get devolve "" e a condição a substitui.
    if not environ.get("NO_PROXY") and environ.get("no_proxy"):
        environ["NO_PROXY"] = environ["no_proxy"]
        changes["NO_PROXY"] = environ["no_proxy"]
    region = environ.get("AWS_REGION") or environ.get("AWS_DEFAULT_REGION")
    for name in ("AWS_REGION", "AWS_DEFAULT_REGION"):
        if region and environ.get(name) != region:
            environ[name] = region
            changes[name] = region
    return changes
