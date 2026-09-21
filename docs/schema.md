# Esquema a partir dos modelos ORM

O esquema das tabelas vive nos modelos ORM, e o módulo `serialize_db.schema`, etapa 1 do plano
([`PLAN-STAGE-1.md`](PLAN-STAGE-1.md)), deriva deles o esquema Arrow, o esquema Delta e o DDL de
cada motor. Gerar o DDL a partir dos modelos. O DDL gerado cobre as tabelas do sandbox no DuckDB e no
Redshift. As vantagens do DDL manual, revisão explícita e opções físicas, vêm de
dois mecanismos:

- Opções físicas no próprio modelo, num espaço de nomes da biblioteca em `Table.info`. A chave de
  ordenação vira `SORTKEY` no Redshift e `ORDER BY` na gravação dos arquivos.
- Arquivos `.sql` com o DDL gerado por backend, versionados no
  repositório do pipeline e comparados por um teste. Uma mudança no modelo aparece no diff.

```python
class Operacao(Base):
    __tablename__ = "operacoes"
    __table_args__ = {
        "info": {
            "serialize_db": {
                "partition_by": ["data_ref_str"],
                "partition_source": "data_ref",
                "sort_key": ["data_ref", "id_operacao"],
                "redshift": {"diststyle": "KEY", "distkey": "id_cliente"},
            }
        }
    }

    id_operacao: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    data_ref: Mapped[date] = mapped_column(primary_key=True)
    id_cliente: Mapped[int] = mapped_column(BigInteger)
    valor: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    descricao: Mapped[str | None] = mapped_column(String(200))
    data_ref_str: Mapped[str] = mapped_column(String(10))
```

A coluna de partição é uma data em texto `AAAA-MM-DD`, `strftime(partition_source, '%Y-%m-%d')`,
nos moldes da base de referência (decisão de 2026-09-20); a biblioteca não fixa o nome nem a
granularidade, e `mes` nos exemplos dos outros documentos é uma coluna de partição ilustrativa.

O `sqlalchemy-redshift` aceita `redshift_diststyle`, `redshift_distkey`, `redshift_sortkey` e
`redshift_interleaved_sortkey` como argumentos de `Table`. Guardar as opções em `info` mantém os
modelos neutros quando o dialeto do Redshift não está instalado.

## Política de restrições

| Restrição               | Sandbox DuckDB | Sandbox Redshift |
| ----------------------- | -------------- | ---------------- |
| `NOT NULL`              | Declarada. | Declarada e aplicada pelo banco. |
| `PRIMARY KEY`, `UNIQUE` | Omitida; a auditoria confere a chave do modelo. | Declarada quando auditada; informativa.|
| `FOREIGN KEY` | Omitida; a auditoria confere com `foreign_keys=True`. | Declarada quando auditada; informativa. |

Unicidade, chave primária e chave estrangeira são informativas no Redshift. O planejador usa essas
chaves para decorrelacionar subconsultas, ordenar e eliminar joins, e supõe que elas são válidas. Com
chaves inválidas, consultas retornam resultados errados; a documentação cita um `SELECT DISTINCT` que
devolve duplicatas. `NOT NULL` é aplicado.

A auditoria da execução é onde as chaves são aplicadas, e as consultas saem do próprio modelo:
`table.primary_key`, os `UniqueConstraint` e os `ForeignKey`, sem uma segunda declaração. A chave
`keys` de `Table.info["serialize_db"]` só acrescenta uma chave de negócio ou exclui uma existente.
Uma chave cujas colunas não incluem a coluna de partição é conferida na tabela inteira, não só nas
partições da execução, porque unicidade dentro da partição não é unicidade. A chave estrangeira é conferida
sob pedido, porque a tabela referenciada pode não estar no sandbox: só o que o pipeline usa é
ingerido. O texto SQL de cada verificação é gerado por dialeto e pode ser impresso ou gravado, para
depurar o comando e para o diff. As primitivas estão em `PLAN-STAGE-4.md`.

A medição publicada na documentação do DuckDB, com 554 milhões de linhas, justifica omitir chaves no
DuckDB:

| Operação | Tempo |
| --- | --- |
| Carga com chave primária | 461,6 s |
| Carga sem chave primária | 121,0 s |
| Criação da chave primária após a carga | 242,0 s |

A documentação recomenda não declarar restrições no DuckDB, exceto para garantir integridade. Índices
ART precisam caber em memória durante a criação.

## Tipos no contrato

| SQLAlchemy | Arrow | Delta | DuckDB | Redshift | Observação |
| --- | --- | --- | --- | --- | --- |
| `SmallInteger` | `int16` | `short` | `SMALLINT` | `SMALLINT` | O Parquet grava `int16` no tipo físico `INT32`. |
| `Integer` | `int32` | `integer` | `INTEGER` | `INTEGER` | |
| `BigInteger` | `int64` | `long` | `BIGINT` | `BIGINT` | Tipos sem sinal do Arrow e do DuckDB ficam fora do contrato. |
| `Boolean` | `bool` | `boolean` | `BOOLEAN` | `BOOLEAN` | |
| `Double` | `float64` | `double` | `DOUBLE` | `DOUBLE PRECISION` | Descarregar e recarregar pelo Redshift pode perder precisão. O modelo de referência usa `Double` em toda coluna numérica (decisão de 2026-09-20); `Numeric(18, 2)` nas colunas contábeis é a melhoria futura. |
| `Numeric(p, s)` | `decimal128(p, s)` | `decimal(p, s)` | `DECIMAL(p, s)` | `DECIMAL(p, s)` | `p` até 38. O delta-rs e o DuckDB gravam `DECIMAL(18, 2)` no tipo físico `INT64`; o PyArrow, em `FIXED_LEN_BYTE_ARRAY`. |
| `String(n)` | `string` | `string` | `VARCHAR` | `VARCHAR(n)` | `n` em bytes no Redshift; auditoria de tamanho. |
| `Text` | `string` | `string` | `VARCHAR` | `VARCHAR(65535)` | `TEXT` no Redshift vira `VARCHAR(256)`. |
| `Date` | `date32` | `date` | `DATE` | `DATE` | |
| `DateTime` | `timestamp[us]` | `timestamp_ntz` | `TIMESTAMP` | `TIMESTAMP` | O `INT96` da base de origem, obsoleto no formato Parquet, vira `INT64` de microssegundos na carga inicial. Cast explícito de nanossegundos (padrão do pandas) para microssegundos; o delta-rs aceita nanossegundos e grava microssegundos em silêncio. `timestamp_ntz` exige o recurso `timestampNtz` (leitor 3, escritor 7), que o delta-rs habilita ao gravar e o DuckDB lê como `TIMESTAMP`. O `UNLOAD` do Redshift traz o `INT96` de volta na exportação, e um arquivo desses registrado na tabela `timestamp_ntz` é lido como `timestamp[us]` pelos dois leitores, com os valores intactos e sem estatística de mínimo e máximo (2026-09-21, [POC.md](POC.md)). |
| `DateTime(timezone=True)` | `timestamp[us, tz=UTC]` | `timestamp` | `TIMESTAMPTZ` | `TIMESTAMPTZ` | Gravar sempre em UTC; o `timestamp` do Delta é ajustado a UTC, e um fuso diferente entra como o mesmo instante. O `UNLOAD` descarta o fuso. |
| `Uuid` | `string` | `string` | `VARCHAR` | `VARCHAR(36)` | O Redshift não tem tipo UUID. |
| `JSON().with_variant(SUPER(), "redshift")` | `string` | `string` | `JSON` | `SUPER` | Texto JSON é a forma de troca, sem a extensão `arrow.json`; a validação é do DuckDB na carga e do `JSON_PARSE` no Redshift. Detalhes na seção seguinte. |
| `LargeBinary`, `ARRAY`, `Interval` | | | | | Fora do contrato até haver um caso de uso. |

### Campos JSON

Um campo JSON guarda um documento sem esquema fixo; chaves com esquema fixo viram colunas. O
tratamento em cada camada, verificado em 2026-09-19:

| Camada | Tipo | Comportamento |
| --- | --- | --- |
| Modelo | `sa.JSON().with_variant(SUPER(), "redshift")`, com `SUPER` de `sqlalchemy_redshift.dialect`. | O DDL compila `JSON` no DuckDB e `SUPER` no Redshift; `isinstance(sa_type, sa.JSON)` continua verdadeiro, e `arrow_type` o reconhece. |
| Arrow | `pa.string()`. A extensão `arrow.json` (`pa.json_(pa.string())`) fica fora do contrato (decisão de 2026-09-20): o dtype dela no pandas com backend pyarrow não tem os kernels de `.str`, e nenhum motor a devolve. | Um `dict` do pandas vira `struct` com a união das chaves; o cliente serializa com `json.dumps` antes de montar a tabela, e `cast` recusa `struct`, `list` e `map`. O Arrow não valida o texto. |
| Delta | `string`, com `ARROW:extension:name = arrow.json` nos metadados do campo quando o esquema Arrow traz a extensão. | O delta-rs grava o arquivo com o tipo lógico `String`; `schema().to_arrow()` devolve `string` simples. |
| Parquet | `BYTE_ARRAY` com tipo lógico `JSON` quando gravado pelo PyArrow ou pelo DuckDB, `String` quando gravado pelo delta-rs. | Os dois entram na mesma tabela Delta e são lidos pelos dois leitores. |
| DuckDB | `JSON` nas tabelas do sandbox; `VARCHAR` no `delta_scan`. | `::JSON` valida na materialização (`Malformed JSON` para texto inválido); `->>`, `json_extract` e `json_valid` funcionam sobre `VARCHAR`; a saída em Arrow volta como `string`. |
| Redshift | `SUPER`. | A staging recebe `VARCHAR(65535)` e o `INSERT ... SELECT` aplica `JSON_PARSE`; o `UNLOAD` devolve texto com `JSON_SERIALIZE`. Documentos acima de 65.535 bytes dependem do `COPY` direto em `SUPER`, pendente da prova de conceito. |

A auditoria confere `json_valid` no DuckDB antes de publicar, porque nem o Arrow nem o Delta validam
o texto.

## Portabilidade de SQL entre DuckDB e Redshift

- As consultas nascem como construções do SQLAlchemy, e o texto SQL gerado por dialeto substitui,
  uma interação por vez, a compilação pelo dialeto em tempo de execução
  ([`sqlalchemy.md`](sqlalchemy.md)).
- Funções com nomes ou semânticas diferentes nos dois bancos ganham uma regra `@compiles` por dialeto.
  A lista sai do código atual do pipeline.
- O DuckDB aceita construções do PostgreSQL ausentes no Redshift, como arrays e `ON CONFLICT`. Uma
  consulta que roda no DuckDB pode falhar no Redshift, então as consultas do pipeline precisam de
  testes de integração no Redshift com uma amostra pequena.
