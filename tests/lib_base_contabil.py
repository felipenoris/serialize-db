"""O nome pelo qual ``tests/reference_model/model_base_gerencial.py`` importa a ``Base`` do pipeline.

A biblioteca ``lib_base_contabil`` não está no repositório; este módulo faz o nome apontar para o
arquivo do modelo de referência, que fica como está.
"""

from reference_model.model_base_contabil import Base  # noqa: F401
