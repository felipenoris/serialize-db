"""O nome pelo qual ``reference_model/model_db_projetado.py`` importa a ``Base`` do pipeline.

A biblioteca ``lib_base_gerencial`` não está no repositório; este módulo faz o nome apontar para o
arquivo do modelo de referência, que fica como está.
"""

from reference_model.model_base_gerencial import Base  # noqa: F401
