from .database_table import DatabaseTable
from .column import Column


class VarDescTable(DatabaseTable):
    def __init__(self):
        super().__init__(
            "variable_descriptions",
            "variable descriptions",
            Column("variable", "VARCHAR(255) UNIQUE"),
            Column("sample", "VARCHAR(255)"),
            Column("is_svar", "BOOLEAN"),
            Column("xml", "TEXT"),
            Column("date_created", "TIMESTAMP"),
            Column("file_timestamp", "str"),
            indexes=dict(vd_sample_idx="sample", vd_is_svar_idx="is_svar"),
        )
