from .database_table import DatabaseTable
from .column import Column


class VariableTTsTable(DatabaseTable):
    """
    The table with translation tables for the project's variables.
    """

    def __init__(self):
        super().__init__(
            "variable_trans_tables",
            "translation tables",
            Column("variable", "VARCHAR(255) UNIQUE"),
            Column("sample", "VARCHAR(255)"),
            Column("is_svar", "BOOLEAN"),
            Column("xml", "BLOB"),
            Column("xml_utf8", "BLOB"),
            Column("date_created", "TIMESTAMP"),
            Column("file_timestamp", "str"),
            indexes=dict(sample_idx="sample", is_svar_idx="is_svar"),
            primary_keys=["variable", "sample"],
        )
