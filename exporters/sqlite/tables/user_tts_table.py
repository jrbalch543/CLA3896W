from .database_table import DatabaseTable
from .column import Column


class UserTTsTable(DatabaseTable):
    """
    User TTs are text representations (csv) of Excel TTs. Currently ipumsi only.
    """

    def __init__(self):
        super().__init__(
            "user_tts",
            "user tts",
            Column("variable", "VARCHAR(255) UNIQUE"),
            Column("sample", "VARCHAR(255)"),
            Column("is_svar", "BOOLEAN"),
            Column("user_tt", "BLOB"),
            Column("date_created", "TIMESTAMP"),
            Column("file_timestamp", "str"),
            indexes=dict(variable_idx="variable"),
            primary_keys=["variable", "sample"],
        )
