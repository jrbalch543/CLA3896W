from .database_table import DatabaseTable
from .column import Column


class WebDocumentsTable(DatabaseTable):
    """
    The table with the project's web documents.
    """

    def __init__(self):
        super().__init__(
            "web_docs",
            "web documents",
            Column("web_doc", "VARCHAR(255) UNIQUE"),
            Column("xml", "TEXT"),
            Column("date_created", "TIMESTAMP"),
            Column("file_timestamp", "str"),
            primary_keys=["web_doc"],
        )
