from .database_table import DatabaseTable
from .column import Column


class InsertHtmlTable(DatabaseTable):
    """
    The table with the project's insert_html documents.
    """

    FILE_SUFFIX = ".html"

    def __init__(self):
        super().__init__(
            "insert_html",
            "insert_html documents",
            Column("insert_html", "VARCHAR(255) UNIQUE"),
            Column("filename", "VARCHAR(255) UNIQUE"),
            Column("html", "TEXT"),
            Column("date_created", "TIMESTAMP"),
            Column("file_timestamp", "str"),
            primary_keys=["insert_html"],
        )
