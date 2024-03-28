from .database_table import DatabaseTable
from .column import Column


class EnumMaterialsTable(DatabaseTable):
    """The table with the project's enumeration materials."""

    def __init__(self):
        super().__init__(
            "enum_materials",
            "enumeration materials",
            Column("enum_material", "VARCHAR(255) UNIQUE"),
            Column("xml", "TEXT"),
            Column("date_created", "TIMESTAMP"),
            Column("file_timestamp", "datetime"),
            primary_keys=["enum_material"],
        )
