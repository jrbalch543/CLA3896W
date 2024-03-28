from .database_table import DatabaseTable
from .column import Column


class ProjectTable(DatabaseTable):
    """Store the name of the project."""

    def __init__(self):
        super().__init__(
            "project",
            "microdata project",
            Column("project", "VARCHAR(25)"),
            Column("date_created", "TIMESTAMP"),
            primary_keys=["project"],
        )
