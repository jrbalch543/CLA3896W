from .database_table import DatabaseTable
from .column import Column


class TTLastUpdated(DatabaseTable):
    """The table that shows when a given sample/variable was last updated."""

    def __init__(self):
        super().__init__(
            "tt_last_updated",
            "trans table last updated",
            Column("sample_or_variable", "VARCHAR(255)"),
            Column("date_created", "TIMESTAMP"),
            primary_keys=["sample_or_variable"],
        )
