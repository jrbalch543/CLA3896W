from .database_table import DatabaseTable
from .column import Column


class TTVariableUniverseDisplayids(DatabaseTable):
    """The table that contains trans table universe display ids."""

    def __init__(self):
        super().__init__(
            "tt_variable_universedisplayids",
            "trans table universe display ids",
            Column("universedisplayid", "INT"),
            Column("variable", "VARCHAR(255)"),
            Column("nosampstatement", "INT"),
            Column("makesampstatement", "VARCHAR(255)"),
            Column("sampstatement", "VARCHAR(255)"),
            Column("univstatement", "VARCHAR(255)"),
            Column("date_created", "TIMESTAMP"),
            indexes=dict(tt_variable_universedisplayids_variable_idx="variable"),
            primary_keys=["variable", "universedisplayid"],
        )
