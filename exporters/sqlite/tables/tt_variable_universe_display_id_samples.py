from .database_table import DatabaseTable
from .column import Column


class TTVariableUniverseDisplayIdSamples(DatabaseTable):
    """The table that contains trans table universe display id sample links."""

    def __init__(self):
        super().__init__(
            "tt_variable_universedisplayid_samples",
            "trans table variable universe display id sample links",
            Column("sample", "VARCHAR(255)"),
            Column("variable", "VARCHAR(255)"),
            Column("universedisplayid", "INT"),
            Column("date_created", "TIMESTAMP"),
            indexes=dict(tt_variable_universedisplayid_samples_sample_idx="sample"),
            primary_keys=["variable", "universedisplayid", "sample"],
        )
