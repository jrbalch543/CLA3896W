from .database_table import DatabaseTable
from .column import Column


class TTSamplevariablesRecodings(DatabaseTable):
    """The table that stores variable recodings."""

    def __init__(self):
        super().__init__(
            "tt_samplevariables_recodings",
            "trans table samplevariables recodings",
            Column("sample", "VARCHAR(255)"),
            Column("variable", "VARCHAR(255)"),
            Column("outputcode", "BLOB"),
            Column("inputcode", "BLOB"),
            Column("date_created", "TIMESTAMP"),
            indexes=dict(
                tt_samplevariables_recodings_sample_idx="sample",
                tt_samplevariables_recodings_variable_idx="variable",
            ),
            primary_keys=["variable", "sample", "inputcode"],
        )
