from .database_table import DatabaseTable
from .column import Column


class TTSamplevariablesSources(DatabaseTable):
    """The table that links variable to sample sources."""

    def __init__(self):
        super().__init__(
            "tt_samplevariables_sources",
            "trans table samplevariables-sources link",
            Column("sample", "VARCHAR(255)"),
            Column("variable", "VARCHAR(255)"),
            Column("source", "VARCHAR(25)"),
            Column("is_svar", "INT"),
            Column("source_order", "INT"),
            Column("col_start", "INT"),
            Column("col_end", "INT"),
            Column("date_created", "TIMESTAMP"),
            indexes=dict(
                tt_samplevariables_sources_variables_idx="variable",
                tt_samplevariables_sources_samples_idx="sample",
                tt_samplevariables_sources_is_svar_idx="is_svar",
            ),
            primary_keys=["variable", "sample", "source"],
        )
