from .database_table import DatabaseTable
from .column import Column


class TTSamplevariablesTable(DatabaseTable):
    """The table that links variables to samples."""

    # XXX consider using a dataclass for these (would require bumping python to >= 3.7)
    def __init__(self):
        super().__init__(
            "tt_samplevariables",
            "trans table samplevariables records",
            Column("sample", "VARCHAR(255)"),
            Column("variable", "VARCHAR(255)"),
            Column("hide", "INT"),
            Column("svar_doc", "VARCHAR(255)"),
            Column("is_svar", "INT"),
            Column("rectype", "VARCHAR(10)"),
            Column("norecode", "INT"),
            Column("anchor_inst", "VARCHAR(10)"),
            Column("univ", "VARCHAR(255)"),
            Column("anchor_form", "VARCHAR(25)"),
            Column("restricted", "VARCHAR(20)"),
            Column("date_created", "TIMESTAMP"),
            indexes=dict(tt_variable_idx="variable", tt_sample_idx="sample"),
            primary_keys=["variable", "sample"],
        )
