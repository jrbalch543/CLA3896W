from .database_table import DatabaseTable
from .column import Column


class SampleSvarTTExportTable(DatabaseTable):
    """
    Keeps track of what samples have been exported and if svar TTs are present.
    """

    def __init__(self):
        super().__init__(
            "sample_svar_tt_export",
            "samples and whether svar tt exports present",
            Column("sample", "VARCHAR(255) UNIQUE"),
            Column("has_tt_exports", "BOOLEAN"),
            Column("date_created", "TIMESTAMP"),
            primary_keys=["sample"],
        )
