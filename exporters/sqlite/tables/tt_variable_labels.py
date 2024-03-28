from .database_table import DatabaseTable
from .column import Column


class TTVariableLabels(DatabaseTable):
    """The table that contains trans table variable labels and ids."""

    def __init__(self):
        super().__init__(
            "tt_variable_labels",
            "trans table variable label ids",
            Column("labelid", "INT"),
            Column("variable", "VARCHAR(255)"),
            Column("labelonly", "INT"),
            Column("label", "VARCHAR(255)"),
            Column("indent", "INT"),
            Column("genlab", "VARCHAR(255)"),
            Column("indentgen", "INT"),
            Column("syntax", "VARCHAR(255)"),
            Column("codetype", "VARCHAR(255)"),
            Column("outputcode", "VARCHAR(255)"),
            Column("missing", "VARCHAR(255)"),
            Column("date_created", "TIMESTAMP"),
            indexes=dict(tt_variable_labels_variable_idx="variable"),
            primary_keys=["variable", "labelid"],
        )
