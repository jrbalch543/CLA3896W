from .database_table import DatabaseTable
from .column import Column


class SamplevariableEditingRulesTable(DatabaseTable):
    def __init__(self):
        # Could be extended with a "language" column if we ever
        # support more than just Stata.
        super().__init__(
            "samplevariable_editing_rules",
            "Editing rule syntax templates for sample variable combonations.",
            Column("sample", "VARCHAR(255)"),
            Column("variable", "VARCHAR(255)"),
            Column("syntax", "VARCHAR(255)"),
            Column("edit_order", "INT"),
            Column("has_getSourceData_call", "BOOLEAN"),
            Column(
                "ids", "VARCHAR(255)"
            ),  # This is a set and I'm not sure I like stuffing it into one field.
            Column("date_created", "TIMESTAMP"),
            indexes=dict(rule_variable_idx="variable", rule_sample_idx="sample"),
            primary_keys=["variable", "sample"],
        )
