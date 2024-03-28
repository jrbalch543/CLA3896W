from .database_table import DatabaseTable
from .column import Column

## Make Dynamic
class InputDataVariableInformationTable(DatabaseTable):
    """The table that stores Svar information from a data dictionary"""

    def __init__(self):
        super().__init__(
            "input_data_variable_info",
            "input data variable info from data dictionary",
            Column("origrow", "INTEGER"),
            Column("sample", "VARCHAR(255)"),
            Column("recordtype", "VARCHAR(2)"),
            Column("var", "VARCHAR(255)"),
            Column("col", "VARCHAR(255)"),
            Column("wid", "VARCHAR(255)"),
            Column("frm", "VARCHAR(255)"),
            Column("varlabel", "VARCHAR(255)"),
            Column("sel", "VARCHAR(255)"),
            Column("notes", "VARCHAR(255)"),
            Column("svar", "VARCHAR(255)"),
            Column("varlabelsvar", "VARCHAR(255)"),
            Column("univsvar", "VARCHAR(255)"),
            Column("norec", "VARCHAR(255)"),
            Column("hide", "VARCHAR(255)"),
            Column("decim", "VARCHAR(255)"),
            Column("string", "VARCHAR(255)"),
            Column("commp", "VARCHAR(255)"),
            Column("codety", "VARCHAR(255)"),
            Column("ddoc1", "VARCHAR(255)"),
            Column("dtag1", "VARCHAR(255)"),
            Column("jdoc1", "VARCHAR(255)"),
            Column("jtag1", "VARCHAR(255)"),
            Column("ddoc2", "VARCHAR(255)"),
            Column("dtag2", "VARCHAR(255)"),
            Column("jdoc2", "VARCHAR(255)"),
            Column("jtag2", "VARCHAR(255)"),
            Column("date_created", "TIMESTAMP"),
            Column("file_timestamp", "str"),
            indexes=dict(
                input_data_variable_origrow_idx="origrow",
                input_data_variable_sample_idx="sample",
                input_data_variable_variable_idx="var",
                # input_data_variable_svar_idx="svar"
            ),
            primary_keys=["origrow", "sample", "var"],
        )


class InputDataVariableValueTable(DatabaseTable):
    """The table that stores"""

    def __init__(self):
        super().__init__(
            "input_data_variable_values",
            "input data variable value information",
            Column("origrow", "INTEGER"),
            Column("sample", "VARCHAR(255)"),
            Column("recordtype", "VARCHAR(255)"),
            Column("svar", "VARCHAR(255)"),
            Column("value", "VARCHAR(255)"),
            Column("valuelabel", "VARCHAR(255)"),
            Column("freq", "VARCHAR(255)"),
            Column("valuesvar", "VARCHAR(255)"),
            Column("valuelabelsvar", "VARCHAR(255)"),
            Column("date_created", "TIMESTAMP"),
            Column("file_timestamp", "str"),
            indexes=dict(
                input_data_variable_value_origrow_idx="origrow",
                input_data_variable_value_sample_idx="sample",
                # input_data_variable_value_recordtype_idx="recordtype",
                # input_data_variable_value_svar_idx="svar",
                # input_data_variable_value_value_idx="value",
            ),
            primary_keys=["origrow", "sample"],
        )
