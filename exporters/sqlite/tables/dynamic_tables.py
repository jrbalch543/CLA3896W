""" Here are primary key mappings for tables that don't have a standardized schema across projects."""
DYNAMIC_PRIMARY_KEY_MAPPINGS = {
    "citations": ["product", "doi"],
    "links": ["link"],
    "constants": ["constant", "value"],
    "rectypes": ["rectype"],
    "variable_renames": ["old_name"],
    "countries": ["country"],
    "variable_trans_tables": ["variable"],
    "samples": ["sample"],
    "survey_groups": ["name"],
    "tt_samplevariables_recodings": ["sample", "variable", "inputcode"],
}
