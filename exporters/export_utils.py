import re
import sys
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from dataclasses import dataclass
from dataclasses import replace

from ipums.metadata import IPUMS
from ipums.metadata import utilities
from ipums.metadata import MetadataError

log = utilities.setup_logging(__name__)


@dataclass
class ExportOpts:
    project: str
    product: str = None
    projects_config: str = False
    sample: str = False
    db_file: str = False
    cf: str = False

    variable: list = False
    samples: list = None
    docs: list = False
    forms: list = False

    all: bool = False
    allsamples: bool = False
    allvars: bool = False

    debug: bool = False
    force: bool = False
    listvars: bool = False
    listall: bool = False
    serial: bool = False
    dryrun: bool = False
    verbose: bool = False
    no_version: bool = False

    output_type: str = "xml"

    def __post_init__(self):
        proj = self.project
        projects_config = self.projects_config

        product = IPUMS(proj, projects_config=projects_config)
        parent = product.project.parent
        if parent:
            print(f"INFO: {proj} has a parent of {parent}", file=sys.stderr)
            print(
                f"INFO: This script will use {parent} as the project, correct in this context.",
                file=sys.stderr,
            )
            proj = parent

        self.product = product
        self.project = proj

        if self.samples is None:
            self.samples = []

    def set_sample(self, sample):
        self.sample = sample

    def set_cf(self, cf):
        self.cf = cf

    def sample_mode(self):
        """Return an ExportOps object just for sample exports

        This means that all variable and control file options will
        be set to False so that only sample exports are performed.

        Returns:
            ExportOps: ExportOps object with only sample-type options
        """
        return replace(
            self,
            cf=False,
            variable=False,
            allvars=False,
            listvars=False,
        )

    def variable_mode(self):
        return replace(self, sample=False, samples=False, allsamples=False)

    def cf_mode(self):
        return replace(self, sample=False, samples=False, allsamples=False)


class ExportTransTableData(object):
    """Create object for assembling TransTable data for export of a variable.
    This works for both source and integrated variables.
    Args:
        product(ipums.metadata.IPUMS): an IPUMS product object
        variable_type(str): either "integrated" or "svar"
        variable(str): name of variable.
    """

    def __init__(self, product, variable_type, variable):
        self.product = product
        self.project = self.product.project
        self.variable = variable
        self.export_dict_populated = False
        self.timestamp = str(datetime.now())
        self.variable_type = variable_type
        self.seen = defaultdict(lambda: set())

        # cache for svars
        self.tt_sample_svars = {}

        if variable_type != "integrated" and variable_type != "svar":
            raise AttributeError(
                "Variable type needs to be one of 'integrated' or 'svar'"
            )
        if variable_type == "svar":
            self.svar = self.variable
            sample = self.product.samples.svar_to_sample(self.svar)
            self.dd = self.product.dd(sample)
        elif variable_type == "integrated":
            dv = self.product.variables.variable_to_display_variable(variable)
            self.tt = self.product.tt(dv)

        self.tt_export_dict = {}
        self.variable_type = variable_type
        # Might as well populate this here so that we can do the heavy lifting in parallel
        self.populateExportDict()

    def populateExportDict(self):
        if self.export_dict_populated:
            return self.tt_export_dict
        self.export_dict_populated = True
        if self.variable_type == "integrated":
            return self._populateExportDictIntegrated()
        else:
            return self._populateExportDictSvar()

    def _is_svar(self):
        if self.variable_type == "svar":
            return 1
        return 0

    def _get_tt_rowdata(self, table):
        """Meta method for getting rowdata for a tt table."""
        method = "_" + table + "_rowdata"
        call_me = getattr(self, method)
        return call_me()

    def _tt_samplevariables_rowdata(self):
        """Export list of dicts for row data of tt_samplevariables table."""
        self.populateExportDict()
        fields = [
            "hide",
            "svar_doc",
            "rectype",
            "norecode",
            "anchor_inst",
            "univ",
            "anchor_form",
            "restricted",
        ]
        return_rows = []
        for row in self.tt_export_dict["samples"]:
            k = (self.variable, row["id"])
            if self._table_row_unique("variable samples", k):
                return_dict = {
                    "variable": self.variable,
                    "sample": row["id"],
                    "date_created": self.timestamp,
                }
                return_dict["is_svar"] = self._is_svar()
                for field in fields:
                    if field == "sample":
                        return_dict["sample"] = row["id"]
                    elif field in row:
                        return_dict[field] = row[field]
                    else:
                        return_dict[field] = ""
                return_rows.append(return_dict)
        return return_rows

    def _table_row_unique(self, table, k):
        if k in self.seen[table]:
            self._warn_about_dups(table, k)
            return False
        else:
            self.seen[table].add(k)
            return True

    def _warn_about_dups(self, table, k):
        k = [str(key) for key in k]
        print("WARNING! duplicate metadata found updating", table + ":", ", ".join(k))
        return

    def _tt_sample_svars(self, sample):
        sample = sample.lower()
        if sample not in self.tt_sample_svars:
            self.tt_sample_svars[sample] = self.tt.sample_to_svars(sample)
        else:
            return self.tt_sample_svars[sample]
        return self.tt_sample_svars[sample]

    def _tt_samplevariables_sources_rowdata(self):
        """Export list of dicts for row data of tt_variable_sample_sources table."""
        self.populateExportDict()
        return_rows = []
        for row in self.tt_export_dict["samples"]:
            sample = row["id"]
            if self.variable_type == "integrated":
                sources = self._tt_sample_svars(sample)
            else:
                sources = ["columns"]
            if len(sources) > 0:
                for source_order, source in enumerate(sources, start=1):
                    k = (self.variable, sample, source)
                    if self._table_row_unique("sample sources", k):
                        col_start = None
                        col_end = None
                        if "cols" in row and "beg" in row["cols"]:
                            col_start = row["cols"]["beg"]
                            col_end = row["cols"]["end"]
                        return_rows.append(
                            {
                                "sample": sample,
                                "source_order": source_order,
                                "source": source,
                                "is_svar": self.variable_type == "svar",
                                "variable": self.variable,
                                "col_start": col_start,
                                "col_end": col_end,
                                "date_created": self.timestamp,
                            }
                        )
        return return_rows

    def _tt_samplevariables_recodings_rowdata(self):
        """Export list of dicts for row data of tt_variable_sample_recodings table."""
        self.populateExportDict()
        return_rows = []
        for row in self.tt_export_dict["samples"]:
            if "recode" in row:
                for recode in row["recode"]:
                    k = (row["id"], self.variable, recode["orig"])
                    if self._table_row_unique("recodings", k):
                        return_dict = {
                            "sample": row["id"],
                            "variable": self.variable,
                            "outputcode": recode["targ"],
                            "inputcode": recode["orig"],
                            "date_created": self.timestamp,
                        }
                        return_rows.append(return_dict)
        return return_rows

    def _tt_variable_labels_rowdata(self):
        """Export list of dicts for row data of tt_variable_labels table."""
        self.populateExportDict()
        return_rows = []
        for row in self.tt_export_dict["codes"]:
            if row["targetcode"] != "":
                k = (self.variable, row["targetcode"])
            # here, every code block in tt_export_dict, including targetcodes of ""
            labelid = int(self._blank_to_zero(row["id"]))
            k = (self.variable, labelid)
            if self._table_row_unique("output labels", k):
                return_dict = {
                    "variable": self.variable,
                    "labelid": labelid,
                    "labelonly": int(self._blank_to_zero(row["labelonly"])),
                    "label": row["label"],
                    "indent": int(self._blank_to_zero(row["indent"])),
                    "genlab": row["genlab"],
                    "indentgen": int(self._blank_to_zero(row["indentgen"])),
                    "syntax": row["syntax"],
                    "codetype": row["codetype"],
                    "missing": int(self._blank_to_zero(row["missing"])),
                    "outputcode": row["targetcode"],
                    "date_created": self.timestamp,
                }
                return_rows.append(return_dict)
        return return_rows

    def _tt_variable_universedisplayids_rowdata(self):
        """Export list of dicts for row data of tt_variable_universedisplayids table."""
        self.populateExportDict()
        return_rows = []
        for idx, row in enumerate(self.tt_export_dict["universe"], start=1):
            return_dict = {
                "variable": self.variable,
                "universedisplayid": idx,
                "nosampstatement": int(self._blank_to_zero(row["nosampstatement"])),
                "makesampstatement": row["makesampstatement"],
                "sampstatement": row["sampstatement"],
                "univstatement": row["univstatement"],
                "date_created": self.timestamp,
            }
            return_rows.append(return_dict)
        return return_rows

    def _tt_variable_universedisplayid_samples_rowdata(self):
        """Export list of dicts for row data of tt_variable_universedisplayid_samples table."""
        self.populateExportDict()
        return_rows = []
        for idx, row in enumerate(self.tt_export_dict["universe"], start=1):
            for samp in row["samps"]:
                k = (self.variable, samp, idx)
                if self._table_row_unique("universe samples", k):
                    return_dict = {
                        "variable": self.variable,
                        "sample": samp,
                        "universedisplayid": idx,
                        "date_created": self.timestamp,
                    }
                    return_rows.append(return_dict)
        return return_rows

    def _populateExportDictIntegrated(self):
        self.tt_export_dict["var"] = self.tt.variable
        self.tt_export_dict["varlab"] = self.tt.variable_label

        # populate the universe part of self.tt_export_dict
        self._get_univ_dict()
        # populate the codes part of self.tt_export_dict
        self._populate_codes_info()
        # populate samples block of self.tt_export_dict
        self._populate_samples_block()
        return self.tt_export_dict

    def _get_univ_dict(self):
        df = self.tt.ws.iloc[self.tt.univ_start() : self.tt.univ_end() + 1]
        df = df.iloc[:, 0:3]
        df.columns = ["samples", "sampstatement", "univstatement"]
        df = df[~df.samples.isnull()]
        df.sampstatement = df.sampstatement.fillna("")
        df.univstatement = df.univstatement.fillna("")
        universe = df.to_dict(orient="records")

        # populate tt_export_dict with universe info
        self.tt_export_dict["universe"] = []
        for row in universe:
            r = {}
            r["sampstatement"] = row["sampstatement"]
            r["univstatement"] = row["univstatement"]
            r["makesampstatement"] = "0"
            r["nosampstatement"] = "0"
            if row["sampstatement"] == "[all]":
                r["nosampstatement"] = "1"
            elif row["sampstatement"] == "[list]":
                r["makesampstatement"] = "1"

            # one row for every sample
            samps = self.explode_univ_string(row["samples"])
            r["samps"] = []
            for samp in samps:
                r["samps"].append(samp.lower())

            self.tt_export_dict["universe"].append(r)

    def explode_univ_string(self, univ_string):
        """explode univ sample string range, which has an = sign in it.

        an = sign in a univ_string signifies a range of samples currently
        only in use with IHIS and FDA projects.
        In an ideal world we'd deprecate this whole thing, but the
        method is written to be resilient against all projects with
        samples that use 4 digit years.

        """
        # samples are stored UPPER, so therefore so should univ_string
        univ_string = str(univ_string).upper()
        univ_string = univ_string.strip()

        if re.search("=", univ_string):
            all_samples = []
            univ_strings = re.split(r"\s+", univ_string)
            for this_string in univ_strings:
                if re.search("=", this_string):
                    begin, end = re.split("=", this_string)
                    digits_begin = int(re.sub(r"\D", "", begin))
                    digits_end = int(re.sub(r"\D", "", end))
                    if digits_begin > digits_end:
                        raise KeyError(
                            "Sample range goes the wrong direction in time "
                            + this_string
                        )
                    if digits_begin < 1000 or digits_end < 1000:
                        raise KeyError(
                            "Something is wrong with the year strings of "
                            + digits_begin
                            + " and/or "
                            + digits_end
                        )
                    for year in range(digits_begin, digits_end + 1):
                        this_year_string = re.sub(r"\d{4}", str(year), begin)
                        all_samples.append(this_year_string)
                else:
                    all_samples.append(this_string)
        else:
            all_samples = re.split(r"\s+", univ_string)
        return all_samples

    def _populate_codes_info(self):
        seen = set()
        idx = 1
        output_values = self.tt.tt_output_values
        self.tt_export_dict["codes"] = []
        for key, row in output_values.items():
            rowdata = {}
            code = str(key)
            targetcode = code
            if code not in seen:
                seen.add(code)
                if code.startswith("#"):
                    targetcode = ""
                rowdata["id"] = str(idx)

                rowdata["targetcode"] = targetcode
                rowdata["labelonly"] = str(row["labelonly"])

                label, indent = self._calculate_indent(row["label"])
                rowdata["label"] = label
                rowdata["indent"] = indent
                rowdata["syntax"] = str(row["syntax"]).strip()

                genlab, indentgen = self._calculate_indent(row["genlabel"])
                rowdata["genlab"] = genlab
                rowdata["indentgen"] = indentgen

                rowdata["codetype"] = str(row["codetype"]).strip()
                rowdata["missing"] = str(row["missing"]).strip()
                idx = idx + 1
                self.tt_export_dict["codes"].append(rowdata)

    def _calculate_indent(self, cell):
        """calculate the amount of indent based on left padding."""
        label = cell.lstrip()
        indent = str(round((len(cell) - len(label)) / 3))
        return label, indent

    def uses_restricted_data(self):
        """boolean to determine if project uses restricted data."""
        if self.product.project.has_restricted_data:
            return True
        else:
            return False

    def _populate_samples_block(self):
        """put together the <sample> block for all samples."""
        # use raw_samples() for co-homed metadata projects like IHIS/MEPS
        self.tt_export_dict["samples"] = []
        for sample in self.tt.raw_samples():
            samp = {}

            samp["id"] = sample.lower()
            samp["rectype"] = str(self.tt.sample_to_rectype(sample))
            samp["norecode"] = str(self.tt.sample_to_norecode(sample))
            samp["hide"] = str(self.tt.sample_to_hide(sample))
            if self.uses_restricted_data():
                if self.tt.sample_to_proj6(sample) == "restricted":
                    samp["restricted"] = "restricted"

            sample_svars = self._tt_sample_svars(sample)
            if len(sample_svars) > 0:
                svarstring = " ".join(sample_svars)
            else:
                svarstring = self.tt.sample_to_proj1(sample)
            samp["svar"] = svarstring

            # apparently no "iv" projects any more, so this is always ''
            # candidate for deprecation later
            samp["invar"] = ""

            if self.project.uses_anchor_form:
                anchor_form = str(self.tt.sample_to_proj4(sample))
                anchor_inst = str(self.tt.sample_to_proj5(sample))
            else:
                anchor_form = ""
                anchor_inst = ""
            samp["anchor_form"] = anchor_form
            samp["anchor_inst"] = anchor_inst

            svar_doc = self.tt.sample_to_proj2(sample)
            samp["svar_doc"] = svar_doc

            try:
                univ = self.tt.sample_to_univ(sample)
            except KeyError:
                univ = "UNKNOWN!"
            samp["univ"] = univ

            # cols block
            samp["cols"] = self._populate_cols_block(sample)

            # recode block
            if samp["norecode"] != "1":
                samp["recode"] = self._populate_recode_block(sample)

            # add this samp to export_dict
            self.tt_export_dict["samples"].append(samp)

    def _populate_cols_block(self, sample):
        """put together the col block for a given sample."""
        # cols block
        col = self.tt.sample_to_col_loc(sample.upper())
        row = self.tt.rowLabels["COLUMNS"]
        colstring = self.tt.cell(row, col)

        cols_block = []

        if colstring != "":
            cols = colstring.split(";")
            cols = [x.strip() for x in cols]
            this_col = {}
            for col in cols:
                locs = col.split("=")
                locs = [x.strip() for x in locs]
                this_col["beg"] = locs[0]
                if len(locs) == 1:
                    this_col["end"] = locs[0]
                else:
                    this_col["end"] = locs[1]
                cols_block.append(this_col)
        return cols_block

    def _populate_recode_block(self, sample):
        """put together the recode block for a given sample."""
        input_codes = self.tt.sample_to_tt_input_codes(sample)
        code_table = self.tt.sample_to_tt_table(sample)
        input_codes = [x for x in input_codes if self._valid_value(x)]
        recode_block = []
        for recode in input_codes:
            this_recode = {}
            try:
                this_recode["orig"] = recode
                this_recode["targ"] = str(code_table[recode]["output_code"])
                this_recode["lab"] = str(code_table[recode]["input_label"])
                this_recode["freq"] = str(code_table[recode]["freq"])
                recode_block.append(this_recode)
            except KeyError:
                log.critical(
                    " ".join(
                        [
                            "FATAL:",
                            self.tt.variable,
                            "input code with no output code Sample:",
                            sample,
                            "Input Code:",
                            recode,
                        ]
                    )
                )
        return recode_block

    def _valid_value(self, val):
        if val == "*" or val == "" or val == "nan":
            return False
        return True

    def _populateExportDictSvar(self):
        self.tt_export_dict["var"] = self.svar.upper()
        self.tt_export_dict["varlab"] = str(self.dd.svar_to_label(self.svar))

        # self.all_info is used by the populate* methods below it
        self.all_info = self.dd.svar_to_all_info(self.svar)
        self._populate_svar_universe_block()
        self._populate_svar_codes_block()
        self._populate_svar_samples_block()
        return self.tt_export_dict

    def _populate_svar_universe_block(self):
        """put together the <univdisp> block."""
        # populate tt_export_dict with universe info from DD
        self.tt_export_dict["universe"] = []
        row = {}
        row["sampstatement"] = "[list]"
        row["univstatement"] = self.all_info["UNIVSVAR"]
        row["makesampstatement"] = "1"
        row["nosampstatement"] = "0"
        row["samp"] = self.dd.sample.lower()
        self.tt_export_dict["universe"].append(row)

    def _populate_svar_codes_block(self):
        """put together the <code> block for each svar output code."""
        seen = set()
        idx = 1

        self.tt_export_dict["codes"] = []
        nontab = False if self.all_info["NONTAB"] == "" else True
        # throw an error if there's no value
        for d in self.all_info["values_and_freqs"]:
            if not nontab and d["VALUESVAR"] == "":
                raise MetadataError(
                    "The svar "
                    + self.svar
                    + " is not a NonTab and has a blank ValueSvar"
                )
            else:
                code = {}
                targetcode = str(d["VALUESVAR"])
                if targetcode not in seen and not targetcode.startswith("~"):
                    if targetcode.startswith("#"):
                        code["targetcode"] = ""
                        code["labelonly"] = "1"
                    else:
                        seen.add(targetcode)
                        code["targetcode"] = targetcode
                        code["labelonly"] = "0"

                    code["id"] = str(idx)
                    code["label"], code["indent"] = self._calculate_indent(
                        d["VALUELABELSVAR"]
                    )
                    code["syntax"] = ""  # blank for svars
                    code["genlab"] = ""  # blank for svars
                    code["missing"] = ""  # missing is unneeded for svar TT
                    code["indentgen"] = "0"  # 0 for svars
                    code["codetype"] = str(d["CODETY"]).strip()
                    idx = idx + 1
                    self.tt_export_dict["codes"].append(code)

    def _populate_svar_samples_block(self):
        """put together the <sample> block for a single svar."""
        sample = self.dd.sample.lower()
        all_info = self.all_info

        self.tt_export_dict["sample"] = {}
        s = {}
        s["id"] = sample

        s["rectype"] = str(self._blank_to_zero(all_info["SVAR_RECORDTYPE"]))
        s["norecode"] = str(self._blank_to_zero(all_info["NOREC"]))
        s["univ"] = all_info["UNIVSVAR"]
        s["hide"] = all_info["HIDE"]
        if "RESTRICTED" in self.all_info:
            s["restricted"] = all_info["RESTRICTED"]
        for tag in ["svar", "invar", "anchor_form", "svar_doc", "anchor_inst"]:
            s[tag] = ""

        s["cols"] = self._populate_svar_cols_block()
        if s["norecode"] != "1":
            s["recode"] = self._populate_svar_recode_block()

        # even though we have only one sample by definition in an svar
        # write this dict as the first element of a list and attach it to "samples"
        # so as to have parallel construction with integrated variable samples block
        self.tt_export_dict["samples"] = [s]

    def _blank_to_zero(self, cell):
        """turn blank into zero."""
        if cell == "":
            return "0"
        else:
            return cell

    def _populate_svar_cols_block(self):
        """put together the <col> block for a given svar."""
        # cols block
        sw = self.dd.svar_to_start_and_wid(self.svar.upper())
        start = sw["start"]
        end = start + sw["wid"] - 1

        return {"beg": str(start), "end": str(end)}

    def _populate_svar_recode_block(self):
        """put together the <recode> block for a given svar."""
        all_info = self.all_info
        recodes = []
        for d in all_info["values_and_freqs"]:
            row = {}
            try:
                val = str(d["VALUE"])
                targetcode = str(d["VALUESVAR"])
                if self._valid_value(val) and not targetcode.startswith("~"):
                    row["orig"] = str(d["VALUE"])
                    row["targ"] = targetcode
                    row["lab"] = str(d["VALUELABEL"])
                    row["freq"] = str(d["FREQ"])
                    recodes.append(row)
            except KeyError:
                log.critical(
                    " ".join(
                        ["FATAL:", self.svar.upper(), "could not write recode block"]
                    )
                )
        return recodes
