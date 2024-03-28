"""Tools for exporting data from Excel to misc formats."""
import os
import sys
import re
from pathlib import Path
import warnings
import json
import abc

from lxml import etree as etree
from pandas import pandas as pd
import numpy as np

from ipums.metadata.exporters import SqliteMetadataDumper
from ipums.metadata import MpcDocument
from ipums.metadata import utilities
from ipums.metadata import MetadataError

log = utilities.setup_logging(__name__)
warnings.simplefilter("default")

# This is a workaround for a bug in pandas to_csv()
# the line_terminator argument in this method does not
# do the right thing cross-platform, so instead we are hard-coding
# the line ending depending on platform
if sys.platform == "linux":
    LINE_TERMINATOR = "\r\n"
else:
    LINE_TERMINATOR = "\n"

"""Class for formatting a yield message dict of common format."""


class MessageYielder(object):
    """Yield messages as a dict."""

    def __msg(self, msg, typ):
        return {"text": msg, "type": typ}

    def ok(self, msg):
        return self.__msg(msg, "ok")

    def warn(self, msg):
        return self.__msg(msg, "warn")

    def error(self, msg):
        return self.__msg(msg, "error")


class CreateVariablesQuick(object):
    """Create variables_quick.txt from .txt files in quick/."""

    def __init__(self, product=None):
        self.product = product
        self.project = self.product.project

    def run(self):
        """Create the variables_quick.txt file."""
        try:
            m_dir = Path("/".join([self.project.path, "metadata/control_files"]))
            quick_dir = m_dir / "quick"

            trigger = quick_dir / "needs_cat"
            vars_quick = m_dir / "variables_quick.txt"

            if trigger.exists():
                g = quick_dir.glob("*.txt")
                filenames = [x for x in g]
                filenames.sort()
                with open(str(vars_quick), "w", newline="\r\n") as vq:
                    for fname in filenames:
                        with open(str(fname)) as f:
                            vq.write(f.read())
                result = {
                    "text": "variables_quick.txt successfully exported",
                    "type": "ok",
                }
                # remove trigger
                os.remove(str(trigger))
            else:
                result = {
                    "text": "Skip: " + str(vars_quick.name) + " is current.",
                    "type": "warn",
                }

        # all exceptions get passed back as a result dict
        except Exception:
            e = sys.exc_info()[1]
            result = {
                "text": "Something failed in variables_quick.txt export! " + str(e),
                "type": "error",
            }
        return result


class ExportBase(abc.ABC):
    """Base class for Export classes."""

    def __init__(self, **kwargs):
        self.product = kwargs["product"]
        self.constants = self.product.constants
        self.project = self.product.project.name
        self.projects_config = self.product.projects_config
        self.proj_path = Path(
            utilities.project_to_path(
                self.project, projects_config=self.projects_config
            )
        )
        obj_kwargs = [
            "product",
            "db_file",
            "list_only",
            "force",
            "dryrun",
            "verbose",
            "batch",
            "dd",
            "debug",
        ]
        for k in obj_kwargs:
            setattr(self, k, kwargs.get(k, None))
        self.msg = MessageYielder()
        if "filepath" in kwargs:
            self.filepath = kwargs["filepath"]
        elif hasattr(self, "sample"):
            self.filepath = str(
                self.proj_path
                / self.product.samples.sample_to_dd(self.sample, self.project)
            )


class ExportAllDDSvarsCsv(ExportBase):
    """Export all svars from all DDs to csv."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def to_csv_generator(self):
        """Generator for exporting all svars from CFs and DDs to csv.

        This generator yields a results dict with keys type and text
        as it iterates through all exports.

        Example calling code::

            gen = export.to_csv_generator()
            for result in gen:
                if result['type'] == 'error':
                    print('Doh!', result['text'], file = sys.stderr)
                else:
                    print('Success!', result['text'])
        """
        projects = utilities.projects(projects_config=self.projects_config)
        if self.project not in projects:
            raise KeyError(self.__class__ + "requires a valid project")
        samples = self.product.samples

        control_files = [
            Path(f).stem
            for f in self.constants.metadata_control_files
            if Path(f"{self.product.project.path}/{f}").exists()
        ]
        if self.list_only:
            all_files = []
            all_files.extend(control_files)
            all_files.extend(samples.all_samples)
            for f in all_files:
                yield self.msg.ok(f)
        else:
            self.errors_detected = []

            for row, cf_name in enumerate(control_files, start=2):
                obj = ExportControlFileCsv(
                    product=self.product,
                    cf_name=cf_name,
                    batch=True,
                    force=self.force,
                    dryrun=self.dryrun,
                    db_file=self.db_file,
                )
                result = obj.to_csv()
                yield result

            for sample in samples.all_samples:
                sample = sample.rstrip()
                dd = samples.sample_to_dd(sample)
                obj = ExportSvarsCsv(
                    sample=sample,
                    product=self.product,
                    samples=samples,
                    filepath=dd,
                    force=self.force,
                    db_file=self.db_file,
                    dryrun=self.dryrun,
                    all_samples=True,
                )
                result = obj.to_csv()
                yield result

            if len(self.errors_detected) > 0:
                errors = ["Error(s) detected! Please analyze output below"]
                errors.extend(self.errors_detected)
                yield self.msg.error("Error(s) detected! Please analyze output below")
            # else:
            #     if not self.dryrun:
            #         yield self.msg.ok("Svars successfully exported!")

            if not self.dryrun:
                vars_quick = CreateVariablesQuick(product=self.product)
                yield vars_quick.run()

    def export_the_dds(self):
        dump = SqliteMetadataDumper(
            product=self.product, verbose=self.verbose, db_file=self.db_file
        )
        dump.create_input_data_variables_tables()


class ExportControlFileCsv(ExportBase):
    """Class to export control file data to csv."""

    def __init__(self, cf_name=None, **kwargs):
        if not cf_name:
            print("cf_name is a required parameter")
            return False
        self.cf_name = cf_name
        super().__init__(**kwargs)

    def to_csv(self):
        """Dump a control file's data to csv.

        The variables control file is forked to separate method,
        as it needs to follow a specific column order to sync
        with Data Dictionary source variable exports.
        All others fall to generic method.
        """
        try:
            cf_stem = Path(self.cf_name).stem
            # variables need special handling
            if cf_stem == "variables":
                result = self.__export_integrated_variables()
                # create variables_quick.txt if needed
                if not self.dryrun and not self.batch:
                    vars_quick = CreateVariablesQuick(product=self.product)
                    vars_quick.run()
            # samples does some data frame massaging in the object, so be specific
            elif cf_stem == "samples":
                self.cf = self.product.samples
                result = self.__export_generic_cf()
            # all others fall to Generic
            else:
                self.cf = self.product.control_file(cf_stem)
                result = self.__export_generic_cf()
            # update sqlite
            if not self.dryrun and result["type"] == "ok":
                dumper = SqliteMetadataDumper(
                    product=self.product, verbose=self.verbose, db_file=self.db_file
                )
                dumper.create_cf_table(cf_stem)
        # all exceptions get passed to Excel
        except UnicodeError:
            e = sys.exc_info()[1]
            result = {"text": "Something went wrong! " + str(e), "type": "error"}
        return result

    def __export_generic_cf(self):
        """Dump generic control file from a project to csv.

        This method should be called only from ExportControlFileCsv.
        """
        try:
            csvf = Path(self.cf.xlpath).stem + ".csv"
            proj_path = self.cf.project.path
            raw_path = "/".join([proj_path, "metadata", "control_files", csvf])
            csv_file = Path(raw_path)
            os.makedirs(str(csv_file.parent), exist_ok=True)
            df = self.cf.ws.copy(deep=True)
            df.columns = [col.lower() for col in df.columns]
            df = df.replace(r"^\s*$", np.nan, regex=True)
            needs_export = utilities.needs_cache(self.cf.xlpath, str(csv_file))
            if self.force or needs_export:
                if self.dryrun:
                    result = {"text": str(csv_file), "type": "ok"}
                else:
                    df.to_csv(
                        str(csv_file),
                        sep=",",
                        index=False,
                        encoding="utf8",
                        float_format="%.f",
                        lineterminator=LINE_TERMINATOR,
                    )
                    result = {
                        "text": "control file exported to " + str(csv_file),
                        "type": "ok",
                    }
            else:
                result = {
                    "text": "Skip: " + str(csv_file.name) + " is current.",
                    "type": "warn",
                }
        # all exceptions get passed to Excel
        except UnicodeError:
            e = sys.exc_info()[1]
            result = {"text": "Something went wrong! " + str(e), "type": "error"}
        return result

    def __export_integrated_variables(self):
        """Dump integrated variables from a project to csv."""
        try:
            f = "metadata/control_files/integrated_variables.csv"
            csv_file = Path(
                utilities.project_to_path(
                    self.project, projects_config=self.product.projects_config
                )
            ) / Path(f)
            os.makedirs(str(csv_file.parent), exist_ok=True)
            ivars = self.product.variables

            ivars.run_audit(audit_level="fail")

            needs_export = utilities.needs_cache(ivars.xlpath, str(csv_file))
            if self.force or needs_export:
                if self.dryrun:
                    msg = str(csv_file.name)
                else:
                    # utilities.variables_cf_column_order() governs both the
                    # content and order of the output csv. Read the data frame
                    # and determine which columns need culling and which need
                    # to be dynamically added. It's essential this happens so it
                    # syncs up with the columns of DD control file data
                    ivars.ws.columns = [col.lower() for col in ivars.ws.columns]
                    headers = [
                        col.lower() for col in ivars.project.variables_cf_column_order
                    ]
                    for col in headers:
                        if col not in ivars.ws.columns:
                            ivars.ws[col] = ""
                    for col in ivars.ws.columns:
                        if col not in headers:
                            ivars.ws = ivars.ws.drop(col, 1)
                    ivars.ws.reindex(headers, axis=1)
                    # Set blanks/space-only cells to NaNs for consistent csv output
                    ivars.ws = ivars.ws.replace(r"^\s*$", np.nan, regex=True)
                    ivars.ws.to_csv(
                        str(csv_file),
                        sep=",",
                        encoding="utf8",
                        header=True,
                        index=False,
                        float_format="%.f",
                        lineterminator=LINE_TERMINATOR,
                    )
                    msg = self.project + " integrated variables exported"
                    # Set NaNs back to blanks as would be expected if the control file is
                    # accessed again after this method.
                    ivars.ws = ivars.ws.fillna("")
                result = {"text": msg, "type": "ok"}
            else:
                msg = "Skip: " + str(csv_file.name) + " is current."
                result = {"text": msg, "type": "warn"}

            if not self.dryrun:
                # export integrated_variables_quick.txt
                f = "metadata/control_files/quick/integrated_variables_quick.txt"
                quick_file = Path(
                    utilities.project_to_path(
                        self.product.project.name,
                        projects_config=self.product.projects_config,
                    )
                ) / Path(f)
                os.makedirs(str(quick_file.parent), exist_ok=True)
                needs_export = utilities.needs_cache(ivars.xlpath, str(quick_file))
                if self.force or needs_export:
                    # create quick file
                    ivars.ws.columns = [col.upper() for col in ivars.ws.columns]
                    # Set blanks/space-only cells to NaNs for consistent csv output
                    ivars.ws = ivars.ws.replace(r"^\s*$", np.nan, regex=True)
                    ivars.ws["QUICK_SVAR"] = 0
                    ivars.ws.to_csv(
                        str(quick_file),
                        sep="\t",
                        header=False,
                        index=False,
                        float_format="%.f",
                        columns=["VARIABLE", "SAMPLE", "QUICK_SVAR"],
                        lineterminator=LINE_TERMINATOR,
                    )
                    # touch a file
                    needs_cat = str(quick_file.parent) + "/needs_cat"
                    if not Path(needs_cat).exists():
                        open(needs_cat, "w").close()

                    # Set NaNs back to blanks as would be expected if the control file is
                    # accessed again after this method.
                    ivars.ws = ivars.ws.fillna("")

        # all exceptions get passed to Excel
        except UnicodeError:
            e = sys.exc_info()[1]
            result = {"text": "Something went wrong! " + str(e), "type": "error"}
        return result


class ExportSvarsCsv(ExportBase):
    """Export source variable control file data to csv."""

    def __init__(self, **kwargs):
        self.sample = kwargs["sample"]
        super().__init__(**kwargs)
        if "samples" in kwargs:
            self.samples = kwargs["samples"]
        else:
            self.samples = self.product.samples
            self.samples.run_audit(audit_level="fail")
        if "all_samples" in kwargs:
            self.skip_dd = True
        else:
            self.skip_dd = False

    def to_csv(self):
        """Exports a DD's source variable control file data to csv.

        ExportSvarsCsv objects are instantiated with a project and
        sample. When to_csv() is called, that Data Dictionary is
        read into memory and the control file data for the source
        variables are exported to csv. The export has the same
        column data in the same order as the integrated variables
        control file.
        """
        try:
            result = self.__dd_svars_to_csv()
            if not self.dryrun:
                vars_quick = CreateVariablesQuick(product=self.product)
                vars_quick.run()

                if result["type"] == "ok":
                    # update sqlite
                    dump = SqliteMetadataDumper(
                        product=self.product, verbose=self.verbose, db_file=self.db_file
                    )
                    dump.update_sample_control_file_data(self.sample)
                    if not self.skip_dd:
                        dump.update_input_data_variables_tables([self.sample])

        except FileNotFoundError:
            result = self.msg.error(str(self.filepath) + " NOT FOUND!")
        except UnicodeError as e:
            result = self.msg.error(
                "Something went wrong reading " + str(self.filepath) + "! " + str(e)
            )
        # this is one of those rare cases where we want to trap *every*
        # exception, because we want to throw it over the fence to Excel
        # sys.exc_info() is a tuple, where elem 0 is the class of exception
        # and elem 1 is the error object ( wrap in a str() to get the error )
        except Exception:
            e = sys.exc_info()[1]
            result = self.msg.error(f"Metadata error in {self.sample} DD! {str(e)}")

        return result

    def __repr__(self):
        return "{}('{}')".format(self.__class__.__name__, self.filepath)

    def __dd_svars_to_csv(self):
        """Takes a sample and project and saves all svars to a csv file
        at <proj_dir>/metadata/control_files/svars_csv/<sample>.csv."""
        m_stub = "metadata/control_files/svars_csv/"
        svars_file = self.sample.lower() + "_svars.csv"
        rel_path = m_stub + "/" + svars_file
        csv_path = self.proj_path / Path(rel_path)
        os.makedirs(str(csv_path.parent), exist_ok=True)

        if not os.path.isfile(self.filepath):
            err = (
                "Data dictionary path for "
                + self.sample.lower()
                + " of "
                + self.filepath
                + " not valid!"
            )
            return self.msg.error(err)

        dd = None
        needs_export = utilities.needs_cache(self.filepath, str(csv_path))
        if self.force or needs_export:
            if self.dryrun:
                return_val = self.msg.ok(str(csv_path))
                # if dryrun, we're done here
                return return_val
            else:
                dd = self.product.dd(self.sample)
                headers = dd.project.variables_cf_column_order
                all_sample_svar_data = []
                for svar in dd.all_svars_ddorder:
                    row_data = dd.__svar_to_variables_control_rowdata__(svar)
                    all_sample_svar_data.append(row_data)
                df = pd.DataFrame(all_sample_svar_data)
                df.columns = [col.lower() for col in headers]
                df.to_csv(
                    str(csv_path),
                    sep=",",
                    header=True,
                    index=False,
                    encoding="utf8",
                    float_format="%.f",
                    lineterminator=LINE_TERMINATOR,
                )
                return_val = self.msg.ok("Exported to " + str(csv_path))
        else:
            return_val = self.msg.warn("Skip: " + str(csv_path.name) + " is current.")

        if not self.dryrun:
            # write quick file
            quick_file = "svars_" + self.sample.lower() + "_quick.txt"
            quick_path = self.proj_path / "metadata/control_files/quick" / quick_file
            os.makedirs(str(quick_path.parent), exist_ok=True)
            needs_export = utilities.needs_cache(self.filepath, str(quick_path))
            if self.force or needs_export:
                if not dd:
                    dd = self.product.dd(self.filepath)
                with open(str(quick_path), "w") as f:
                    for svar in dd.all_svars_ddorder:
                        line = "\t".join([svar, self.sample.upper(), "1\n"])
                        f.write(line)
                # touch a file
                needs_cat = str(quick_path.parent) + "/needs_cat"
                if not Path(needs_cat).exists():
                    open(needs_cat, "w").close()
        return return_val


class ExportJsonFile(object):
    """This class exports a structured tt dict to json.

    tt_export_dict is a dict specifically structured for export of translation
    table information.
    """

    def __init__(self, output_file, tt_export_dict, encoding="utf8"):
        self.output_file = output_file
        self.tt_export_dict = tt_export_dict
        self.encoding = encoding

    def export_tt(self):
        with open(str(self.output_file), "w", encoding=self.encoding) as fh:
            json.dump(self.tt_export_dict, fh, indent=4)


class ExportUserCsvFile(object):
    """This class exports a modified tt dataframe to csv."""

    def __init__(self, output_file, tt, samples, encoding="utf8"):
        """Initialize ExportUserCsvFile object

        Args:
            output_file (filepath): Path to desired output csv file
            tt (ipums.metadata.TranslationTable): Translation Table object
            samples (ipums.metadata.Samples): Samples object
            encoding (str, optional): File encoding for exported csv. Defaults to "utf8".
        """
        self.output_file = output_file
        self.tt = tt
        self.samples = samples
        self.encoding = encoding
        self.user_df = self.to_user_df()

    def to_user_df(self):
        """Returns the User Translation Table as a Dataframe for export to csv

        Returns:
            pandas.DataFrame: Dataframe with modified TT information fit for public consumption
        """
        tt = self.tt
        user_df = pd.concat([self.__header_block(), self.__user_csv_tt_block()])

        # Drop hidden columns
        user_df = user_df.T[~tt.ws.T[1].isin(["1", "2"])].T

        # Drop everything to the right of an "UNNAMED:" column
        user_df = user_df.drop(columns=user_df.columns[tt.samples_end() + 1 :])

        # Drop unwanted columns
        unwanted_columns = ["SYNTAX", "GENLABEL", "CODETYPE", "MISSING"]
        user_df = user_df.drop(columns=unwanted_columns)

        # remove frequencies from cells
        # (Assumes the only strings of numbers between curly braces anywhere in the file are frequencies)
        user_df = user_df.replace(r"\{\d+\}", "", regex=True)

        return user_df

    def export_tt(self):
        """Write user csv file"""
        with open(str(self.output_file), "w", encoding=self.encoding) as fh:
            self.user_df.to_csv(fh, index=False)

    def __parse_header_item(self, item):
        """Translate samples from header to publicly recognized sample names

        Sample names have spaces replaced with '_' so as to be identified as a single "word"

        Args:
            item (str): A single sample from a header or one of the expected other non-sample values.

        Returns:
            str: Either an expected non-sample header value or a publicly recognized sample name
        """
        try:
            sample_info = self.samples.sample_all_info(item)
            name = (
                sample_info["DISPLAY_NAME"]
                if "DISPLAY_NAME" in sample_info
                else sample_info["LONG_NAME"]
            )
            parsed_item = name.replace(" ", "_")
        except TypeError:
            print(item)
            parsed_item = None
        return parsed_item

    def __tt_samples_to_names(self, samples_list):
        """Translate header labels to User Translation Table version

        Args:
            samples_list (list): List of one or more samples or a known header label

        Returns:
            str: String representation of header label, primarily lists of sample names, '; '-delimited
        """
        samples = samples_list.strip().split(" ")
        parsed_samples = "; ".join([self.__parse_header_item(s) for s in samples])
        return parsed_samples

    def __user_csv_tt_block(self):
        """Dataframe of just the translation part of the Translation Table

        Returns:
            pandas.DataFrame: tt block of translation table
        """
        df = self.tt.ws.iloc[self.tt.tt_start() : self.tt.tt_end() + 1]
        df = df[~df.CODE.isnull()]
        return df

    def __header_block(self):
        """Header rows for user translation table csv

        Currently includes: Sample(s), svars, recode status

        Returns:
            pandas.DataFrame: Dataframe of header rows for user tt csv
        """
        # The header rows are 0 through 10
        df = self.tt.ws.iloc[:10]
        row_index = {
            "rectype": 0,
            "columns": 1,
            "norecode": 2,
            "hide": 3,
            "svars": 4,
            "proj2": 5,
            "proj3": 6,
            "proj4": 7,
            "proj5": 8,
            "proj6": 9,
            "sample_name": 10,
        }
        df = df.T
        df[row_index["sample_name"]] = df.apply(
            lambda x: self.__tt_samples_to_names(x.name)
            if x.name
            in self.tt.ws.columns[self.tt.samples_start() : self.tt.samples_end() + 1]
            else x.name,
            axis=1,
        )
        # Translate norecode codes to meaning
        df[row_index["norecode"]] = df[row_index["norecode"]].replace(
            {"1": "No recoding", "2": "Partial recoding"}
        )

        # select only the rows we want to include
        df = df[[row_index[k] for k in ["sample_name", "svars", "norecode"]]].T

        df["CODE"] = ""
        df["LABEL"] = ""
        return df


class ExportXmlFile(object):
    """This class exports either an xml_block or a structured tt dict to xml.

    tt_export_dict is a dict specifically structured for export of translation
    table information into XML.
    xml_block is a raw block of xml. xml_block is typically used by variable
    description export.
    """

    def __init__(
        self, output_file, xml_block=None, tt_export_dict=None, encoding="utf8"
    ):
        self.xml_file = []
        self.output_file = output_file
        self.xml_block = xml_block
        self.tt_export_dict = tt_export_dict
        self.encoding = encoding

    def export_tt(self, tt_type="integrated"):
        """all the beautiful soup."""
        self.__write_xml_preamble()
        if tt_type == "integrated":
            self.__write_universe_xml_block()
            self.__write_codes_xml_block()
            self.__write_samples_xml_block()
        else:
            self.__write_svar_universe_xml_block()
            self.__write_svar_codes_xml_block()
            self.__write_svar_samples_xml_block()
        self.__write_xml_suffix()
        self.writeXmlFile()

    def export_vd(self):
        self.writeXmlFile()

    def xmlTag(self, tag, openOrClose="open"):
        """xml tag open or close. defaults to open."""
        if openOrClose.lower() == "open":
            return "<" + tag + ">"
        elif openOrClose.lower() == "close":
            return "</" + tag + ">"

    def writeXmlTag(self, tag, openOrClose="open"):
        self.xml_file.append(self.xmlTag(tag, openOrClose))

    def writeXmlTagEnc(self, tag, string):
        self.xml_file.append(self.xmlTagEnc(tag, string))

    def xmlTagEnc(self, tag, string):
        """enclose string in xml tag."""
        if "<" in string or "&" in string:
            tag = etree.Element(tag)
            tag.text = etree.CDATA(string)
            return etree.tostring(tag, encoding="unicode")
        else:
            return self.xmlTag(tag, "open") + string + self.xmlTag(tag, "close")

    def write_line(self, strings, number_of_tabs=0):
        """join list of strings and adds specified tab indent."""
        string_list = ["\t" * number_of_tabs, "".join(strings)]
        self.xml_file.append("".join(string_list))

    def writeXmlNewline(self):
        """add a blank line to xml_file."""
        self.xml_file.append("")

    def writeXmlFile(self):
        if self.xml_block:
            xml = self.xml_block
        else:
            xml = "\n".join(self.xml_file)

        fh = open(str(self.output_file), "w", encoding=self.encoding)
        fh.write(xml)
        fh.write("\n")
        fh.close()

    def __write_xml_suffix(self):
        # xml file close tags
        self.writeXmlTag("translation_table", "close")
        self.writeXmlNewline()

    def __write_xml_preamble(self):
        self.writeXmlTag("translation_table", "open")
        self.writeXmlNewline()
        for v in ["var", "varlab"]:
            self.write_line([self.xmlTagEnc(v, self.tt_export_dict[v])], 1)
        self.writeXmlNewline()

    def __write_svar_universe_xml_block(self):
        """write together the <univdisp> block."""

        # write the XML
        self.write_line([self.xmlTag("univdisp", "open")], 1)
        for r in self.tt_export_dict["universe"]:
            for k, v in r.items():
                self.write_line([self.xmlTagEnc(k, v)], 2)
        self.write_line([self.xmlTag("univdisp", "close")], 1)
        self.writeXmlNewline()

    def __write_universe_xml_block(self):
        """put together the <univdisp> block."""

        # write the xml
        for row in self.tt_export_dict["universe"]:
            self.write_line([self.xmlTag("univdisp", "open")], 1)
            for k, v in row.items():
                if k == "samps":
                    for samp in row[k]:
                        self.write_line([self.xmlTagEnc("samp", samp)], 2)
                else:
                    self.write_line([self.xmlTagEnc(k, v)], 2)
            self.write_line([self.xmlTag("univdisp", "close")], 1)
            self.writeXmlNewline()

    def __write_svar_codes_xml_block(self):
        """write the <code> block xml for each svar output code."""
        for row in self.tt_export_dict["codes"]:
            self.write_line([self.xmlTag("code", "open")], 1)
            for k, v in row.items():
                self.write_line([self.xmlTagEnc(k, v)], 2)

            self.write_line([self.xmlTag("code", "close")], 1)
            self.writeXmlNewline()

    def __write_codes_xml_block(self):
        """put together the <code> block for each output code."""
        for r in self.tt_export_dict["codes"]:
            self.write_line([self.xmlTag("code", "open")], 1)

            for t in [
                "id",
                "targetcode",
                "labelonly",
                "label",
                "indent",
                "syntax",
                "genlab",
                "indentgen",
                "codetype",
                "missing",
            ]:
                self.write_line([self.xmlTagEnc(t, r[t])], 2)
            self.write_line([self.xmlTag("code", "close")], 1)
            self.writeXmlNewline()

    def __write_svar_recode_xml_block(self):
        """put together the <recode> block for a given svar."""
        for r in self.tt_export_dict["sample"]["recode"]:
            self.write_line([self.xmlTag("recode", "open")], 2)
            for k, v in r.items():
                self.write_line([self.xmlTagEnc(k, v)], 3)
            self.write_line([self.xmlTag("recode", "close")], 2)

    def __write_svar_cols_xml_block(self):
        """put together the <col> block for a given svar."""
        # cols block
        sw = self.tt_export_dict["sample"]["cols"]
        self.write_line([self.xmlTag("cols", "open")], 2)
        for k, v in sw.items():
            self.write_line([self.xmlTagEnc(k, v)], 3)
        self.write_line([self.xmlTag("cols", "close")], 2)

    def __write_svar_samples_xml_block(self):
        """put together the <sample> block for a single svar."""

        self.write_line([self.xmlTag("sample", "open")], 1)

        for k, v in self.tt_export_dict["sample"].items():
            if k == "cols":
                self.__write_svar_cols_xml_block()
            elif k == "recode":
                self.__write_svar_recode_xml_block()
            else:
                self.write_line([self.xmlTagEnc(k, v)], 2)
        self.writeXmlNewline()

        self.write_line([self.xmlTag("sample", "close")], 1)
        self.writeXmlNewline()

    def __write_inner_samples_block(self, k, rows):
        """this is for <col> and <recode> blocks inside <sample>"""
        self.writeXmlNewline()
        for r in rows:
            self.write_line([self.xmlTag(k, "open")], 2)
            for inner_k, inner_v in r.items():
                self.write_line([self.xmlTagEnc(inner_k, inner_v)], 3)
            self.write_line([self.xmlTag(k, "close")], 2)

    def __write_samples_xml_block(self):
        """using tt_export_dict, write the <sample> block for all samples."""
        # use raw_samples() for co-homed metadata projects like IHIS/MEPS
        for r in self.tt_export_dict["samples"]:
            self.write_line([self.xmlTag("sample", "open")], 1)
            for k, v in r.items():
                if k == "recode" or k == "cols":
                    if len(v) > 0:
                        self.__write_inner_samples_block(k, v)
                else:
                    self.write_line([self.xmlTagEnc(k, v)], 2)
            self.write_line([self.xmlTag("sample", "close")], 1)
            self.writeXmlNewline()


class Exporter(ExportBase):
    def __init__(self, **kwargs):
        # NOTE: the __init__ should always be VERY lightweight.
        # Don't instantiate anything heavy.
        self.tt_export_dict = {}

        # from kwargs
        self.print_enabled = kwargs.get("print_enabled", True)
        self.encoding = kwargs.get("encoding", "utf8")

        super().__init__(**kwargs)

    def __tt_timestamp_file(self, sample):
        return Path(
            "/".join(
                [
                    self.product.project.path,
                    "metadata",
                    "trans_tables",
                    sample,
                    "sample_exported.txt",
                ]
            )
        )

    def console_print_now(self, msg):
        if self.print_enabled:
            sys.stdout.write(msg)
            sys.stdout.flush()

    def make_sample_timestamp_file(self, tt_exports, sample=None):
        """Create a timestamp file for comparison to metadata file.

        This method is an alias of make_sample_timestamp()

        Args:
            tt_exports(bool): True if TTs were exported, False if not.

        """
        warnings.warn(
            "make_sample_timestamp_file() will be deprecated "
            + "in future releases. use make_sample_timestamp() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.make_sample_timestamp(tt_exports, sample)

    def make_sample_timestamp(self, tt_exports, sample=None):
        """Create a timestamp file for comparison to metadata file.

        This method will both create a sample_exported.txt file in
        metadata/trans_tables/<sample> and update the sample_svar_tt_export
        table in the sqlite metadata.db file.

        Args:
            tt_exports(bool): True if TTs were exported, False if not.
            sample(str): Name of sample, else pull from Data Dictionary. Default: None

        """
        if sample:
            sample = sample.lower()
        else:
            sample = self.dd.sample.lower()

        tt_timestamp_file = self.__tt_timestamp_file(sample)

        # create the dir if necessary
        tt_timestamp_file.parent.mkdir(parents=True, exist_ok=True)

        # create/overwrite timestamp file
        with open(tt_timestamp_file, "w") as fh:
            fh.write("1")

        # now that we're here, let's update the DB!
        if not self.debug:
            dumper = SqliteMetadataDumper(
                product=self.product, verbose=self.verbose, db_file=self.db_file
            )
            if tt_exports:
                dumper.update_sample_trans_tables(sample)
            else:
                # if tt_exports is False, xml files are cruft
                xml_cruft = tt_timestamp_file.parent.glob("*.xml")
                [f.unlink() for f in xml_cruft]
            dumper.update_sample_svar_tt_export(sample=sample, tt_exports=tt_exports)

    def svars_need_export(self, ddpath, sample):
        """Decide if svars from a sample need export."""
        timestamp_file = self.__tt_timestamp_file(sample)

        # if the project publishes svars, we export unless everything fresh
        if self.product.project.publish_svars:
            # if the timestamp file isn't there, punt and say we need to export
            if not timestamp_file.exists():
                return True
            if utilities.needs_cache(ddpath.strip(), str(timestamp_file)):
                return True
            if self.force:
                return True
        # if the project does NOT publish svars, we don't export if all vars are NOREC
        else:
            if (
                self.force
                or (not timestamp_file.exists())
                or (utilities.needs_cache(ddpath.strip(), str(timestamp_file)))
            ):
                # if we don't have the dd, grab it now
                if not self.dd:
                    try_cache = True
                    if self.force:
                        try_cache = False
                    self.dd = self.product.dd(sample, try_cache=try_cache)
                if self.dd.has_recodes:
                    return True
        return False

    # def svars_need_export(self, ddpath, sample):
    #     timestamp_file = self.__tt_timestamp_file(sample)
    #     if (
    #         self.force
    #         or (not timestamp_file.exists())
    #         or (utilities.needs_cache(ddpath.strip(), str(timestamp_file)))
    #     ):
    #         if self.product.project.publish_svars:
    #             return True
    #         # if we don't have the dd, grab it now
    #         if not self.dd:
    #             try_cache = True
    #             if self.force:
    #                 try_cache = False
    #             self.dd = self.product.dd(sample, try_cache=try_cache)
    #         return True
    #     return False

    def usesRestrictedData(self):
        """boolean to determine if project uses restricted data."""
        if self.product.project.has_restricted_data:
            return True
        else:
            return False

    def usesAnchorForm(self):
        """boolean to determine if project uses anchor form fields."""
        if self.project.lower() == "usa":
            return True
        else:
            return False

    def usesSvarDoc(self):
        """boolean to determine if project uses svar_doc."""
        if self.project.lower() == "utah":
            return False
        else:
            return True

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

    def __get_univ_dict(self):
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

    def __populate_svar_universe_block(self):
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

    def __calculate_indent(self, cell):
        """calculate the amount of indent based on left padding."""
        label = cell.lstrip()
        indent = str(round((len(cell) - len(label)) / 3))
        return label, indent

    def __blank_to_zero(self, cell):
        """turn blank into zero."""
        if cell == "":
            return "0"
        else:
            return cell

    def __populate_svar_codes_block(self):
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
                    code["label"], code["indent"] = self.__calculate_indent(
                        d["VALUELABELSVAR"]
                    )
                    code["syntax"] = ""  # blank for svars
                    code["genlab"] = ""  # blank for svars
                    code["missing"] = ""  # missing is unneeded for svar TT
                    code["indentgen"] = "0"  # 0 for svars
                    code["codetype"] = str(d["CODETY"]).strip()
                    idx = idx + 1
                    self.tt_export_dict["codes"].append(code)

    def __populate_codes_info(self):
        seen = set()
        idx = 1
        output_values = self.tt.tt_output_values
        self.tt_export_dict["codes"] = []
        for key, row in output_values.items():
            row_data = {}
            code = str(key)
            targetcode = code
            if code not in seen:
                seen.add(code)
                if code.startswith("#"):
                    targetcode = ""
                row_data["id"] = str(idx)

                row_data["targetcode"] = targetcode
                row_data["labelonly"] = str(row["labelonly"])

                label, indent = self.__calculate_indent(row["label"])
                row_data["label"] = label
                row_data["indent"] = indent
                row_data["syntax"] = str(row["syntax"]).strip()

                genlab, indentgen = self.__calculate_indent(row["genlabel"])
                row_data["genlab"] = genlab
                row_data["indentgen"] = indentgen

                row_data["codetype"] = str(row["codetype"]).strip()
                row_data["missing"] = str(row["missing"]).strip()
                idx = idx + 1
                self.tt_export_dict["codes"].append(row_data)

    def __valid_value(self, val):
        if val == "*" or val == "" or val == "nan":
            return False
        return True

    def __populate_svar_recode_block(self):
        """put together the <recode> block for a given svar."""
        all_info = self.all_info
        recodes = []
        for d in all_info["values_and_freqs"]:
            row = {}
            try:
                val = str(d["VALUE"])
                targetcode = str(d["VALUESVAR"])
                if self.__valid_value(val) and not targetcode.startswith("~"):
                    row["orig"] = str(d["VALUE"])
                    row["targ"] = targetcode
                    row["lab"] = str(d["VALUELABEL"])
                    row["freq"] = str(d["FREQ"])
                    recodes.append(row)
            except KeyError:
                print(
                    "FATAL:",
                    self.svar.upper(),
                    "could not write recode block",
                    file=sys.stderr,
                )
        return recodes

    def __populate_recode_block(self, sample):
        """put together the recode block for a given sample."""
        input_codes = self.tt.sample_to_tt_input_codes(sample)
        code_table = self.tt.sample_to_tt_table(sample)
        input_codes = [x for x in input_codes if self.__valid_value(x)]
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
                print(
                    "FATAL:",
                    self.tt.variable,
                    "input code with no output code Sample:",
                    sample,
                    "Input Code:",
                    recode,
                    file=sys.stderr,
                )
        return recode_block

    def __populate_svar_cols_block(self):
        """put together the <col> block for a given svar."""
        # cols block
        sw = self.dd.svar_to_start_and_wid(self.svar.upper())
        start = sw["start"]
        end = start + sw["wid"] - 1

        return {"beg": str(start), "end": str(end)}

    def __populate_cols_block(self, sample):
        """put together the col block for a given sample."""
        # cols block
        col = self.tt.sample_to_col_loc(sample.upper())
        row = self.tt.rowLabels["COLUMNS"]
        col_string = self.tt.cell(row, col)

        cols_block = []

        if col_string != "":
            cols = col_string.split(";")
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

    def __cols_xml_block(self, sample):
        """put together the <col> block for a given sample."""
        # cols block
        col = self.tt.sample_to_col_loc(sample.upper())
        row = self.tt.rowLabels["COLUMNS"]
        col_string = self.tt.cell(row, col)

        if col_string != "":
            cols = col_string.split(";")
            cols = [x.strip() for x in cols]
            for col in cols:
                self.write_line([self.xmlTag("cols", "open")], 2)
                locs = col.split("=")
                locs = [x.strip() for x in locs]
                self.write_line([self.xmlTagEnc("beg", locs[0])], 3)
                if len(locs) == 1:
                    self.write_line([self.xmlTagEnc("end", locs[0])], 3)
                else:
                    self.write_line([self.xmlTagEnc("end", locs[1])], 3)
                self.write_line([self.xmlTag("cols", "close")], 2)

    def __populate_svar_samples_block(self):
        """put together the <sample> block for a single svar."""
        sample = self.dd.sample.lower()
        all_info = self.all_info

        self.tt_export_dict["sample"] = {}
        s = {}
        s["id"] = sample

        s["rectype"] = str(self.__blank_to_zero(all_info["SVAR_RECORDTYPE"]))
        s["norecode"] = str(self.__blank_to_zero(all_info["NOREC"]))
        s["univ"] = all_info["UNIVSVAR"]
        if "RESTRICTED" in self.all_info:
            s["restricted"] = all_info["RESTRICTED"]
        for tag in ["svar", "invar", "anchor_form", "svar_doc", "anchor_inst"]:
            s[tag] = ""

        s["cols"] = self.__populate_svar_cols_block()
        if s["norecode"] != "1":
            s["recode"] = self.__populate_svar_recode_block()

        self.tt_export_dict["sample"] = s

    def __populate_samples_block(self):
        """put together the <sample> block for all samples."""
        tt = self.tt
        # use raw_samples() for co-homed metadata projects like IHIS/MEPS
        self.tt_export_dict["samples"] = []
        for sample in tt.raw_samples():
            samp = {}

            samp["id"] = sample.lower()
            samp["rectype"] = str(tt.sample_to_rectype(sample))
            samp["norecode"] = str(tt.sample_to_norecode(sample))
            samp["hide"] = str(tt.sample_to_hide(sample))
            if self.usesRestrictedData():
                if tt.sample_to_proj6(sample) == "restricted":
                    samp["restricted"] = "restricted"

            if len(tt.sample_to_svars(sample)) > 0:
                svar_string = " ".join(tt.sample_to_svars(sample))
            else:
                svar_string = tt.sample_to_proj1(sample)
            samp["svar"] = svar_string

            # apparently no "iv" projects any more, so this is always ''
            # candidate for deprecation later
            samp["invar"] = ""

            if self.usesAnchorForm():
                anchor_form = str(tt.sample_to_proj4(sample))
                anchor_inst = str(tt.sample_to_proj5(sample))
            else:
                anchor_form = ""
                anchor_inst = ""
            samp["anchor_form"] = anchor_form

            if self.usesSvarDoc():
                svar_doc = tt.sample_to_proj2(sample)
            else:
                svar_doc = ""
            samp["svar_doc"] = svar_doc
            samp["anchor_inst"] = anchor_inst

            try:
                univ = tt.sample_to_univ(sample)
            except KeyError:
                univ = "UNKNOWN!"
            samp["univ"] = univ

            # cols block
            samp["cols"] = self.__populate_cols_block(sample)

            # recode block
            if samp["norecode"] != "1":
                samp["recode"] = self.__populate_recode_block(sample)

            # add this samp to export_dict
            self.tt_export_dict["samples"].append(samp)

    def __integrated_variable_export_info(self):
        """Populate tt_export_dict with integrated variable information."""
        self.tt_export_dict["var"] = self.tt.variable
        self.tt_export_dict["varlab"] = self.tt.variable_label

        # populate the universe part of self.tt_export_dict
        self.__get_univ_dict()
        # populate the codes part of self.tt_export_dict
        self.__populate_codes_info()
        # populate samples block of self.tt_export_dict
        self.__populate_samples_block()

    def __get_tt(self, variable):
        try_cache = True
        if self.force:
            try_cache = False
        self.tt = self.product.tt(variable.lower(), try_cache=try_cache)

    def __get_samples(self):
        self.samples = self.product.samples

    def integrated_variable_tt_export(
        self, variable, output_file, path_constant="xml_metadata_trans"
    ):
        """This method gathers all information for a TT to be exported.

        It is currently used by integrated_variable_tt_to_xml, but could be
        used by other methods, perhaps exporting to different file formats.

        Args:
            variable(str): name of integrated variable to export
            output_file(str): name of export file

        Return:
            bool: True if file should be exported, False if current
        """
        if self.debug:
            self.output_file = Path("/tmp/metadata/trans_tables") / output_file
        else:
            self.output_file = Path(
                "/".join(
                    [
                        self.tt.project.path,
                        self.constants.scalar_constant(path_constant),
                        "integrated_variables",
                        output_file,
                    ]
                )
            )
        if not Path.exists(self.output_file.parent):
            Path.mkdir(self.output_file.parent, parents=True)
            self.output_file.parent.chmod(0o777)

        if self.force or utilities.needs_cache(self.tt.xlpath, str(self.output_file)):
            # gather the information
            if not self.dryrun:
                self.__integrated_variable_export_info()
            return True
        return False

    def integrated_variable_tt_to_json(self, variable=None, tt_stem=None):
        """Dump a TranslationTable out to JSON.

        Args:
            variable(str): name of integrated variable to export
            tt_stem(str): if given, dump the JSON out to that name instead.
                          Used when display_variable and variable from control
                          file differ (not common).
        Returns:
            str: '*' if file freshly exported, '.' if file export current
        """

        self.__get_tt(variable)

        p = Path(self.tt.xlpath)
        if tt_stem:
            f = tt_stem + "_tt.json"
        else:
            f = p.stem + ".json"

        needs_export = self.integrated_variable_tt_export(
            variable=variable, output_file=f
        )
        if self.force or needs_export:
            if not self.dryrun:
                # create an ExportJson object
                exportJson = ExportJsonFile(
                    tt_export_dict=self.tt_export_dict,
                    output_file=self.output_file,
                    encoding=self.encoding,
                )
                # write it out
                exportJson.export_tt()

            return "*"
        else:
            return "."

    def integrated_variable_tt_to_user_csv(self, variable=None, tt_stem=None):
        """Dump a TranslationTable out to User CSV

        Args:
            variable (str, optional): name of integrated variable to export. Defaults to None.
            tt_stem (str, optional): if given, dump the csv out to that name instead.
                                     Used when display_variable and variable from control
                                     file differ (not common). Defaults to None.
        """
        if not self.product.project.publish_user_trans_tables:
            return "*"
        self.__get_tt(variable)

        p = Path(self.tt.xlpath)
        if tt_stem:
            f = tt_stem + "_tt.csv"
        else:
            f = p.stem + ".csv"

        needs_export = self.integrated_variable_tt_export(
            variable=variable,
            output_file=f,
            path_constant="csv_metadata_user_trans_tables",
        )
        if self.force or needs_export:
            if not self.dryrun:
                # create an exportUserCsv object
                self.__get_samples()
                exportUserCsv = ExportUserCsvFile(
                    tt=self.tt,
                    samples=self.samples,
                    output_file=self.output_file,
                    encoding=self.encoding,
                )
                # write it out
                exportUserCsv.export_tt()

            return "*"
        else:
            return "."

    def integrated_variable_tt_to_xml(self, variable=None, tt_stem=None):
        """Dump a TranslationTable out to XML.

        Args:
            variable(str): name of integrated variable to export
            tt_stem(str): if given, dump the XML out to that name instead.
                          Used when display_variable and variable from control
                          file differ (not common).
        Returns:
            str: '*' if file freshly exported, '.' if file export current
        """

        self.__get_tt(variable)

        p = Path(self.tt.xlpath)
        if tt_stem:
            f = tt_stem + "_tt.xml"
        else:
            f = p.stem + ".xml"

        needs_export = self.integrated_variable_tt_export(
            variable=variable, output_file=f
        )
        if self.force or needs_export:
            if not self.dryrun:
                # create an ExportXml object
                exportXml = ExportXmlFile(
                    tt_export_dict=self.tt_export_dict,
                    output_file=self.output_file,
                    encoding=self.encoding,
                )
                # write it out
                exportXml.export_tt(tt_type="integrated")

            status = "*"
        else:
            status = "."
        csv_status = self.integrated_variable_tt_to_user_csv(variable, tt_stem)
        if csv_status == "*" and status == "*":
            return "*"
        else:
            return "."

    def _variable_desc_to_xml(
        self,
        path_or_variable=None,
        debug=False,
        outfile=None,
        variable_type="integrated",
    ):
        """Dump a Variable Description Doc out to XML."""
        if variable_type == "integrated":
            self.vd = self.product.var_desc(path_or_variable.lower())
        else:
            self.vd = self.product.var_desc(path_or_variable)

        if outfile:
            self.output_file = outfile
        else:
            p = Path(self.vd.wordpath)
            f = p.stem + ".xml"

            if self.debug:
                self.output_file = Path("/tmp/metadata/var_descs") / f
            else:
                self.output_file = Path(
                    "/".join(
                        [self.vd.project.path, self.constants.xml_metadata_vardesc, f]
                    )
                )

        needs_export = utilities.needs_cache(self.vd.wordpath, str(self.output_file))
        if self.force or needs_export:
            if not self.dryrun:
                self.vd.run_audit(audit_level="fail")
                exportXml = ExportXmlFile(
                    xml_block=self.vd.text,
                    output_file=self.output_file,
                    encoding=self.encoding,
                )
                exportXml.export_vd()
            return "*"
        else:
            return "."

    def integrated_variable_desc_to_xml(self, variable=None, debug=False):
        return self._variable_desc_to_xml(
            path_or_variable=variable, debug=debug, variable_type="integrated"
        )

    def source_variable_desc_to_xml(self, infile=None, outfile=None, debug=False):
        return self._variable_desc_to_xml(
            path_or_variable=str(infile),
            debug=debug,
            outfile=str(outfile),
            variable_type="source",
        )

    def export_doc_to_file(self, infile=None, outfile=None, debug=False):
        """Dump the text from a word document out to a file path."""
        self.doc = MpcDocument(str(infile))
        self.output_file = outfile
        if self.force or utilities.needs_cache(str(infile), str(outfile)):
            if not self.dryrun:
                exportXml = ExportXmlFile(
                    output_file=self.output_file,
                    xml_block=self.doc.text,
                    encoding=self.encoding,
                )
                exportXml.export_vd()
            return "*"
        else:
            return "."

    def __svar_tt_export_info(self, svar=None):
        """Assemble all the svar data for tt_export_dict."""
        self.tt_export_dict["var"] = self.svar.upper()
        self.tt_export_dict["varlab"] = str(self.dd.svar_to_label(self.svar))

        # self.all_info is used by the populate* methods below it
        self.all_info = self.dd.svar_to_all_info(self.svar)
        self.__populate_svar_universe_block()
        self.__populate_svar_codes_block()
        self.__populate_svar_samples_block()

    def svar_tt_export(self, svar, output_file):
        """This method gathers all information for an svar TT to be exported.

        It is currently used by svar_tt_to_xml, but could be
        used by other methods, perhaps exporting to different file formats.

        Args:
            svar(str): name of source variable to export
            output_file(str): name of file which to export source var TT
        Returns:
            bool: True if file should be exported, False if file is current.
        """
        self.svar = svar

        if self.debug:
            self.output_file = Path("/tmp/metadata/trans_tables") / output_file
        else:
            self.output_file = (
                Path(self.dd.project.path)
                / "metadata"
                / "trans_tables"
                / self.dd.sample.lower()
                / output_file
            )
        if not Path.exists(self.output_file.parent):
            Path.mkdir(self.output_file.parent, parents=True)
            self.output_file.parent.chmod(0o777)
            if self.debug:
                self.output_file.parent.parent.chmod(0o777)

        if self.force or utilities.needs_cache(self.dd.xlpath, str(self.output_file)):
            self.__svar_tt_export_info()
            return True
        return False

    def svar_tt_to_json(self, svar):
        """Dump an svar from a DataDictionary object out to XML."""
        f = svar.upper() + "_tt.json"

        needs_export = self.svar_tt_export(svar=svar, output_file=f)
        if needs_export or self.force:
            if not self.dryrun:
                exportJson = ExportJsonFile(
                    output_file=self.output_file,
                    tt_export_dict=self.tt_export_dict,
                    encoding=self.encoding,
                )
                exportJson.export_tt()
            return "*"
        else:
            return "."

    def svar_tt_to_xml(self, svar=None):
        """Dump an svar from a DataDictionary object out to XML."""
        f = svar.upper() + "_tt.xml"
        needs_export = self.svar_tt_export(svar=svar, output_file=f)
        if self.force or needs_export:
            if not self.dryrun and self.product.project.publish_svars:
                exportXml = ExportXmlFile(
                    output_file=self.output_file,
                    tt_export_dict=self.tt_export_dict,
                    encoding=self.encoding,
                )
                exportXml.export_tt(tt_type="svar")
            return "*"
        else:
            return "."
