import logging
from ipums.metadata.errors import MetadataError
from pathlib import Path
import os
import sys
import re
from glob import glob
import datetime
from collections import namedtuple
from joblib import Parallel, delayed
from .sql_data_manager import SqlDataManager
from .sqlite_connection_manager import SqliteConnectionManager

from .tables.dynamic_tables import DYNAMIC_PRIMARY_KEY_MAPPINGS
from .tables.database_table import DatabaseTable
from .tables.sample_svar_tt_export_table import SampleSvarTTExportTable
from .tables.user_tts_table import UserTTsTable
from .tables.tt_samplevariables_table import TTSamplevariablesTable
from .tables.tt_samplevariables_sources import TTSamplevariablesSources
from .tables.tt_samplevariables_recodings import TTSamplevariablesRecodings
from .tables.tt_variable_labels import TTVariableLabels
from .tables.tt_variable_universe_displayids import TTVariableUniverseDisplayids
from .tables.var_desc_table import VarDescTable
from .tables.variable_tts_table import VariableTTsTable
from .tables.tt_variable_universe_display_id_samples import (
    TTVariableUniverseDisplayIdSamples,
)
from .tables.tt_last_updated import TTLastUpdated
from .tables.enum_materials_table import EnumMaterialsTable
from .tables.web_documents_table import WebDocumentsTable
from .tables.insert_html_table import InsertHtmlTable
from .tables.project_table import ProjectTable
from .tables.samplevariable_editing_rules_table import SamplevariableEditingRulesTable

from .tables.input_data_variable_info import (
    InputDataVariableInformationTable,
    InputDataVariableValueTable,
)

import pandas as pd

from ipums.metadata import utilities, DataDictionary
from ipums.metadata.exporters import ExportTransTableData

log = utilities.setup_logging(__name__)

#######################################################################################
#
# These are the relational tables
#
#    tt_samplevariables (1 to many relationship with tt_samplevariables_recodings)
#    tt_samplevariables_sources (many to 1 relationship with tt_samplevariables records)
#    tt_samplevariables_recodings ("atomic unit"; many to 1 relationship
#                                   with tt_samplevariables_recodings)
#    tt_variables_labels (1 to many relationship
#                               with tt_samplevariables_recodings;
#                               many to 1 relationship with variables)
#    tt_variables_universedisplayids (1 to many relationship
#                                    with tt_samplevariables)
#    tt_variable_universedisplayid_samples (linking table between
#               tt_variables_universedisplayids and tt_samplevariables)
#    tt_last_updated timestamp records of when sample or variable was last updated
#
#######################################################################################


TT_TABLES = [
    "tt_samplevariables_sources",
    "tt_samplevariables_recodings",
    "tt_variable_labels",
    "tt_samplevariables",
    "tt_variable_universedisplayids",
    "tt_variable_universedisplayid_samples",
]

VARIABLE_SAMPLE_COLUMN_TABLES = [t for t in TT_TABLES if "sample" in t]
VARIABLE_COLUMN_TABLES = list(set(TT_TABLES) - set(VARIABLE_SAMPLE_COLUMN_TABLES))


class SqliteMetadataDumper(object):
    """Class to dump metadata from a project to a sqlite db."""

    def __init__(
        self, product=None, db_file=None, verbose=False, encoding="utf8", wipe=None
    ):
        self.product = product
        self.project = self.product.project
        self.no_sqlite = self.project.no_sqlite
        self.samples = self.product.samples
        self.constants = self.product.constants
        self.verbose = verbose
        if self.verbose:
            log.setLevel(logging.INFO)
        # XXX these hard-coded paths are BAD. These should come from self.constants
        self.cf_dir = Path(self.project.path) / "metadata" / "control_files"
        self.em_dir = Path(self.project.path) / "metadata" / "enum_materials"
        self.tt_dir = Path(self.project.path) / "metadata" / "trans_tables"
        self.vd_dir = Path(self.project.path) / "metadata" / "var_descs"
        self.wd_dir = Path(self.project.path) / "metadata" / "web_docs"
        self.ih_dir = Path(self.project.path) / "metadata" / "insert_html"
        self.er_dir = (
            Path(self.constants.xml_metadata_editing_rules) / "stata_variables_to_edit"
        )

        self.user_tt_dir = (
            Path(self.project.path) / "metadata" / "user_trans_table_csvs"
        )

        if db_file:
            self.db_path = Path(db_file)
        else:
            self.db_path = Path(self.project.path) / "metadata" / "metadata.db"

        self.now = str(datetime.datetime.now())

        # create an object to manage DELETE and INSERT statements
        # this needs to be separate for the two fundamental "units" of
        # SQL updates: sample, and integrated_vars
        # Which is to say: if we're caching SQL updates to be run en masse
        # after e.g. parallelized info gathering, the block units of those
        # queries, including DELETE statements that wrap the INSERT statements,
        # are a) a set of integrated variables (1-N TT documents),
        #  or b) a sample (1 Data Dictionary)
        self.samp_mgr = SqlDataManager()
        self.ivar_mgr = SqlDataManager()

        self.sqliteObj = SqliteConnectionManager(self.db_path)
        if not wipe:
            self.sqliteObj.check_project_match(self.project.name)

        # Tables in the database
        self.em_table = EnumMaterialsTable()
        self.tt_table = VariableTTsTable()
        self.wd_table = WebDocumentsTable()
        self.ih_table = InsertHtmlTable()
        self.sample_svar_tt_export_table = SampleSvarTTExportTable()
        self.project_table = ProjectTable()
        self.user_tt_table = UserTTsTable()
        self.editing_rule_table = SamplevariableEditingRulesTable()

        # TT tables in the database
        self.relational_tt_tables = [
            TTSamplevariablesTable(),
            TTSamplevariablesSources(),
            TTSamplevariablesRecodings(),
            TTVariableUniverseDisplayids(),
            TTVariableUniverseDisplayIdSamples(),
            TTVariableLabels(),
            TTLastUpdated(),
        ]

        self.idvi_table = InputDataVariableInformationTable()
        self.idvv_table = InputDataVariableValueTable()

        self._get_tt_subdirectories()
        self.encoding = encoding
        self.debug = False
        self.test_sample = None

    # get all svar xml files
    def _get_tt_subdirectories(self):
        if not self.tt_dir.exists():
            log.warning(f"{self.tt_dir} does not exist!")
            return
        ttdir = str(self.tt_dir)
        subdirs = [
            name
            for name in os.listdir(ttdir)
            if os.path.isdir(os.path.join(ttdir, name))
            and (name in self.samples.raw_samples or name == "integrated_variables")
        ]
        subdirs.sort()
        self.ttdirs = subdirs

    def _editing_rule_rowdata(self, editing_rule):
        """Given an editing rule, return row data for every sample.
        (even if sample does not exist in the given variable)

        Args:
            editing_rule (str): Editing rule name

        Returns:
            list(tuples): tuples contain row data to be inserted into the editing
                          rules table
        """
        editing_rule = self.product.stata_editing_rule(editing_rule)
        rows = []
        for s in self.samples.all_samples:
            sample_specific_syntax = editing_rule.for_sample(s)
            if not sample_specific_syntax:
                continue
            has_get_source_data_call = 1 if editing_rule.has_get_source_data_call else 0
            rows.append(
                (
                    s.lower(),
                    editing_rule.name.upper(),
                    sample_specific_syntax,
                    editing_rule.order,
                    has_get_source_data_call,
                    ", ".join(editing_rule.ids_for_sample(s)),
                    datetime.datetime.now(),
                )
            )
        return rows

    def _inserts_for_editing_rules(self, files):
        """Given a list of editing rules, returns list of tuples representing row data

        Args:
            files (list(Path)): List of editing rule paths.

        Raises:
            FileNotFoundError: One or more provided files does not exist.

        Returns:
            list(tuple(str*)): list of tuples containing editing rule rowdata
        """

        for file in files:
            missing_files = []
            try:
                assert file.exists()
            except AssertionError:
                missing_files.append(file)
        if missing_files:
            missing_files = "\n".join(missing_files)
            raise FileNotFoundError(
                f"Could not find the following files: \n{missing_files}."
            )

        inserts = []
        for f in files:
            inserts.extend(self._editing_rule_rowdata(str(f)))
        return inserts

    def _inserts_for_one_group(self, table_name, group, group_list=None):
        is_svar = 1
        if group == "integrated_variables":
            is_svar = 0

        group_inserts = []
        if (is_svar == 1) and (table_name == "variable_descriptions"):
            true_file_suffix = ".xml"
            file_suffix = "_desc.xml"
            base_dir = self.vd_dir
        elif table_name == "variable_descriptions":
            file_suffix = "_desc.xml"
            base_dir = self.vd_dir
        elif table_name == "variable_trans_tables":
            file_suffix = "_tt.xml"
            base_dir = Path(self.tt_dir / group)
        elif table_name == "user_tts":
            file_suffix = "_tt.csv"
            base_dir = Path(self.user_tt_dir / group)

        if is_svar == 1:
            if (group == "source_variables") and group_list:
                file_name = Path(group_list[0]).stem
                pattern = r"_\d\d\d+_(desc|tt)"
                capt = re.search(pattern, file_name)
                if capt:
                    samp = file_name.replace(capt.group(0), "")
                    svarstem = self.project.sample_to_svarstem(samp)
                else:
                    raise ValueError(
                        f"FATAL: Sample could not be intuited from {file_name}"
                    )
            else:
                svarstem = self.project.sample_to_svarstem(group)

        if (is_svar == 1) and (table_name == "variable_descriptions"):
            files = [str(Path(g).stem) + true_file_suffix for g in group_list]
            paths = [base_dir / f for f in files]
        elif group_list:
            files = [g + file_suffix for g in group_list]
            paths = [base_dir / f for f in files]
        else:
            paths = [Path(f) for f in glob(str(base_dir / f"*{file_suffix}"))]

        for xml_file in paths:
            variable = xml_file.name[: -len(file_suffix)].upper()

            if not is_svar or (is_svar and variable.startswith(svarstem)):
                try:
                    if self.debug:
                        if table_name == "variable_trans_tables":
                            xml = ("n/a", "n/a")
                        else:
                            xml = ("n/a",)
                        xml_mtime = 0
                    else:
                        xml_utf = self._read_xml_file(xml_file)
                        if table_name == "variable_trans_tables":
                            xml = (xml_utf.encode("latin1", errors="replace"), xml_utf)
                        else:
                            xml = (xml_utf,)
                        xml_mtime = self._get_file_mtime(xml_file)
                    # XXX add xml_utf after xml when we get it in the schema
                    group_inserts.append(
                        (
                            variable,
                            group,
                            is_svar,
                            *xml,
                            datetime.datetime.now(),
                            xml_mtime,
                        )
                    )
                except UnicodeDecodeError as e:
                    log.critical("%s has an encoding problem", xml_file.name)
                    log.critical(str(e))
                    raise
        return group_inserts

    def _inserts_docs(self, base_dir, doc_list=None, file_suffix=".xml"):
        """Get the info for the documents to insert into their database table.

        Args:
            base_dir (Path): Path object where exported documents reside.
            doc_list (list[str], optional): list of documents. Defaults to None.
            file_suffix (str, optional): expected document file suffix. Defaults to ".xml".

        Returns:
            list[tuple(str, str, datetime, datetime)] : list of tuples describing docs to be inserted
        """
        doc_inserts = []

        if doc_list:
            files = [str(Path(g).stem) + file_suffix for g in doc_list]
            paths = [base_dir / f for f in files]
        else:
            paths = [Path(f) for f in glob(str(base_dir / f"*{file_suffix}"))]
        for xml_file in paths:
            doc_name = xml_file.name
            stripped_name = doc_name[: -len(file_suffix)].upper()
            try:
                if self.debug:
                    xml = "n/a"
                    xml_mtime = 0
                else:
                    xml_utf = self._read_xml_file(xml_file)
                    xml = xml_utf.encode("latin1", errors="replace")
                    xml_mtime = self._get_file_mtime(xml_file)
                    # XXX add xml_utf after xml when we get it in the schema
                    if file_suffix == ".html":
                        rowdata = (
                            stripped_name,
                            doc_name,
                            xml_utf,
                            datetime.datetime.now(),
                            xml_mtime,
                        )
                    else:
                        rowdata = (
                            stripped_name,
                            xml,
                            datetime.datetime.now(),
                            xml_mtime,
                        )
                    doc_inserts.append(rowdata)
            except UnicodeDecodeError as e:
                log.critical("%s has an encoding problem", xml_file.name)
                log.critical(str(e))
                raise MetadataError(
                    f"{xml_file.name} has an encoding problem:\n {str(e)}"
                ) from e

        return doc_inserts

    def _generate_sample_inserts(self):
        """Generator for compiling batches of insert data."""
        # limit self.ttdirs if necessary
        if self.test_sample:
            if self.test_sample == "integrated":
                self.ttdirs = ["integrated_variables"]
            else:
                self.ttdirs = [self.test_sample.lower()]

            for i, ttdir in enumerate(self.ttdirs, start=1):
                self._infolog(i, "/", len(self.ttdirs), ttdir)
                yield self._inserts_for_one_group(self.tt_table.name, ttdir)

    def _delete_variable_rows(self, table, var):
        return f"DELETE from {table} where variable = '{var}'"

    def _delete_variable_sample_rows(self, table, sample, var=None, svars_only=False):
        if var is not None:
            return (
                f"DELETE from {table} where sample = '{sample}' and variable = '{var}'"
            )
        else:
            if svars_only:
                return f"DELETE from {table} where sample = '{sample}' AND is_svar = 1"
            else:
                return f"DELETE from {table} where sample = '{sample}'"

    def _read_xml_file(self, f):
        """Reads and returns text in file."""
        with open(str(f), encoding="utf-8") as infile:
            xml_data = infile.read()
            # xml_data = xml_data.decode("latin1")
            # xml_data = xml_data.encode(self.encoding, errors="replace")
        return xml_data

    def _create_variable_trans_tables_tables(self):
        """Creates the trans_table schema"""
        print("Creating variable trans table tables")
        with self.sqliteObj.connect_via_sqlite() as con:
            if not self.tt_table.exists(self.tt_table.name, con):
                self.tt_table.create_in_db(con)
            for relational_tt_table in self.relational_tt_tables:
                if not relational_tt_table.exists(relational_tt_table.name, con):
                    relational_tt_table.create_in_db(con)

    def _create_sample_svar_tt_export(self):
        with self.sqliteObj.connect_via_sqlite() as con:
            self.sample_svar_tt_export_table.create_in_db(con)

    def _create_variable_descriptions_table(self):
        """Creates the variable_descriptions table schema"""
        vd_table = VarDescTable()
        with self.sqliteObj.connect_via_sqlite() as con:
            vd_table.create_in_db(con)

    def _infolog(self, *msg_parts):
        if self.verbose:
            msg = " ".join([str(x) for x in msg_parts])
            log.info(msg)

    def _create_cf_table(self, cf):
        self._infolog("Creating", cf, "table")
        csv_file = cf + ".csv"
        f = self.cf_dir / csv_file
        try:
            df, dtypes = self._prep_cf_data_frame(f)
        except FileNotFoundError:
            log.info("%s listed as CF but does not exist. No action taken.", cf)
            return
        with self.sqliteObj.connect_via_sqlite() as con:
            con.execute(f"DROP TABLE IF EXISTS {cf}")
            if cf in DYNAMIC_PRIMARY_KEY_MAPPINGS:
                create_table = pd.io.sql.get_schema(
                    df, cf, con=con, keys=DYNAMIC_PRIMARY_KEY_MAPPINGS[cf]
                )
                con.execute(create_table)
                df.to_sql(cf, con, if_exists="append", index=False, dtype=dtypes)
            else:
                if not "id" in df.columns:
                    df = df.reset_index().rename(columns={"index": "ID"})
                create_table = pd.io.sql.get_schema(df, cf, con=con, keys=["ID"])
                con.execute(create_table)
                df.to_sql(
                    cf,
                    con,
                    if_exists="append",
                    index=False,
                    dtype=dtypes.update({"ID": "INTEGER"}),
                )
            if cf == "samples":
                index = "CREATE INDEX sample_table_idx ON samples (sample)"
                con.execute(index)

    def _get_file_mtime(self, f):
        t = os.path.getmtime(str(f))
        mt = datetime.datetime.fromtimestamp(t)
        return mt.strftime("%Y-%m-%d %H:%M:%S.%f")

    def _prep_cf_data_frame(self, f):
        df = pd.read_csv(str(f), dtype=str, encoding=self.encoding)
        df.columns = map(str.lower, df.columns)
        df.columns = [col.replace(" ", "_") for col in df.columns]
        df = df.loc[:, ~df.columns.str.contains("^unnamed")]
        dtypes = {k: "varchar" for k in df.columns}
        df["date_created"] = datetime.datetime.now()
        dtypes["date_created"] = "TIMESTAMP"
        df["file_timestamp"] = self._get_file_mtime(f)
        dtypes["file_timestamp"] = "str"
        return df, dtypes

    def _read_and_enter_variables_csv_data(self, con, f, if_exists_action):
        df, dtypes = self._prep_cf_data_frame(f)
        if if_exists_action == "replace":
            con.execute(f"DROP TABLE IF EXISTS variables")
            create_table = pd.io.sql.get_schema(
                df, "variables", con=con, keys=["variable"]
            )
            con.execute(create_table)
        df.to_sql("variables", con, if_exists="append", index=False, dtype=dtypes)

    def _create_empty_database(self):
        """Deletes database if exists and creates empty one."""
        if self.db_path.exists():
            self._infolog("Deleting existing database...")
            os.remove(str(self.db_path.resolve()))

        self._create_variable_trans_tables_tables()
        self._create_variable_descriptions_table()
        with self.sqliteObj.connect_via_sqlite() as con:
            for table in (
                self.em_table,
                self.wd_table,
                self.ih_table,
                self.user_tt_table,
                self.project_table,
                self.editing_rule_table,
            ):
                self._infolog(
                    f'Creating the "{table.name}" table for {table.description}'
                )
                table.create_in_db(con)
        # add project to project table
        self._add_project_to_db()

    def _add_project_to_db(self):
        proj_insert = f"INSERT into project(project, date_created) values ('{self.project.name}', '{self.now}')"
        with self.sqliteObj.connect_via_sqlite() as con:
            con.execute(proj_insert)

    def _check_for_duplicates(self, info):
        variables = [x[0] for x in info]
        dups = set([x for x in variables if variables.count(x) > 1])
        if dups:
            log.warning("Duplicates found: %s", dups)

    def _nosqlite(self):
        if self.no_sqlite:
            log.warning(self.product.name + " is not configured to use sqlite")
            return True
        return False

    def create_fresh_database(self):
        """Creates empty database, fills variables_trans_tables table."""
        if self._nosqlite():
            return

        self._create_empty_database()

        # xml trans table population
        for idx, info in enumerate(self._generate_sample_inserts()):
            with self.sqliteObj.connect_via_sqlite() as con:
                insert_string = self.tt_table.sql_cmd_to_insert()
                con.executemany(insert_string, info)

        # XXX what to do about relational TT data in this method?
        # XXX what to do about control files in this method?

    def create_temp_database(self, test_sample=None):
        """Creates a temp database for debugging.

        Does not slurp xml files, which speeds up record
        creation for debug purposes. Temp database is
        at /tmp/temp.db.
        """
        if self._nosqlite():
            return
        self.test_sample = test_sample
        self.debug = True
        self.db_path = Path("/tmp/temp.db")
        self.create_fresh_database()

    def create_variables_table(self):
        """Wipes and recreates variables table from control file data."""

        if self._nosqlite():
            return
        self._infolog("Creating variables table")

        with self.sqliteObj.connect_via_sqlite() as con:
            integrated_csv = self.cf_dir / "integrated_variables.csv"
            self._read_and_enter_variables_csv_data(con, integrated_csv, "replace")

            svars_folder = self.cf_dir / "svars_csv"
            csvfiles = glob(str(svars_folder / "*.csv"))
            paths = [Path(f) for f in csvfiles]
            paths = [
                p
                for p in paths
                if p.stem.replace("_svars", "") in self.samples.all_samples
            ]
            paths.sort()
            for f in paths:
                self._read_and_enter_variables_csv_data(con, f, "append")

            con.execute("CREATE INDEX variables_idx ON variables (variable)")
            con.execute("CREATE INDEX samples_idx ON variables (sample)")
            con.execute("CREATE INDEX svar_idx ON variables (svar)")

    def create_all_cf_tables(self):
        """Wipes and recreates every cf table listed in constants.xlsx."""

        if self._nosqlite():
            return
        c = self.constants
        files = list(c.ws[c.ws["CONSTANT"] == "metadata_control_files"]["VALUE"])
        names = [name.replace("metadata/", "").replace(".xlsx", "") for name in files]
        for cf in names:
            # variable table created with separate method
            if cf != "variables":
                self._create_cf_table(cf)

    def create_cf_table(self, cf):
        """Wipes and recreates generic cf table."""

        if self._nosqlite():
            return
        if cf == "variables":
            self.create_variables_table()
        else:
            self._create_cf_table(cf)

    def update_sample_control_file_data(self, sample):
        """Update the control file data for an individual sample."""

        sample = sample.lower()

        if self._nosqlite():
            return
        self._infolog(f"updating {sample} in variables table")

        with self.sqliteObj.connect_via_sqlite() as con:
            table_exists = DatabaseTable.exists("variables", con)
            # this should not typically happen
            if not table_exists:
                self.create_variables_table()

            svars_folder = self.cf_dir / "svars_csv"
            csv_file = svars_folder / f"{sample}_svars.csv"
            if csv_file.exists():
                # first delete them all
                con.execute(f'DELETE from variables where lower(sample) = "{sample}"')
                # then insert the new set
                self._read_and_enter_variables_csv_data(con, csv_file, "append")
            else:
                print(str(csv_file), "does not exist, DB not updated", file=sys.stderr)

    def update_sample_trans_tables(self, sample):
        """Update the trans table for an individual sample."""

        if self._nosqlite():
            return
        self._infolog("updating", sample, "in variable_trans_tables")

        with self.sqliteObj.connect_via_sqlite() as con:
            table_exists = DatabaseTable.exists("variable_trans_tables", con)
            if not table_exists:
                self._create_variable_trans_tables_tables()

            # first delete them all
            delete = 'DELETE from variable_trans_tables where sample = "' + sample + '"'
            con.execute(delete)
            # then insert the new set

            insert_string = self.tt_table.sql_cmd_to_insert(name_placeholders=True)

            info = self._inserts_for_one_group(self.tt_table.name, sample)
            seq_of_param_dicts = []
            for param_tuple in info:
                var, group, is_svar, xml, xml_utf8, date_created, file_ts = param_tuple
                param_dict = dict(
                    variable=var,
                    sample=group,
                    is_svar=is_svar,
                    xml=xml,
                    xml_utf8=xml_utf8,
                    date_created=date_created,
                    file_timestamp=file_ts,
                )
                seq_of_param_dicts.append(param_dict)
            con.executemany(insert_string, seq_of_param_dicts)

    def update_sample_svar_tt_export(self, sample, tt_exports=True):
        """Updates sample_svar_tt_export table for a sample."""

        if self._nosqlite():
            return

        has_tt_exports = 0
        if tt_exports:
            has_tt_exports = 1

        with self.sqliteObj.connect_via_sqlite() as con:
            table_exists = DatabaseTable.exists("sample_svar_tt_export", con)
            if not table_exists:
                self._create_sample_svar_tt_export()
            timestamp = datetime.datetime.now()
            insert_string = f'INSERT or REPLACE into \
                            sample_svar_tt_export \
                            (sample, has_tt_exports, date_created) \
                            VALUES("{sample}", "{has_tt_exports}", "{timestamp}")'

            con.execute(insert_string)

            # if has_tt_exports is False, we should delete cruft
            # from variable_trans_tables if it exists
            if not has_tt_exports and DatabaseTable.exists(self.tt_table.name, con):
                delete_string = (
                    f'DELETE from variable_trans_tables where sample = "{sample}"'
                )
                con.execute(delete_string)

    def _update_integrated_variable_tables(self, table_name, variable_list):
        """Update integrated variable tables.

        Requires a list of integrated variables to update.
        """

        if self._nosqlite():
            return
        self._infolog(f"updating integrated vars in {table_name}")
        with self.sqliteObj.connect_via_sqlite() as con:
            table_exists = DatabaseTable.exists(table_name, con)
            if not table_exists:
                if table_name == "variable_descriptions":
                    self._create_variable_descriptions_table()
                elif table_name == self.tt_table.name:
                    self._create_variable_trans_tables_tables()
            if "trans_tables" in table_name:
                cols = [
                    "variable",
                    "sample",
                    "is_svar",
                    "xml",
                    "xml_utf8",
                    "date_created",
                    "file_timestamp",
                ]
            else:
                cols = [
                    "variable",
                    "sample",
                    "is_svar",
                    "xml",
                    "date_created",
                    "file_timestamp",
                ]
            wildcards = ["?" for t in cols]
            insert_string = f"INSERT or REPLACE into \
                             {table_name} \
                             ({', '.join(cols)}) \
                             VALUES({', '.join(wildcards)})"
            info = self._inserts_for_one_group(
                table_name, "integrated_variables", group_list=variable_list
            )
            con.executemany(insert_string, info)
        # if the project publishes user TT csvs (ipumsi), do that now too
        self._update_user_tts(variable_list)

    def _update_user_tts(self, variable_list):
        """Update user_tts table

        Requires a list of integrated variables to update.
        """

        if self._nosqlite():
            return

        if not self.project.publish_user_trans_tables:
            return

        table_name = "user_tts"
        self._infolog(f"updating integrated vars in {table_name}")
        with self.sqliteObj.connect_via_sqlite() as con:
            user_tt_table = UserTTsTable()
            table_exists = DatabaseTable.exists(table_name, con)
            if not table_exists:
                self._infolog(
                    f"Creating the '{user_tt_table.name}' table for {user_tt_table.description}"
                )
                user_tt_table.create_in_db(con)

            cols = [
                "variable",
                "sample",
                "is_svar",
                "user_tt",
                "date_created",
                "file_timestamp",
            ]
            wildcards = ["?" for t in cols]
            insert_string = f"INSERT or REPLACE into \
                             {table_name} \
                             ({', '.join(cols)}) \
                             VALUES({', '.join(wildcards)})"
            info = self._inserts_for_one_group(
                table_name, "integrated_variables", group_list=variable_list
            )
            con.executemany(insert_string, info)

    def update_editing_rules(self, rule_list):
        """Update samplevariable_editing_rules table

        If no rules list provided, all rules will be updated.
        Currently, no rule_list is expected as the process updates
        the entire table every call.

        Args:
            rule_list (list[str]): List of integrated variables with editing rules.
        """
        if self._nosqlite():
            return
        if not self.project.has_stata_editing_rules:
            return

        table_name = "samplevariable_editing_rules"
        self._infolog(f"updating stata editing rules in {table_name}")
        with self.sqliteObj.connect_via_sqlite() as con:
            editing_rule_table = SamplevariableEditingRulesTable()
            table_exists = DatabaseTable.exists(table_name, con)
            # Right now the plan is to create all records fresh with every
            # export. This is simply to keep things clean, making sure stale
            # files are not released. If we outgrow this approach we can
            # reevaluate.
            if table_exists:
                delete = f"DROP TABLE {editing_rule_table.name}"
                con.execute(delete)
            self._infolog(
                f"Creating the '{editing_rule_table.name}' table for {editing_rule_table.description}"
            )
            editing_rule_table.create_in_db(con)
            # then insert the new set
            rule_list = [
                Path(self.project.variable_stata_editing_rule(rule))
                for rule in rule_list
            ]
            cols = [col.name for col in editing_rule_table.columns]
            insert_statement = self._parameterize_insert(editing_rule_table.name, cols)
            data = self._inserts_for_editing_rules(files=rule_list)
            try:
                con.executemany(insert_statement, data)
            except Exception as e:
                print(data[0])
                raise e

    def _remove_integrated_variable_records(self, table_name, variable_list):
        """Remove integrated variable records."""
        if self._nosqlite():
            return
        self._infolog(f"removing vars in {table_name}")
        with self.sqliteObj.connect_via_sqlite() as con:
            delete_string = f"DELETE from {table_name} where variable = ?"
            seq_of_parameters = [(v,) for v in variable_list]
            con.executemany(delete_string, seq_of_parameters)

    def remove_integrated_variable_trans_table_records(self, variable_list):
        """Remove list of integrated variable trans table records."""
        self._remove_integrated_variable_records(self.tt_table.name, variable_list)
        if self.project.publish_user_trans_tables:
            self._remove_integrated_variable_records("user_tts", variable_list)
        for table in self.relational_tt_tables:
            if table.name == "tt_last_updated":
                # This is handled in the _tt_last_updated_sql() method
                continue
            self._remove_integrated_variable_records(table.name, variable_list)
        if self.project.has_stata_editing_rules:
            self._remove_integrated_variable_records(
                "samplevariable_editing_rules", variable_list
            )

    def warn_on_invalid_variables(self):
        for t in self.relational_tt_tables:
            if t.name == "tt_last_updated":
                # This is handled in the _tt_last_updated_sql() method
                continue
            self._warn_on_invalid_rows(t, "variable", "variable_trans_tables")

    def _warn_on_invalid_rows(self, table, foreign_key, foreign_table):
        with self.sqliteObj.connect_via_sqlite() as con:
            try:
                values = table.invalid_values(foreign_key, foreign_table, con)
            except AttributeError:
                values = []
        # Better formatted warnings, grouped by table here
        if values:
            values = "\n\t".join(values)
            log.warning(
                f"Invalid {foreign_key} value(s) in {table.name}:\n\t" f"{values}\n"
            )

    def remove_integrated_variable_descriptions(self, variable_list):
        """Remove list of integrated variable description records."""
        self._remove_integrated_variable_records("variable_descriptions", variable_list)

    def update_integrated_variable_trans_tables(self, variable_list):
        """Update integrated variable trans tables."""
        self._update_integrated_variable_tables(self.tt_table.name, variable_list)

        # if the project publishes user TT csvs (ipumsi), do that now too
        self._update_user_tts(variable_list)

        # relational TT tables
        errors = self._update_tt_tables_for_integrated_variables(variable_list)
        # XXX The returning of errors here feels inelegant, as none of the other
        # similar methods return anything. This is mostly because we expect all of
        # the errors to be encountered during the export process, not during the
        # sql dump process. These relational TT tables are an exception in that no
        # where else is some of this information accessed. We should probably collect
        # the new errors that are encountered and add audits for them so that we
        # can take this error reporting away. The hope is that this errors variable
        # will always be an empty list.
        return errors

    def update_integrated_variable_descriptions(self, variable_list):
        """Update integrated variable descriptions."""
        self._update_integrated_variable_tables("variable_descriptions", variable_list)

    def update_tt_tables_for_sample(self, sample):
        """Cache all data for tt tables for a given sample."""
        if self._nosqlite():
            return

        dd = self.product.dd(sample)

        self._infolog(["dumping ", sample, "tt tables metadata"])
        error_message = []
        # TODO: break out specific error events to be more informative
        try:
            self._tt_tables_rowdata_for_source_variables(dd)
            # always export svars to the samplevariables table
            self._tt_samplevariables_table_rowdata_for_source_variable(dd)

            # add inserts and deletes for tt_last_updated table
            self._tt_last_updated_sql(self.samp_mgr, sample)

            self._update_via_apsw(self.samp_mgr)
        except Exception as e:
            error_message = [
                f"ERROR: {sample} svar metadata could not be parsed for inclusion in "
                f"the metadata database, check the sample's DD audits if this error "
                f"is not clear: {e}",
            ]
        return error_message

    def _update_tt_tables_for_integrated_variables(self, varlist):
        """Updating TT tables for a given list of integrated variables."""
        self._infolog(["updating integrated variable tt tables metadata"])
        err_messages = []
        if len(varlist) > 2:
            title = "Integrated vars SQLite TT Tables"
            export_tts = Parallel(n_jobs=100)(
                delayed(self._export_tt_row_and_delete_data)(v)
                for v in utilities.progress_bar(varlist, desc=title)
            )
        else:
            export_tts = [self._export_tt_row_and_delete_data(v) for v in varlist]

        for e in export_tts:
            err_messages.extend(
                self._tt_tables_rowdata_for_integrated_variable(
                    e.var, e.rows, e.deletes, e.errors
                )
            )
        self._update_via_apsw(self.ivar_mgr)
        return err_messages

    def _update_source_variable_tables(self, table_name, variable_list):
        """Update source variable tables.

        Requires a list of source variables to update.
        """

        if self._nosqlite():
            return
        self._infolog(f"updating source vars in {table_name}")
        with self.sqliteObj.connect_via_sqlite() as con:
            table_exists = DatabaseTable.exists(table_name, con)
            if not table_exists:
                if table_name == "variable_descriptions":
                    self._create_variable_descriptions_table()
            insert_string = f"INSERT or REPLACE into {table_name}(variable, sample, is_svar, xml, date_created, file_timestamp) VALUES(?, ?, ?, ?, ?, ?)"
            info = self._inserts_for_one_group(
                table_name, "source_variables", group_list=variable_list
            )
            con.executemany(insert_string, info)

    def update_source_variable_descriptions(self, variable_list):
        """Update source variable descriptions."""
        self._update_source_variable_tables("variable_descriptions", variable_list)

    def update_enum_materials(self, doc_list):
        """Update enum materials tables.

        Requires a list of enum docs to update.
        """
        self._update_document_table(self.em_table, doc_list, self.em_dir)

    def update_web_documents(self, doc_list):
        """Update the table with web documents.

        Requires a list of web docs to update.
        """
        self._update_document_table(self.wd_table, doc_list, self.wd_dir)

    def update_insert_html(self, doc_list):
        """Update the table with insert_html documents.

        Requires a list of insert_html documents to update.
        """
        self._update_document_table(self.ih_table, doc_list, self.ih_dir)

    def _update_document_table(self, table, doc_list, base_dir):
        """
        Update a database table from a set of documents with XML content.

        Args:
            table (DatabaseTable):  A table with XML content.
            doc_list (list of str):  The file names of the documents with
                                     updated XML content.
            base_dir (Path):  The directory where the documents are located.
        """
        if self._nosqlite():
            return
        self._infolog(f"updating {table.description} in table {table.name}")
        with self.sqliteObj.connect_via_sqlite() as con:
            table_exists = DatabaseTable.exists(table.name, con)
            if not table_exists:
                self._infolog(
                    f'Creating the "{table.name}" table for {table.description}'
                )
                table.create_in_db(con)
            insert_string = table.sql_cmd_to_insert(or_replace=True)
            info = self._inserts_docs(
                base_dir, doc_list=doc_list, file_suffix=table.FILE_SUFFIX
            )
            con.executemany(insert_string, info)

    # XXX this might (?) be redundant to other code in this module
    def _parameterize_insert(self, table, cols, insert_or_replace=False):
        columns_string = ", ".join(cols)
        parameters = ", ".join(["?" for v in cols])
        if insert_or_replace is True:
            insert = "INSERT or REPLACE"
        else:
            insert = "INSERT"
        return (
            insert
            + " into "
            + table
            + "("
            + columns_string
            + ") VALUES("
            + parameters
            + ")"
        )

    def _update_via_apsw(self, mgr):
        """Update accumulated tt_tables_rowdata to sqlite via apsw."""
        for table, rowdata in mgr.cached_table_rowdata.items():
            if len(rowdata):
                table_columns = rowdata[0].keys()
                insert_string = self._parameterize_insert(table, table_columns)

                if table in mgr.cached_table_delete_statements:
                    deletes = list(set(mgr.cached_table_delete_statements[table]))
                    self.sqliteObj._execute_transactions(deletes)
                info = []
                for row in rowdata:
                    info.append([row[key] for key in table_columns])
                self.sqliteObj._executemany_transaction(insert_string, info)

    def _tt_tables_rowdata_for_source_variables(self, dd):
        """Populate rowdata into tt_tables_rowdata struct for source variables."""
        sample = dd.sample
        for table in VARIABLE_SAMPLE_COLUMN_TABLES:
            self.samp_mgr.cached_table_delete_statements[table].append(
                self._delete_variable_sample_rows(table, sample, svars_only=True)
            )
        for var in utilities.progress_bar(
            dd.all_svars, desc=f"SQLite svars for {sample}"
        ):
            var = var.upper()
            export_tt = ExportTransTableData(self.product, "svar", var)
            for table in VARIABLE_COLUMN_TABLES:
                self.samp_mgr.cached_table_delete_statements[table].append(
                    self._delete_variable_rows(table, var)
                )
                rowdata = export_tt._get_tt_rowdata(table)
                self.samp_mgr.cached_table_rowdata[table].extend(rowdata)

    def _tt_samplevariables_table_rowdata_for_source_variable(self, dd):
        """Populate svar rowdata into tt_tables_rowdata for tt_samplevariables"""
        table = "tt_samplevariables"
        sample = dd.sample
        self.samp_mgr.cached_table_delete_statements[table].append(
            self._delete_variable_sample_rows(table, sample, svars_only=True)
        )
        for var in dd.all_svars:
            var = var.upper()
            export_tt = ExportTransTableData(self.product, "svar", var)

            rowdata = export_tt._get_tt_rowdata(table)
            self.samp_mgr.cached_table_rowdata[table].extend(rowdata)

    def _export_tt_row_and_delete_data(self, var):
        var = var.upper()
        export_tt = ExportTransTableData(self.product, "integrated", var)
        row_data = {}
        delete_data = {}
        error_data = {}
        for table in TT_TABLES:
            with self.sqliteObj.connect_via_sqlite() as con:
                if not DatabaseTable.exists(table, con):
                    self._create_variable_trans_tables_tables()
            try:
                row_data[table] = export_tt._get_tt_rowdata(table)
                delete_data[table] = self._delete_variable_rows(
                    table, export_tt.variable
                )
            except Exception as e:
                row_data[table] = (
                    f"ERROR: {var} Metadata could not be parsed for inclusion in "
                    f"database for the {table} table: {e}"
                )
                delete_data[table] = False
                error_data[table] = True
            else:
                error_data[table] = False

        Data = namedtuple("Data", ["var", "rows", "deletes", "errors"])
        return Data(var=var, rows=row_data, deletes=delete_data, errors=error_data)

    def _tt_tables_rowdata_for_integrated_variable(self, var, rows, deletes, errors):
        """Populate rowdata into cached_table_rowdata struct for an integrated variable."""
        err_messages = []
        for table in TT_TABLES:
            if errors[table]:
                err_messages.append(rows[table])
            else:
                self.ivar_mgr.cached_table_delete_statements[table].append(
                    deletes[table]
                )

                self.ivar_mgr.cached_table_rowdata[table].extend(rows[table])
        # add inserts and deletes for tt_last_updated table
        self._tt_last_updated_sql(self.ivar_mgr, var)
        return err_messages

    def _tt_last_updated_sql(self, mgr, sample_or_var):
        delete = "DELETE from tt_last_updated where sample_or_variable = '{}'".format(
            sample_or_var
        )
        mgr.cached_table_delete_statements["tt_last_updated"].append(delete)
        mgr.cached_table_rowdata["tt_last_updated"].append(
            {"sample_or_variable": sample_or_var, "date_created": self.now}
        )

    def create_input_data_variables_tables(self):
        """Wipes and recreates the input data variables tables from data dictionaries"""
        self._infolog("Creating input data variables tables")

        with self.sqliteObj.connect_via_sqlite() as con:
            samples = iter(self.product.samples.all_samples)

            first_sample = next(samples)
            self._build_input_data_variable_tables(con, first_sample)

            self.mass_process_dds(samples, con)

    def _export_dd_to_db(self, con, var_info, val_info, first_time=False):
        """Export dataframes of variable and value info into DB.

        Arguments:
            con:        SqliteConnection. Should be supplied via self.sqliteObj.connect_via_sqlite(). Passed as an argument as to not cause any multithreading issues.
            var_info:   Dataframe of all input data variable information rows to export. Will be exported into input_data_variable_info.
            val_info:   Dataframe of all input data variable value rows to export. Will be exported into input_data_variable_values.

        Returns:
            None
        """
        idvi_insert = self.idvi_table.sql_cmd_to_insert(name_placeholders=True)
        idvv_insert = self.idvv_table.sql_cmd_to_insert(name_placeholders=True)
        vi_dicts = [d._asdict() for d in list(var_info.itertuples(index=False))]
        vv_dicts = [d._asdict() for d in list(val_info.itertuples(index=False))]
        if first_time:
            con.executemany(idvi_insert, vi_dicts)
            con.executemany(idvv_insert, vv_dicts)
        else:
            if len(vi_dicts) > 100000:
                # Faster, more efficient, less interpretable
                for i in utilities.progress_bar(
                    range(0, len(vi_dicts), 100000),
                    desc="Loading Input Data Var Records (Chunks of 100,000)",
                    unit_scale=True,
                ):
                    con.executemany(
                        idvi_insert, vi_dicts[i : min(i + 100000, len(vi_dicts))]
                    )
            else:
                # From a readability standpoint, this is better, but less efficient
                for d in utilities.progress_bar(
                    list(var_info.itertuples(index=False)),
                    desc="Loading Input Data Var Records",
                    unit_scale=True,
                ):
                    con.execute(idvi_insert, d._asdict())

            if len(vv_dicts) > 100000:
                # Faster for bulk
                for i in utilities.progress_bar(
                    range(0, len(vv_dicts), 500000),
                    desc="Loading Input Data Value Records (Chunks of 500,000)",
                    unit_scale=True,
                ):
                    con.executemany(
                        idvv_insert, vv_dicts[i : min(i + 500000, len(vv_dicts))]
                    )

            else:
                # More interpretable for small
                for d in utilities.progress_bar(
                    list(val_info.itertuples(index=False)),
                    desc="Loading Input Data Val Records",
                    unit_scale=True,
                ):
                    con.execute(idvv_insert, d._asdict())

    def mass_process_dds(self, dd_list, con):
        """Mass process DDs. Run self._prep_dd_data_frame in parallel to build large dataframes to export once.

        Arguments:
            dd_list: List of DDs to export.
            con:     SqliteConnection. Should be supplied via self.sqliteObj.connect_via_sqlite(). Passed as an argument as to try avoiding any multithreading issues.

        Returns:
            None
        """

        zipped = Parallel(n_jobs=100)(
            delayed(self._prep_dd_data_frame)(dd)
            for dd in utilities.progress_bar(
                dd_list, desc="Input Data Variable DD Tables"
            )
        )

        zipped = list(filter(lambda x: x is not None, zipped))

        var_rows, val_rows = zip(*zipped)
        var_info = pd.concat(list(var_rows))
        val_info = pd.concat(list(val_rows))
        self._export_dd_to_db(con, var_info, val_info)

    def update_input_data_variables_tables(self, dd_list):
        """Update given samples by dropping old copies.

        Arguments:
            dd_list: List of samples to update. Will load new entries into a DB and drop older entries of a sample.

        Returns:
            None
        """
        self._infolog(
            "Updating input data variable in input_data_variable_info and input_data_variable_values"
        )
        with self.sqliteObj.connect_via_sqlite() as con:
            idv_tables_exists = DatabaseTable.exists(
                "input_data_variable_info", con
            ) and DatabaseTable.exists("input_data_variable_values", con)
            if not idv_tables_exists:
                self._infolog(
                    "Could not find input_data_variables tables. Building now."
                )
                self.create_input_data_variables_tables()
                return
            cur_time = datetime.datetime.now()
            self.drop_old(con, dd_list, cur_time)
            if len(dd_list) > 3:
                self.mass_process_dds(dd_list, con)
            else:
                for samp in dd_list:
                    var_rows, val_rows = self._prep_dd_data_frame(samp)
                    self._export_dd_to_db(con, var_rows, val_rows)

    def drop_old(self, con, samples, cur_time):
        """Drop old DDs from DB.

        Arguments:
            con:        SqliteConnection. Should be supplied via self.sqliteObj.connect_via_sqlite(). Passed as an argument as to not cause any multithreading issues.
            samples:    List of sample strings. List of samples to drop existing from.
            cur_time:   Time of upload. Used to determine old samples.

        Returns:
            None
        """
        samp_str = "', '".join(samples)
        con.execute(
            f"DELETE FROM input_data_variable_info WHERE sample IN ('{samp_str}') AND date_created < '{cur_time}'"
        )
        con.execute(
            f"DELETE FROM input_data_variable_values WHERE sample IN ('{samp_str}') AND date_created < '{cur_time}'"
        )

    def _build_input_data_variable_tables(
        self, con: SqliteConnectionManager, sample: str
    ):
        """Create input data variable tables in SQLite Database.

        Arguments:
            con:    SqliteConnection. Should be supplied via self.sqliteObj.connect_via_sqlite(). Passed as an argument as to not cause any multithreading issues.
            sample: Sample name string. Will be passed along to self._prep_dd_data_frame(),

        Returns:
            None

        """
        var_rows, val_rows = self._prep_dd_data_frame(sample)
        con.execute(f"DROP TABLE IF EXISTS input_data_variable_info")
        con.execute(f"DROP TABLE IF EXISTS input_data_variable_values")

        con.execute(self.idvi_table.sql_cmd_to_create())
        con.execute(self.idvv_table.sql_cmd_to_create())

        self._export_dd_to_db(con, var_rows, val_rows, first_time=True)

    def _get_dd_val_rows(self, df: pd.DataFrame, samp: str) -> pd.DataFrame:
        """Build value rows from DD dataframe

        Arguments:
            df:     DD Dataframe. Should be built out from _prep_dd_data_frame method beforehand.
            samp:   Sample name.

        Return:
            df: Dataframe containing all value information. Will be used to export to 'input_data_variable_values'.
        """

        df = df.replace(r"^\s*$", pd.NA, regex=True)

        # Only can do ffill if NA
        ffilled = df.ffill(axis=0)
        ffilled.drop(df.dropna(axis=0, subset="svar").index, inplace=True)
        df = ffilled.filter([col.name for col in self.idvv_table.columns])
        df["sample"] = samp
        return df

    def _prep_dd_data_frame(self, sample: str) -> (tuple | None):
        """Prepare a data dictionary for export by separating it into components.

        Arguments:
            sample: is the name of the sample

        Returns:
            var_rows: Pandas DataFrame of input data variable information rows. Will export to 'input_data_variable_info'.
            val_rows: Pandas DataFrame of input data variable value information rows. Will export to 'input_data_variable_values'.

        Raise:
            If sample does not exist, returns None and lets user know

        """
        try:
            dd = DataDictionary(sample, self.project.name)
        except AssertionError:
            print(f"Could not find {sample}. Skipping export for only this sample.")
            return None

        df = dd.ws

        ## Build dataframe representation of DD
        df.columns = map(str.lower, df.columns)
        df.columns = [col.replace(" ", "_") for col in df.columns]
        df = df.loc[:, ~df.columns.str.contains("^unnamed")]
        df = df.rename_axis("origrow").reset_index()
        df["date_created"] = str(datetime.datetime.now())
        df["file_timestamp"] = self._get_file_mtime(dd.xlpath)
        df = df[df["recordtype"] != "<END>"]
        df = df.convert_dtypes()
        df["origrow"] = df["origrow"].astype("str")

        ## Build var_rows to export to 'input_data_variable_info'
        var_rows = df[df["svar"] != ""]
        var_rows.insert(len(var_rows.columns), "sample", sample)
        var_rows = var_rows[[col.name for col in self.idvi_table.columns]]

        ## Build val_rows to export to 'input_data_variable_values'
        val_rows = self._get_dd_val_rows(df, sample).fillna("")
        return var_rows, val_rows

    def drop_web_docs_table(self):
        self._drop_table(self.wd_table)

    def drop_insert_html_table(self):
        self._drop_table(self.ih_table)

    def drop_enum_materials_table(self):
        self._drop_table(self.em_table)

    def drop_user_tts_table(self):
        self._drop_table(UserTTsTable())

    def drop_tt_table(self):
        self._drop_table(self.tt_table)

    def drop_vd_table(self):
        self._drop_table(VarDescTable())

    def _drop_table(self, table):
        """Drop table from database

        Args:
            table (DatabaseTable): Table to be dropped
        """
        if self._nosqlite():
            return
        self._infolog(f"Removing {table.description} table for fresh create.")
        with self.sqliteObj.connect_via_sqlite() as con:
            table.drop_from_db(con)
