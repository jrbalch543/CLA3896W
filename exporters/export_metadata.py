import sys
from pathlib import Path
import argparse
import warnings


from ipums.metadata import utilities
from ipums.metadata.exporters import ExportOpts
from ipums.metadata.exporters.sqlite.sqlite_db_versioning import (
    VersioningMetadataDatabase,
)

import ipums.metadata.exporters.export_control_file as exp_cf
import ipums.metadata.exporters.export_trans_table as exp_tt
import ipums.metadata.exporters.export_var_desc as exp_vd
import ipums.metadata.exporters.export_enum_materials as exp_em
import ipums.metadata.exporters.export_insert_html as exp_ih
import ipums.metadata.exporters.export_web_docs as exp_wd
import ipums.metadata.exporters.export_editing_rules as exp_er

# Copied from https://stackoverflow.com/questions/842557/how-to-prevent-a-block-of-code-from-being-interrupted-by-keyboardinterrupt-in-py
import signal
import logging


class DelayedKeyboardInterrupt:
    def __enter__(self):
        self.signal_received = False
        self.old_handler = signal.signal(signal.SIGINT, self.handler)

    def handler(self, sig, frame):
        self.signal_received = (sig, frame)
        logging.debug("SIGINT received. Delaying KeyboardInterrupt.")

    def __exit__(self, type, value, traceback):
        signal.signal(signal.SIGINT, self.old_handler)
        if self.signal_received:
            self.old_handler(*self.signal_received)


# silence warning from kombu. This warning should go away
# with version 5.3 https://github.com/celery/kombu/pull/1509
warnings.filterwarnings("ignore", message=".*SelectableGroups")


class ExportReport:
    EXPORT_HEADER = "Exported Metadata:"
    ERROR_HEADER = "Errors:"

    def __init__(self, message) -> None:
        self.message = message
        self.reports_by_exporter = {}

    def add_report(self, exporter, exports, errors):
        if exporter not in self.reports_by_exporter:
            self.reports_by_exporter[exporter] = {
                "exports": [],
                "errors": [],
            }
        if isinstance(exports, str):
            exports = [exports]
        if isinstance(errors, str):
            errors = [errors]
        self.reports_by_exporter[exporter]["exports"].extend(exports)
        self.reports_by_exporter[exporter]["errors"].extend(errors)

    def underline_header(self, header, underline_char="="):
        return "\n".join(["", header, underline_char * len(header)])

    @property
    def report(self):
        message = [self.message]
        exports_lines = []
        errors_lines = []
        for exporter, report in self.reports_by_exporter.items():
            if report["exports"]:
                exports_lines.append(self.underline_header(exporter, "+"))
                exports_lines.append("\n\t" + "\n\t".join(report["exports"]))
            if report["errors"]:
                errors_lines.append(self.underline_header(exporter, "+"))
                errors_lines.append("\n\t" + "\n\t".join(report["errors"]))

        if exports_lines:
            message.append(self.underline_header(self.EXPORT_HEADER))
            message.extend(exports_lines)
        if errors_lines:
            message.append(self.underline_header(self.ERROR_HEADER))
            message.extend(errors_lines)

        return "\n".join(message)

    def __str__(self):
        return self.report


parser = argparse.ArgumentParser(
    description="""
-----------------------------------
IPUMS Microdata Metadata Exporter:
-----------------------------------

This utility can be used to export individual IPUMS Microdata Metadata files,
collections of specific metadata types (e.g. control files, translation tables,
etc.) or an entire metadata collection. The exported metadata is then loaded
into a sqlite metadata.db

The `--docs` and `--editing-rules` are flags that will always export the entire
collection, not individual docs or editing rule files by name.

The `--dd` option will export svar content from a data dictionary as both
variables control file info and translation tables (if needed for the project)

""",
    epilog="""
-----------------------------------
Examples:
-----------------------------------

Export all MICS metadata that needs to be exported:
> export_metadata --project mics --all

Export all metadata, even if it doesn't need it:
> export_metadata -p mics --all --force

Export all docs (enumeration materials, web docs, etc) that need to be exported:
> export_metadata -p mics --docs

Export a specific list of translation tables:
> export_metadata -p usa --tt age sex race relate

Export a specific list of data dictionaries:
> export_metadata -p usa --dd us2013b us2014b

Export just the variables control file:
> export_metadata -p cps --cf variables

Force export just the revisions and the links control files:
> export_metadata -p dhs --cf links revisions --force
""",
    formatter_class=argparse.RawDescriptionHelpFormatter,
)
args = parser.add_argument_group("arguments")
args.add_argument(
    "-p",
    "--project",
    action="store",
    dest="project",
    required=True,
    help="IPUMS Project",
)
args.add_argument(
    "-a",
    "--all",
    action="store_true",
    dest="all",
    required=False,
    default=False,
    help="Export ALL metadata across microdata project",
)
args.add_argument(
    "--cf",
    action="store",
    dest="cf",
    required=False,
    default=False,
    nargs="*",
    help="Export one or more control files. Enter `--cf all` for all CFs.",
)
args.add_argument(
    "--tt",
    action="store",
    dest="tt",
    required=False,
    default=False,
    nargs="*",
    help="Export one or more integrated variable translation tables."
    " Enter `--tt all` for all TTs.",
)
args.add_argument(
    "--dd",
    action="store",
    dest="dd",
    required=False,
    default=False,
    nargs="*",
    help="Export one or more sample data dictionaries. Enter `--dd all` for all DDs.",
)
args.add_argument(
    "--vd",
    action="store",
    dest="vd",
    required=False,
    default=False,
    nargs="*",
    help="Export one or more integrated variable descriptions. Enter"
    " `--vd all` for all VDs.",
)
args.add_argument(
    "--docs",
    action="store_true",
    dest="docs",
    required=False,
    default=False,
    help="Export documents (enum_materials, web_docs, insert_html).",
)
args.add_argument(
    "--editing-rules",
    action="store_true",
    help="Export all editing rules (currently only Stata format supported).",
)
args.add_argument(
    "--force",
    action="store_true",
    dest="force",
    required=False,
    help="If set, will write out exports even if exports are current.",
)
args.add_argument(
    "--dryrun",
    action="store_true",
    dest="dryrun",
    required=False,
    default=False,
    help="If set, a list of exports will be printed without actually exporting",
)
args.add_argument(
    "--projects_config",
    action="store",
    dest="projects_config",
    required=False,
    default=None,
    help="(for debug and testing use) Use alternative path to projects.json file",
)
args.add_argument(
    "--verbose",
    action="store_true",
    dest="verbose",
    required=False,
    help="(useful for sqlite debugging) Emit verbose output",
)
args.add_argument(
    "--debug",
    action="store_true",
    dest="debug",
    required=False,
    help="(for debug purposes only) Write XML outputs to /tmp/metadata/trans_tables.",
)
args.add_argument(
    "--db_file",
    action="store",
    dest="db_file",
    required=False,
    help="(for debug purposes only) Write to a non-default database path.",
)
args.add_argument(
    "--serial",
    action="store_true",
    dest="serial",
    default=False,
    required=False,
    help="(for debug purposes only) Export TTs in serial instead of parallel.",
)
args.add_argument(
    "--no-version",
    action="store_true",
    dest="no_version",
    default=False,
    help=argparse.SUPPRESS,
    # "Do not create a new metadata version as part of this export."
    # " This is useful when you need to export one or two metadata files but will not"
    # " need the new metadata immediately so you want to avoid the time consuming"
    # " versioning operations.",
)

# Suppress this arg for now. It is only really used by the tests
args.add_argument("--list", action="store_true", help=argparse.SUPPRESS)


def check_args(opts):
    if opts.dryrun:
        print("*** --dryrun flag used, no actual exports will be performed ***")
    if opts.projects_config and not Path(opts.projects_config).exists():
        raise FileNotFoundError("No file exists at: " + opts.projects_config)

    projects = utilities.projects(projects_config=opts.projects_config)
    project_list = ", ".join(projects)
    if opts.project not in projects:
        raise argparse.ArgumentError(
            argument=None,
            message=opts.project + " not in known project list: " + project_list,
        )


def _build_helper(opts):
    helper = {
        "project": opts.project,
        "force": opts.force,
        "dryrun": opts.dryrun,
        "projects_config": opts.projects_config,
        "verbose": opts.verbose,
        "debug": opts.debug,
        "db_file": opts.db_file,
        "serial": opts.serial,
        "no_version": opts.no_version,
        "listall": opts.list,
    }
    if opts.all:
        helper["all"] = True
        helper["allsamples"] = True
        helper["allvars"] = True
    else:
        if opts.cf:
            if "all" in opts.cf:
                helper["all"] = True
            else:
                helper["cf"] = opts.cf
        if opts.tt:
            if opts.tt[0] == "all":
                helper["allvars"] = True
                helper["variable"] = False
            else:
                helper["variable"] = opts.tt
        if opts.dd:
            if opts.dd[0] == "all":
                helper["allsamples"] = True
            else:
                helper["samples"] = opts.dd
        if opts.vd:
            if opts.vd[0] == "all":
                helper["all"] = True

            else:
                helper["variable"] = opts.vd
        if opts.docs:
            helper["all"] = True
        if opts.editing_rules:
            helper["all"] = True
    return helper


def check_metadata_version_status(opts):
    if opts.dryrun or opts.no_version:
        return None
    if opts.db_file:
        db_file = Path(opts.db_file)
    else:
        db_file = Path(opts.product.project.path) / "metadata" / "metadata.db"
    try:
        versioning = VersioningMetadataDatabase(db_file, use_scratch=True)
        print(
            f"Current metadata version: {versioning.current_version}",
            f"\nThis export will create version {versioning.next_version}",
        )
        if versioning.dvc_file_is_dirty():
            warnings.warn("Versioning Database has untracked changes.")
    except FileNotFoundError as e:
        print(f"Metadata Versioning is not enabled for this project. {e}")
        return None
    return versioning


def construct_commit_message():
    msg = " ".join(sys.argv)
    msg += f"\n\nPython: {sys.executable}\n"
    return msg


def main(opts):
    # Simple warning format so that users don't get a full stack trace
    warnings.formatwarning = lambda msg, *args, **kwargs: f"warning: {msg}\n"
    if opts.dryrun:
        print("DRYRUN selected. No actual exports will occur.")

    helper = _build_helper(opts)
    export_opts = ExportOpts(**helper)

    versioning = check_metadata_version_status(export_opts)
    if versioning:
        # use the DB file defined by the versioning object
        export_opts.db_file = versioning.db_file
    report = ExportReport(construct_commit_message())
    try:
        if opts.all:
            report.add_report(*exp_cf.main(export_opts))
            report.add_report(*exp_tt.main(export_opts))
            report.add_report(*exp_vd.main(export_opts))
            report.add_report(*exp_em.main(export_opts))
            report.add_report(*exp_ih.main(export_opts))
            report.add_report(*exp_wd.main(export_opts))
            report.add_report(*exp_er.main(export_opts))
        else:
            if opts.cf:
                for cf in opts.cf:
                    export_opts.set_cf(cf)
                    report.add_report(*exp_cf.main(export_opts.cf_mode()))
            if opts.tt:
                report.add_report(*exp_tt.main(export_opts.variable_mode()))
            if opts.dd:
                if export_opts.allsamples:
                    report.add_report(*exp_cf.main(export_opts.sample_mode()))
                    report.add_report(*exp_tt.main(export_opts.sample_mode()))
                    report.add_report(*exp_vd.main(export_opts.sample_mode()))
                else:
                    for samp in export_opts.samples:
                        export_opts.set_sample(samp)
                        report.add_report(*exp_cf.main(export_opts.sample_mode()))
                        report.add_report(*exp_tt.main(export_opts.sample_mode()))
                    # exp_vd only operates on the samples option and not the sample
                    # option like exp_cf and exp_tt, so just having the correct
                    # samples export_opts set is sufficient.
                    report.add_report(*exp_vd.main(export_opts.sample_mode()))
            if opts.vd:
                report.add_report(*exp_vd.main(export_opts))
            if opts.docs:
                report.add_report(*exp_em.main(export_opts))
                report.add_report(*exp_ih.main(export_opts))
                report.add_report(*exp_wd.main(export_opts))
            if opts.editing_rules:
                report.add_report(*exp_er.main(export_opts))
        if versioning:
            versioning.commit(str(report))
        print("ALL DONE!")
    except Exception as e:
        if versioning and versioning.current_version.is_dirty:
            report.add_report("EXPORT FAILED", [], [str(e)])
            versioning.commit("!!!FAILURE!!! " + str(report), tag_version=False)
            warnings.warn(
                f"Error occurred during export!\n"
                f"!!! New un-versioned commit {versioning.current_version} "
                "was created but probably should not be used."
            )
        raise
    except KeyboardInterrupt as e:
        with DelayedKeyboardInterrupt():
            if versioning and versioning.current_version.is_dirty:
                print(f"Ctrl-C detected. Rolling back any incomplete export process.")
                versioning.reset_to_current_version()


def entry_point():
    opts = parser.parse_args()
    check_args(opts)
    main(opts)


if __name__ == "__main__":
    sys.exit(entry_point())
