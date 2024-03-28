"""Command-line wrapper for xml_exporter.py."""
from ipums.metadata import (
    constants,
    utilities,
    IPUMS,
    MetadataError,
)
from ipums.metadata.exporters import ExportOpts, Exporter, SqliteMetadataDumper

from pprint import pprint
from pathlib import Path
from joblib import Parallel, delayed
import sys


def export_integrated_variable(v, helper):
    """export variable from project to xml."""
    try:
        proj = helper.product.project
        constants = helper.product.constants
        var_desc_folder = Path(proj.path) / constants.xml_metadata_vardesc
        if helper.debug:
            var_desc_folder = Path("/tmp/metadata/var_descs")

        if not var_desc_folder.exists():
            var_desc_folder.mkdir(parents=True)

        export = Exporter(
            product=helper.product,
            force=helper.force,
            debug=helper.debug,
            dryrun=helper.dryrun,
            encoding="utf8",
            db_file=helper.db_file,
        )
        res = export.integrated_variable_desc_to_xml(variable=v)
        if res:
            if res == "*":
                return (True, v.lower())
            else:
                return (True, None)
        else:
            return (False, "!" + v.lower() + " did not export")
    except Exception as e:
        return (False, " ".join(["!", v.lower(), ":", str(e)]))


def export_source_variable(svar_path, svar_outpath, helper):
    """export source variable from project to xml."""
    try:
        export = Exporter(
            product=helper.product,
            force=helper.force,
            debug=helper.debug,
            dryrun=helper.dryrun,
            encoding="utf8",
            db_file=helper.db_file,
        )
        res = export.source_variable_desc_to_xml(infile=svar_path, outfile=svar_outpath)
        if res:
            if res == "*":
                return (True, svar_outpath.name)
            else:
                return (True, None)
        else:
            return (False, "!" + svar_outpath.name + " did not export")
    except Exception as e:
        return (False, " ".join(["!", svar_outpath.name, ":", str(e)]))


def export_to_sqlite(var_list, helper, svars=False, drop_table=False):
    if "no_sqlite" not in helper.product.project.config and not helper.debug:
        if svars:
            var_type = "source"
        else:
            var_type = "integrated"
        print("Updating {} var descs in sqlite database...".format(var_type), end="")
        dump = SqliteMetadataDumper(
            product=helper.product, verbose=helper.verbose, db_file=helper.db_file
        )
        if drop_table:
            dump.drop_vd_table()
        if svars:
            dump.update_source_variable_descriptions(var_list)
        else:
            dump.update_integrated_variable_descriptions(var_list)
        print("DONE!")


def _listvars(project, projvars):
    print("Integrated variables listed for", project, ":")
    [print(var.lower()) for var in projvars.all_variables]


def _integrated_variable_export(out, helper):
    product = helper.product
    projvars = product.variables
    if helper.all:
        integrated_variable_export_list = projvars.all_variables
    else:
        integrated_variable_export_list = helper.variable

    if helper.serial:
        integ_out = []
        progress_bar = utilities.progress_bar(
            integrated_variable_export_list, desc="Variable Descriptions"
        )
        for var in progress_bar:
            progress_bar.set_description(var)
            integ_out.append(export_integrated_variable(var, helper))
    else:
        integ_out = Parallel(n_jobs=25)(
            delayed(export_integrated_variable)(var, helper)
            for var in utilities.progress_bar(
                integrated_variable_export_list, desc="Variable Descriptions"
            )
        )
    out.extend(integ_out)
    exports = [x[1] for x in integ_out if x[0] and x[1]]
    if len(exports) > 0:
        if helper.all and helper.force:
            drop_table = True
        else:
            drop_table = False
        export_to_sqlite(exports, helper, drop_table=drop_table)

    return out


def _single_integrated_variable_export(out, helper):
    # single integrated variable
    v = helper.variable[0]
    helper.force = True
    print("Exporting", v)
    out_tuple = export_integrated_variable(v, helper)
    (success, result) = out_tuple
    out.append(out_tuple)
    if success:
        export_to_sqlite([v.lower()], helper)
    else:
        print("Export failed:", result)
    return out


def _svar_export(out, constants, helper):
    proj = helper.product.project
    var_desc_folder = Path(proj.path) / constants.xml_metadata_vardesc
    if helper.debug:
        var_desc_folder = Path("/tmp/metadata/var_descs")

    if not var_desc_folder.exists():
        var_desc_folder.mkdir(parents=True)

    if helper.samples:
        samples = helper.samples
    else:
        s = helper.product.samples
        samples = s.all_samples
    # find sample directory
    out_dir = Path(proj.path) / var_desc_folder
    input_and_output_paths = []
    for s in samples:
        samp = s.upper()
        base_dir = Path(proj.path) / "variables" / "svars" / samp
        if (not base_dir.exists()) and (proj.publish_svars):
            print("!!! WARNING: No directory found for", samp, file=sys.stderr)
        else:
            # get all vd.doc files from base_dir

            # check for either xml or doc files
            # check for duplicate file types
            # process .doc files
            # process .xml files

            var_descs_docs = list(base_dir.glob(samp + "*_desc.doc*"))
            var_descs_xml = list(base_dir.glob(samp + "*_desc.xml"))

            doc_stems = [Path(doc).stem for doc in var_descs_docs]
            xml_stems = [Path(xml).stem for xml in var_descs_xml]

            duplicate_formats = list(set(doc_stems) & set(xml_stems))
            if len(duplicate_formats) > 0:
                for df in duplicate_formats:
                    dup_message = (
                        "Variable description "
                        + df
                        + " exists as both .doc and .xml files in the directory "
                        + str(base_dir)
                        + ". Enum materials must only exist in one format in this directory."
                    )
                    raise MetadataError(dup_message)
            var_descs = list(
                (
                    set(var_descs_docs)
                    - set([stem + ".doc" for stem in duplicate_formats])
                )
                | (
                    set(var_descs_xml)
                    - set([stem + ".xml" for stem in duplicate_formats])
                )
            )

            out_paths = [out_dir / (f.stem + ".xml") for f in var_descs]
            combined = list(zip(var_descs, out_paths))
            input_and_output_paths.extend(combined)

    svars_out = Parallel(n_jobs=100)(
        delayed(export_source_variable)(infile, outfile, helper)
        for infile, outfile in utilities.progress_bar(
            input_and_output_paths, desc="Source variable descriptions"
        )
    )

    out.extend(svars_out)
    exports = [x[1] for x in svars_out if x[0] and x[1]]
    if len(exports) > 0:
        export_to_sqlite(exports, helper, svars=True)
    print("ALL DONE!")
    return out


def _export_report(out, dryrun):
    errors = [x[1] for x in out if not x[0]]
    if len(errors) > 0:
        error_report = []
        error_report.append("-------------------------------------")
        error_report.append("These are the errors that were found:")
        error_report.extend(errors)
        # these get a bit verbose for the console...
        # [print(err) for err in error_report]

        report = "export_vd_errors.txt"
        f = open(report, "w")
        for err in error_report:
            f.write(err)
            f.write("\n")
        f.close()
        print(len(errors), "errors found. Error report saved to:", report)

    # both values (success, result) of tuples in "out"
    # need to return True for this to be an export
    exports = [x[1] for x in out if x[0] and x[1]]

    if len(exports) > 0:

        exports.sort()

        if dryrun:
            print("Dryrun: these variable descriptions would be exported:")
            [print(export) for export in exports]
        else:
            report = "exported_var_descs.txt"
            f = open(report, "w")
            for export in exports:
                f.write(export)
                f.write("\n")
            f.close()
            print("Exported variable description list saved to", report)
    else:
        if dryrun:
            print("Dryrun: nothing to export")
    return exports, errors


def main(helper):
    out = []

    helper.product = IPUMS(
        helper.project.lower(), projects_config=helper.projects_config
    )
    proj = helper.product.project
    constants = helper.product.constants

    if helper.listvars:
        _listvars(helper.project, helper.product.variables)
        sys.exit(0)

    if helper.all or (helper.variable and len(helper.variable) > 1):
        out = _integrated_variable_export(out, helper)

    if helper.variable and len(helper.variable) == 1:
        out = _single_integrated_variable_export(out, helper)

    if helper.samples or helper.allsamples or (helper.all and proj.publish_svars):
        out = _svar_export(out, constants, helper)

    exports, errors = _export_report(out, helper.dryrun)
    return "Variable Descriptions", exports, errors
