"""Command-line wrapper for xml_exporter.py."""
import argparse
from pathlib import Path
import sys

from joblib import Parallel, delayed

from ipums.metadata import utilities
from ipums.metadata.exporters import Exporter, SqliteMetadataDumper


def __print_now(msg):
    """__print_now prints to stdout immediately, used for progress."""
    sys.stdout.write(msg)
    sys.stdout.flush()


def export_tt(export, variable, tt_type, output_type, tt_stem=None):
    if output_type == "xml":
        if tt_type == "integrated":
            return export.integrated_variable_tt_to_xml(
                variable=variable, tt_stem=tt_stem
            )
        elif tt_type == "svar":
            return export.svar_tt_to_xml(svar=variable)
    elif output_type == "json":
        if tt_type == "integrated":
            return export.integrated_variable_tt_to_json(
                variable=variable, tt_stem=tt_stem
            )
        elif tt_type == "svar":
            return export.svar_tt_to_json(svar=variable)


def export_integrated_variable(tup, helper):
    """export variable from project to xml.

    The tuple that's sent to this method includes the
    variable and display_variable. The common case is
    display_variable and variable being equal, in which
    case the trans table the export object looks for
    will have the same file stem as the variable name.
    When they are different, e.g. year-meps (variable)
    and year (display_variable), it will look for the
    display variable TT but output an xml file consistent
    with the variable. e.g. TT is year_tt.xls but XML
    is year-meps_tt.xml.
    """
    v = tup[0].lower()
    dv = tup[1].lower()
    try:
        export = Exporter(
            product=helper.product,
            force=helper.force,
            debug=helper.debug,
            dryrun=helper.dryrun,
            db_file=helper.db_file,
        )
        if dv == v:
            tt_stem = None
            var = v
        else:
            tt_stem = v
            var = dv
        res = export_tt(
            export,
            variable=var,
            output_type=helper.output_type,
            tt_type="integrated",
            tt_stem=tt_stem,
        )
        if res:
            if res == "*":
                return (True, v)
            else:
                return (True, None)
        else:
            return (False, "!" + v.lower() + " did not export")
    except Exception as e:
        return (False, " ".join(["!", v.lower(), ":", str(e)]))


def export_svar(export, svar, output_type):
    """Export the svar to XML."""
    """export svar from dd to xml."""
    try:
        export_tt(export, variable=svar, output_type=output_type, tt_type="svar")
        return (True, None)
    except Exception as e:
        return (False, " ".join([svar, ":", str(e)]))


def _export_sample(all_sample_errors, helper):
    (success, errors) = export_sample(helper)
    if not success:
        all_sample_errors.extend(errors)
        print("FAILED:", helper.sample)
        export = []
    else:
        export = [helper.sample]
    return export, all_sample_errors


def export_sample(helper):
    """Wrapper for exporting all svars in a sample to XML."""
    product = helper.product
    errors = []
    try:
        dd = product.dd(helper.sample)
    except Exception as e:
        errors.append(str(e))
        return (False, errors)

    # svar_all_info is slow the first time but then lru_cache kicks in
    # grab one variable all_info and see if that helps speed
    svars = list(dd.all_svars)
    if len(svars) == 0:
        errors.append(helper.sample + " Data Dictionary has no svars!")
        return (False, errors)

    svars.sort()
    for svar in svars:
        dd.svar_to_all_info(svar)
        break

    export = Exporter(
        product=product,
        force=helper.force,
        dd=dd,
        debug=helper.debug,
        dryrun=helper.dryrun,
        db_file=helper.db_file,
    )

    success = True
    # if dryrun, return success

    # if we are on a dryrun, we're done
    if helper.dryrun:
        return (success, errors)

    # check for timestamp file, compare to dd
    needs_export = export.svars_need_export(ddpath=dd.xlpath, sample=helper.sample)
    if needs_export:
        for svar in utilities.progress_bar(svars, desc=helper.sample):
            (worked, err) = export_svar(export, svar, helper.output_type)
            if not worked:
                success = False
                errors.append(err)
        if success:
            export.make_sample_timestamp(tt_exports=True, sample=helper.sample)
    # we make a sample_timestamp here for projects that don't publish_svars
    # because we want those samples flagged as "assessed needs no export"
    if not product.project.publish_svars:
        export.make_sample_timestamp(sample=helper.sample, tt_exports=False)

    return (success, errors)


def export_to_sqlite(var_list, sample_list, helper):
    product = helper.product
    errors = []
    if "no_sqlite" not in product.project.config:
        if not helper.debug or helper.db_file:
            __print_now("Updating integrated vars in sqlite database...")
            dump = SqliteMetadataDumper(
                product=product, verbose=helper.verbose, db_file=helper.db_file
            )
            if var_list:
                errors.extend(dump.update_integrated_variable_trans_tables(var_list))
            __print_now("Updating source vars in sqlite database...")
            for s in sample_list:
                errors.extend(dump.update_tt_tables_for_sample(s))
            __print_now("DONE!\n")
    return errors


def clean_up_sqlite_trans_table_table(cruft, helper):
    product = helper.product
    dump = SqliteMetadataDumper(
        product=product, db_file=helper.db_file, verbose=helper.verbose
    )
    if (
        len(cruft) > 0
        and "no_sqlite" not in product.project.config
        and not helper.debug
    ):
        __print_now("Removing invalid integrated vars from sqlite database...")
        dump.remove_integrated_variable_trans_table_records(cruft)
        __print_now("DONE!\n")
    # Removing this warning for now since it can't account for SVARs that are in
    # tt tables by not in the xml blob "trans_tables" table (JG 10/10/2023)
    # dump.warn_on_invalid_variables()


def clean_up_integrated_export_directory(exports, helper):
    product = helper.product
    __print_now("Removing cruft from export directory...")
    valid_integrated_variables = set(product.variables.all_variables)
    export_dir = (
        Path(product.constants.root_unix)
        / product.constants.metadata_dir
        / "trans_tables"
        / "integrated_variables"
    )
    xml_files = set([x.stem[:-3].upper() for x in export_dir.glob("*_tt.xml")])
    cruft = xml_files - valid_integrated_variables
    cruft_path_list = [export_dir / (v.lower() + "_tt.xml") for v in cruft]
    for p in cruft_path_list:
        __print_now("Deleting " + p.name)
        p.unlink()
    __print_now("DONE!\n")
    return list(cruft)


def _export_all_sample_tts(all_sample_errors, helper):
    product = helper.product
    s = product.samples
    check = Exporter(
        product=product,
        force=helper.force,
        debug=helper.debug,
        dryrun=helper.dryrun,
        db_file=helper.db_file,
    )
    exports = []

    samples_to_export = []
    print("Evaluating samples for svar TT export...")
    if helper.all and helper.force and not helper.dryrun:
        dump = SqliteMetadataDumper(
            product=product, verbose=helper.verbose, db_file=helper.db_file
        )
        dump.drop_tt_table()
        dump.drop_user_tts_table()
        # TODO: drop other relational TT tables here as well?

    for sample, dd in utilities.progress_bar(s.all_samples_dds.items(), desc="samples"):
        helper.set_sample(sample)
        (success, errors) = export_sample(helper)
        if success:
            exports.append(sample)
        else:
            all_sample_errors.extend(errors)
            print("FAILED:", sample)

    if helper.dryrun:
        if len(exports):
            print("These samples would have their svar TTs exported:")
            for sample in exports:
                print(sample)
        else:
            print("Dryrun: No svar TTs need exporting")

    return exports, all_sample_errors


def _export_integrated_variables(helper):
    product = helper.product
    proj = helper.project
    projvars = product.variables

    if helper.listvars:
        print(f"Integrated variables listed for {proj}:")
        [print(var.lower()) for var in projvars.all_variables]
        sys.exit(0)

    out = []
    if helper.all or helper.allvars or (helper.variable and len(helper.variable) > 1):
        if helper.allvars:
            var_tuples = projvars.var_display_var_tuples
        else:
            var_tuples = [
                (v, projvars.variable_to_display_variable(v)) for v in helper.variable
            ]
        if helper.serial:
            for tup in var_tuples:
                print(f"Exporting {tup[0]}", end="...")
                ret = export_integrated_variable(tup, helper)
                if ret == (True, None):
                    print("skipped")
                else:
                    print(ret)
                out.append(ret)
        else:
            out = Parallel(n_jobs=100)(
                delayed(export_integrated_variable)(tup, helper)
                for tup in utilities.progress_bar(
                    var_tuples, desc="Integrated Variables"
                )
            )
        skipped = len([r for r in out if r == (True, None)])
        exported = len([r for r in out if r[0] is True and r[1] is not None])
        failed = len([r for r in out if r[0] is False])
        if not helper.dryrun:
            print("Integrated variable exports:")
            print(f"{skipped} Skipped (export current)")
            print(f"{exported} Freshly exported")
            print(f"{failed} Failed to export")

    # single integrated variable
    else:
        if helper.variable is None:
            raise argparse.ArgumentTypeError(
                "You must specify an integrated variable or a sample for export"
            )
        elif helper.variable is not None:
            v = helper.variable[0]

            # always force a single variable
            helper.force = True

            print(f"Exporting {v}")
            tup = (v, projvars.variable_to_display_variable(v))
            (success, result) = export_integrated_variable(tup, helper)
            if not success:
                print(f"Export failed: {result}")
            # sent back results as a single element array
            out = [(success, result)]
    return out


def _export_report(exports):
    report = "exported_tts.txt"
    with open(report, "w") as f:
        [f.write(export + "\n") for export in exports]
    print("Exported TT list saved to", report)


def _error_report(all_sample_errors, integrated_variable_export_output):
    # put together sample errors and any errors found in integrated var export
    errors = all_sample_errors
    errors.extend([x[1] for x in integrated_variable_export_output if not x[0]])

    if len(errors) > 0:
        error_report = []
        error_report.append("-------------------------------------")
        error_report.append("These are the errors that were found:")
        error_report.extend(errors)
        [print(err) for err in error_report]

        report = "export_tt_errors.txt"
        f = open(report, "w")
        for err in error_report:
            f.write(err)
            f.write("\n")
        f.close()
        print("Error report saved to", report)
    return errors


def main(helper):
    if helper.dryrun:
        print("DRYRUN of export_trans_table. No exports will occur")

    integrated_variable_export_output = []
    all_sample_errors = []
    sample_exports = []
    iv_exports = []
    exports = []

    if helper.all:
        helper.allsamples = True
        helper.allvars = True

    # svars from a single sample
    if helper.sample:
        sample_exports, all_sample_errors = _export_sample(all_sample_errors, helper)

    elif helper.allsamples:
        sample_exports, all_sample_errors = _export_all_sample_tts(
            all_sample_errors, helper
        )

    # all integrated variables
    if helper.listvars or helper.allvars or helper.variable:
        integrated_variable_export_output = _export_integrated_variables(helper)
        # both values (success, result) of tuples in "integrated_variable_export_output"
        # need to return True for this to be an export
        iv_exports = [x[1] for x in integrated_variable_export_output if x[0] and x[1]]

        iv_exports.sort()
        if helper.dryrun:
            if len(iv_exports):
                print("These integrated variables would have been exported:")
                [print(export) for export in iv_exports]
            else:
                print("Dryrun: No integrated variables need exporting")

    # print error report
    # _error_report(all_sample_errors, integrated_variable_export_output)

    if not helper.dryrun:
        if iv_exports or sample_exports:
            errors = export_to_sqlite(iv_exports, sample_exports, helper)
            for e in errors:
                integrated_variable_export_output.append((False, e))
            exports = sample_exports + iv_exports
            _export_report(exports)

        if helper.allvars:
            # remove cruft from export directories
            cruft = clean_up_integrated_export_directory(iv_exports, helper)
            clean_up_sqlite_trans_table_table(cruft, helper)

    errors = _error_report(all_sample_errors, integrated_variable_export_output)

    return "Translation Tables", exports, errors
