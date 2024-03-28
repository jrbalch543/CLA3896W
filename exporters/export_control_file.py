"""Command-line wrapper for export_tools"""
from ipums.metadata import utilities
from ipums.metadata.exporters import (
    ExportControlFileCsv,
    ExportSvarsCsv,
    ExportAllDDSvarsCsv,
)

from pathlib import Path
import sys


def _export_all(helper):
    """Export all control file data from a project to CSV."""

    # samples is the samples CF object
    samples = helper.product.samples

    # First find the size for the progress bar
    control_files = [
        f
        for f in helper.product.constants.metadata_control_files
        if Path(f"{helper.product.project.path}/{f}").exists()
    ]
    # CFs + samps + 2 for extra yields in gen: vars_quick and completed
    total_size = len(control_files) + len(samples.all_samples) + 2

    export = ExportAllDDSvarsCsv(
        list_only=helper.listall,
        force=helper.force,
        dryrun=helper.dryrun,
        verbose=helper.verbose,
        product=helper.product,
        db_file=helper.db_file,
    )
    gen = export.to_csv_generator()
    export.export_the_dds()
    results = []
    if helper.listall:
        for result in gen:
            print(result["text"])
    else:
        for result in utilities.progress_bar(gen, total=total_size):
            results.append(result)

    return results


def _export_single_sample(helper):
    try:
        msg = None
        sample = helper.sample
        export = ExportSvarsCsv(
            product=helper.product,
            sample=sample,
            samples=helper.product.samples,
            force=helper.force,
            dryrun=helper.dryrun,
            verbose=helper.verbose,
            db_file=helper.db_file,
        )
        result = export.to_csv()
        if result["type"] == "error":
            msg = result["text"]
        else:
            print(result["text"])

    except AssertionError as e:
        msg = f"FAIL! (AssertionError): {sample} failed to export: {str(e)}"
        result = {"text": msg, "type": "error"}
    except KeyError as e:
        msg = f"FAIL! (KeyError): {sample} failed to export: {str(e)}"
        result = {"text": msg, "type": "error"}
    except UnicodeEncodeError as e:
        msg = f"FAIL! (UnicodeEncodeError): {sample} failed to export: {str(e)}"
        result = {"text": msg, "type": "error"}
    finally:
        if msg:
            print(msg, file=sys.stderr)
        return result


def _export_single_control_file(helper):
    try:
        cf = helper.cf
        msg = None
        export = ExportControlFileCsv(
            product=helper.product,
            cf_name=cf,
            force=helper.force,
            dryrun=helper.dryrun,
            verbose=helper.verbose,
            db_file=helper.db_file,
        )
        result = export.to_csv()
        if result["type"] == "error":
            msg = result["text"]
        else:
            print(result["text"])
    except AssertionError as e:
        msg = f"FAIL! (AssertionError): {cf} failed to export: {str(e)}"
        result = {"text": msg, "type": "error"}
    except KeyError as e:
        msg = f"FAIL! (KeyError): {cf} failed to export: {str(e)}"
        result = {"text": msg, "type": "error"}

    except UnicodeEncodeError as e:
        msg = f"FAIL! (UnicodeEncodeError): {cf} failed to export: {str(e)}"
        result = {"text": msg, "type": "error"}
    except FileNotFoundError as e:
        msg = f"FAIL! (FileNotFoundError): {cf} failed to export: {str(e)}"
        result = {"text": msg, "type": "error"}
    finally:
        if msg:
            print(msg, file=sys.stderr)
    return result


def main(helper):

    if helper.dryrun:
        print("*** --dryrun selected, no exports will be written ***")

    if "no_sqlite" in helper.product.project.config:
        print(
            f"WARN- {helper.project} is not configured to use sqlite", file=sys.stderr
        )
    results = []
    # svars from a single sample
    if helper.sample:
        results.append(_export_single_sample(helper))

    # everything
    elif not helper.cf or helper.cf == "all":
        samples = helper.product.samples
        samples.run_audit(audit_level="fail")
        if helper.all or helper.listall:
            results = _export_all(helper)
    # single control file export
    else:
        results.append(_export_single_control_file(helper))

    fresh_exports = [r["text"] for r in results if r["type"] == "ok"]
    error_exports = [r["text"] for r in results if r["type"] == "error"]
    if not helper.dryrun and len(results) > 1:
        print(len(results), "exports processed")
        print(len(fresh_exports), "freshly exported")
        print(len(error_exports), "errors")

    elif helper.dryrun and len(fresh_exports):
        print("Dryrun: these control file csv exports would have happened:")
        [print(Path(r).name) for r in fresh_exports]

    if len(error_exports) > 0:
        print("\nWARNING: Errors detected! Please analyze")
        print("\n".join(error_exports))
    else:
        print("\nSuccess!")

    return "Control Files", fresh_exports, error_exports
