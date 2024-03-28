#!/usr/bin/env python

"""Command-line wrapper for exporting web documents."""
from ipums.metadata import utilities, IPUMS
from ipums.metadata.exporters import ExportOpts, Exporter, SqliteMetadataDumper
from joblib import Parallel, delayed
from pathlib import Path
import argparse
import sys


def export_web_doc(word_path, xml_web_docs_folder, helper):
    """export a web document from the project to xml."""

    outfile = word_path.stem + ".xml"
    export_path = xml_web_docs_folder / outfile

    if ".doc" in word_path.suffix:
        try:
            export = Exporter(
                product=helper.product,
                force=helper.force,
                debug=helper.debug,
                db_file=helper.db_file,
            )
            res = export.export_doc_to_file(infile=word_path, outfile=export_path)
            if res:
                if res == "*":
                    return (True, word_path.name)
                else:
                    return (True, None)
            else:
                return (False, "!" + word_path.name + " did not export")
        except Exception as e:
            return (False, " ".join(["!", word_path.name, ":", str(e)]))
    elif ".xml" in word_path.suffix:
        try:
            # subprocess.run(["cp", str(word_path), export_path])
            export_path.write_text(word_path.read_text())
            return (True, word_path.name)
        except Exception as e:
            return (False, " ".join(["!", word_path.name, ":", str(e)]))
    else:
        return (False, f"! {word_path.name} is not .doc or .xml")


def export_to_sqlite(doc_list, helper, drop_table=False):
    if "no_sqlite" not in helper.product.project.config and not helper.debug:
        print("Updating web documents in sqlite database...", end="")
        dump = SqliteMetadataDumper(
            product=helper.product, verbose=helper.verbose, db_file=helper.db_file
        )
        if drop_table:
            dump.drop_web_docs_table()
        dump.update_web_documents(doc_list)
        print("DONE!")


def main(helper):
    out = []

    proj = helper.product.project
    constants = helper.product.constants

    xml_web_docs_folder = Path(proj.path) / constants.xml_metadata_web_docs
    web_docs = [Path(proj.path) / d for d in constants.metadata_web_docs]

    if helper.debug:
        xml_web_docs_folder = Path("/tmp/metadata/web_docs")

    if not xml_web_docs_folder.exists():
        xml_web_docs_folder.mkdir(parents=True)

    if helper.listall or helper.all:
        if helper.listall:
            print(f"Web documents listed for {helper.project}:")
            [print(f.name) for f in web_docs]
            sys.exit()
        else:
            print("Exporting web documents:")
            for f in utilities.progress_bar(web_docs, desc="Web Documents"):
                out.append(export_web_doc(f, xml_web_docs_folder, helper))

    else:
        if helper.docs is None:
            raise argparse.ArgumentTypeError(
                "You must specify at least one web document with --docs "
                + "or all the documents with --all"
            )
        else:
            helper.force = True
            for f in [f for f in web_docs if f.stem in helper.docs]:
                print("Exporting", f.name)
                (success, result) = export_web_doc(f, xml_web_docs_folder, helper)
                out.append((success, result))
                if success and result:
                    print(result)
                elif not success:
                    print("Export failed:", result, file=sys.stderr)
    print("ALL DONE!")

    errors = [x[1] for x in out if not x[0]]
    if len(errors) > 0:
        error_report = []
        error_report.append("-------------------------------------")
        error_report.append("These are the errors that were found:")
        error_report.extend(errors)
        # these get a bit verbose for the console...
        # [print(err) for err in error_report]

        report = "export_web_doc_errors.txt"
        f = open(report, "w")
        for err in error_report:
            f.write(err)
            f.write("\n")
        f.close()
        print("Error report saved to", report)

    # both values (success, result) of tuples in "out"
    # need to return True for this to be an export
    exports = [x[1] for x in out if x[0] and x[1]]

    if len(exports) > 0:
        if helper.all and helper.force:
            drop_table = True
        else:
            drop_table = False
        export_to_sqlite(exports, helper, drop_table)
        exports.sort()

        report = "exported_web_docs.txt"
        f = open(report, "w")
        for export in exports:
            f.write(export)
            f.write("\n")
        f.close()
        print("Exported documents list saved to", report)
    else:
        print(f"No web_docs to export for {helper.project}")

    return "Web Docs", exports, errors
