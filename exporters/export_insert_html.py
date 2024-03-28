"""Command-line wrapper for exporting web documents."""
from ipums.metadata import utilities, IPUMS
from ipums.metadata.exporters import SqliteMetadataDumper
from pathlib import Path
import argparse
import sys


def export_to_sqlite(doc_list, helper, drop_table=False):
    product = helper.product
    if "no_sqlite" not in product.project.config:
        if helper.debug and not helper.db_file:
            helper.db_file = "/tmp/metadata/insert_html/metadata.db"
            Path.mkdir(Path(helper.db_file).parent, parents=True, exist_ok=True)
        print(f"Updating insert_html in sqlite db {helper.db_file}...", end="")
        dump = SqliteMetadataDumper(
            product=product, verbose=helper.verbose, db_file=helper.db_file
        )
        if drop_table:
            dump.drop_insert_html_table()
        dump.update_insert_html(doc_list)
        print("DONE!")


def main(helper):
    out = []

    constants = helper.product.constants
    insert_html_folder = (
        Path(helper.product.project.path) / constants.xml_metadata_insert_html
    )

    if helper.listall or helper.all:
        # all html files that don't start with ~
        html_docs = insert_html_folder.glob(r"[!~\.]*.html")
        if helper.listall:
            print("insert_html documents listed for", helper.project, ":")
            [print(f.name) for f in html_docs]
            sys.exit()
        else:
            print("Exporting insert_html documents:")
            [out.append((True, f.name)) for f in html_docs]
    else:
        if helper.docs is None:
            raise argparse.ArgumentTypeError(
                "You must specify at least one insert_html document with --docs "
                + "or all the documents with --all"
            )
        else:
            # helper.force = True
            for f in [insert_html_folder / f"{f}.html" for f in helper.docs]:
                out.append((True, f.name))

    errors = [x[1] for x in out if not x[0]]
    if len(errors) > 0:
        error_report = []
        error_report.append("-------------------------------------")
        error_report.append("These are the errors that were found:")
        error_report.extend(errors)
        # these get a bit verbose for the console...
        # [print(err) for err in error_report]

        report = "export_insert_html_errors.txt"
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
        if helper.all:
            drop_table = True
        else:
            drop_table = False
        export_to_sqlite(exports, helper, drop_table)
        exports.sort()

        report = "exported_insert_html.txt"
        f = open(report, "w")
        for export in exports:
            f.write(export)
            f.write("\n")
        f.close()
        print("Exported documents list saved to", report)
    else:
        print(f"No insert_html docs to export for {helper.project}")

    return "Insert HTML", exports, errors
