"""Command-line wrapper for exporting enumeration materials."""
from pprint import pprint
from pathlib import Path
import glob
import argparse
import os
import sys
import subprocess

from joblib import Parallel, delayed

from ipums.metadata import utilities, IPUMS
from ipums.metadata.exporters import ExportOpts, Exporter, SqliteMetadataDumper


def export_enum_material(word_path, helper, xml_enum_materials_folder):
    """export enumeration material form from project to xml."""

    outfile = word_path.stem + ".xml"
    export_path = xml_enum_materials_folder / outfile
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
        export_path.write_text(word_path.read_text())
        return (True, word_path.name)
    else:
        return (False, f"! {word_path.name} is not .doc or .xml")


def export_to_sqlite(doc_list, helper, drop_table=False):
    if not helper.product.project.no_sqlite and not helper.debug:
        print("Updating enum materials in sqlite database...", end="")
        dump = SqliteMetadataDumper(
            product=helper.product, verbose=helper.verbose, db_file=helper.db_file
        )
        if drop_table:
            dump.drop_enum_materials_table()
        dump.update_enum_materials(doc_list)
        print("DONE!")


def main(helper):
    out = []

    proj = helper.product.project
    constants = helper.product.constants

    xml_enum_materials_folder = Path(proj.path) / constants.xml_metadata_enum_materials
    enum_materials_folder = Path(proj.path) / constants.metadata_enum_materials

    if helper.debug:
        xml_enum_materials_folder = Path("/tmp/metadata/enum_materials")

    if not xml_enum_materials_folder.exists():
        xml_enum_materials_folder.mkdir(parents=True)

    if helper.listall or helper.all:
        # all doc and docx that don't start with ~

        emf_docs = glob.glob1(str(enum_materials_folder), "[!~]*.doc*")
        emf_xmls = glob.glob1(str(enum_materials_folder), "[!~]*.xml")

        emf_doc_stems = [Path(emf_doc).stem for emf_doc in emf_docs]
        emf_xml_stems = [Path(emf_xml).stem for emf_xml in emf_xmls]

        duplicate_formats = list(set(emf_doc_stems) & set(emf_xml_stems))
        if len(duplicate_formats) > 0:
            for df in duplicate_formats:
                dup_message = (
                    "Enumeration material "
                    + df
                    + " exists as both .doc and .xml files in the directory "
                    + str(enum_materials_folder)
                    + ". Enum materials must only exist in one format in this directory."
                )
                print(dup_message, file=sys.stderr)
                out.append((False, dup_message))
        emf = list(
            (set(emf_docs) - set([stem + ".doc" for stem in duplicate_formats]))
            | (set(emf_xmls) - set([stem + ".xml" for stem in duplicate_formats]))
        )

        allforms = [enum_materials_folder / f for f in emf]
        if helper.listall:
            print("Enumeration materials listed for", helper.project, ":")
            [print(f.name) for f in allforms]
            sys.exit()
        else:
            print("Exporting enumeration materials:")
            out = Parallel(n_jobs=100)(
                delayed(export_enum_material)(f, helper, xml_enum_materials_folder)
                for f in utilities.progress_bar(allforms, desc="Enum Materials")
            )
    else:
        if helper.forms is False:
            raise argparse.ArgumentTypeError(
                "You must specify at least one form with --forms "
                + "or all forms with --all"
            )
        else:
            helper.force = True
            allforms = [
                enum_materials_folder / Path(f).with_suffix(".doc")
                for f in helper.forms
            ]
            for f in allforms:
                if not f.exists() and f.with_suffix(".xml").exists():
                    f = f.with_suffix(".xml")

                if not f.exists():
                    print(
                        f"SKIPPED: neither {f.with_suffix('.doc').name} nor "
                        f"{f.with_suffix('.xml').name} exist"
                    )
                    continue
                print(f"Exporting {f.name}")
                (success, result) = export_enum_material(
                    f, helper, xml_enum_materials_folder
                )
                out.append((success, result))
                if not success:
                    print(f"Export failed: {result}", file=sys.stderr)
    print("ALL DONE!")

    errors = [x[1] for x in out if not x[0]]
    if len(errors) > 0:
        error_report = []
        error_report.append("-------------------------------------")
        error_report.append("These are the errors that were found:")
        error_report.extend(errors)
        # these get a bit verbose for the console...
        # [print(err) for err in error_report]

        report = "export_em_errors.txt"
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

        report = "exported_enum_materials.txt"
        f = open(report, "w")
        for export in exports:
            f.write(export)
            f.write("\n")
        f.close()
        print("Exported enumeration materials list saved to", report)
    else:
        print(f"No enum_materials to export for {helper.project}")

    return "Enum Materials", exports, errors
