from ipums.metadata import utilities
from ipums.metadata.exporters import SqliteMetadataDumper
from pathlib import Path
import sys


def export_editing_rule(rule_path, rules_folder):
    outfile = rule_path.name
    export_path = rules_folder / outfile

    if ".do" in rule_path.suffix:
        try:
            export_path.write_text(rule_path.read_text())
            return (True, rule_path.stem)
        except Exception as e:
            return (False, " ".join(["!", rule_path.name, ":", str(e)]))

    else:
        return (
            False,
            f"! {rule_path.name}: ValueError: {rule_path.suffix} is not a currently supported rule format.",
        )


def export_to_sqlite(rule_list, helper):
    if "no_sqlite" not in helper.product.project.config and not helper.debug:
        print("Updating editing rules in sqlite database...", end="")
        dump = SqliteMetadataDumper(
            product=helper.product, verbose=helper.verbose, db_file=helper.db_file
        )
        dump.update_editing_rules(rule_list)
        print("Done!")


def main(helper):
    out = []
    proj = helper.product.project
    constants = helper.product.constants
    if not proj.has_stata_editing_rules:
        print(f"Editing Rules are not currently supported metadata for {proj.name}.")
        return "Editing Rules", [], []

    # Stata is currently the only syntax supported
    xml_rules_folder = (
        Path(proj.path)
        / constants.xml_metadata_editing_rules
        / "stata_variables_to_edit"
    )
    editing_rules_folder = (
        Path(proj.path) / constants.metadata_editing_rules / "stata_variables_to_edit"
    )

    if helper.debug:
        xml_rules_folder = Path("/tmp/metadata/editing_rules")

    if not xml_rules_folder.exists():
        xml_rules_folder.mkdir(parents=True)

    # Exporting of editing rules is all or nothing
    editing_rules = [f for f in editing_rules_folder.glob("*.do")]

    if helper.listall:
        print("Editing Rules listed for", helper.project, ":")
        [print(f.name) for f in editing_rules]
        sys.exit()
    else:
        print("Exporting editing rules:")
        for f in utilities.progress_bar(editing_rules, desc="Editing Rules"):
            out.append(export_editing_rule(f, xml_rules_folder))

    print("ALL DONE!")

    errors = [x[1] for x in out if not x[0]]
    if errors:
        error_report = []
        error_report.append("-------------------------------------")
        error_report.append("These are the errors that were found:")
        error_report.extend(errors)

        report = "export_er_errors.txt"
        with open(report, "w") as f:
            for err in error_report:
                f.write(err)
                f.write("\n")

        print("Error report saved to", report)

    exports = [x[1] for x in out if x[0] and x[1]]

    if exports:
        export_to_sqlite(exports, helper)
        exports.sort()

        report = "exported_editing_rules.txt"
        with open(report, "w") as f:
            for export in exports:
                f.write(export)
                f.write("\n")
        print("Exported editing rules list saved to", report)

    return "Editing Rules", exports, errors
