from ipums.metadata import db, files
import argparse
import polars as pl
from itertools import chain
from ipums.metadata.db.models import TtSamplevariables
import os


def build_parser():
    parser = argparse.ArgumentParser(
        prog="tt_creator.py",
        description="""
-----------------------
TT Creator
-----------------------
This program creates new Translation Table files for a microdata project defined by a set of information given in a csv config file.
Using mnemonics and samples given in the config, sample data dictionaries are read, source variables found, and Translation Tables are populated with informaton.
Universe, Description, Rectype, NoRecode, and Universe Label are all populated as well, all given in the config file.

The expected format of the config csv is as follows:
new_var,mnemonic(s),Sample(s),Universe,Desc,Rectype,NoRecode,UnivLabel

    new_var: The name of the new integrated variable (also will be the name of TT)

# Limited to just variables, as no good way to look for mnemonics:    
mnemonics(s): The name of the mnemomnic (or mnemonics) to look for in the sample, which will also go into the TT in teh desc cell.
Note: mnemonics have to exist in the db rn, but that isn't the case in the current script
    Sample(s): Sample is the list of samples to search DDs for matches on mnemonic(s). If it finds a match, a column for that sample will be created as well as a line in the <univ> section. This list must be comma-delimited.

    Universe: Universe is the text that will go into column C of the <univ> section

    Desc: Desc is the description that get written to cell B3 of the TT

    Have not solved:    Rectype: Rectype is oprional, but if given will only match DD mnemonics of that rectype

    NoRecode: NoRecode is optional, but if given will fill in the norecode row of the TT with the value in this column

    UnivLabel: UnivLabel is optional, but if given will fill in column B in the <univ> block with the value in this column

Please use quotation marks around multiple samples or mnemonics if using more than one. You'll run into issues otherwise.
Any line that starts with a # is a comment line and will be ignored. Blank lines will also be ignored.

The Sample column is optional. If nothing is entered for Sample, it will look at every data dictionary in the project for matching mnemonic(s)
For IHIS and MEPS, only samples that match ihis or meps in the GROUP column of samples.xlsx will be used.

Example config lines:
    # look in all sample DDs for mnemonic qmarst to populate the TT marst_tt.xls
    marst,qmarst,,This is a test universe,This is a test description
    # look in 2013a and 2013b sample DDs for mnemonics mn_a and mn_b to populate newvar_tt.xls
    # this is a norecode of 2 variable, and needs a universe label of "Foobar"
    newvar,\"mn_a,mn_b\",\"2013a,2013b\",This is a test universe,This is a test description,,2,Foobar
""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "project", help="The name of an IPUMS microdata project.", type=str
    )
    parser.add_argument(
        "config",
        help="Path to a CSV file of metadata to use to create new translation table(s)",
    )
    parser.add_argument(
        "-m",
        "--multisample_column",
        dest="m",
        action="store_true",
        help="Puts all samples given on a row into one column of the translation table. This is for use in e.g. CPS, where multi-sample columns are standard practice.",
    )
    parser.add_argument(
        "-c",
        "--collate",
        action="store_true",
        help="This option collates listed mnemonics to listed samples rather than trying to match every mnemonic to each sample.",
    )
    parser.add_argument(
        "-nv",
        "--nosvar",
        dest="nv",
        action="store_true",
        help="This option allows the creation of translation tables that include samples, norecode, and universe information, but have no svars.",
    )
    parser.add_argument(  # Default rn
        "-d",
        dest="debug",
        action="store_true",
        help="Debug. Print TT to screen rather than to file.",
    )
    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    df = pl.read_csv(args.config, comment_prefix="#")
    proj = db.Project(args.project)

    for row in df.iter_rows(named=True):
        v, row = process_row(row, proj)
        attached_samples = attach_samples(row, proj, args)
        add_integrations(v, row, attached_samples, proj, args)
        add_universe(v, row, attached_samples, proj)
        write_trans_table(v, proj, args)


def process_row(row, proj):
    all_keys = [
        "new_var",
        "mnemonics",
        "Samples",
        "Universe",
        "Desc",
        "Rectype",
        "NoRecode",
        "UnivLabel",
    ]
    try:
        v = db.Variable.new(row["new_var"], proj)
        if v:
            row = dict.fromkeys(all_keys, None) | row
            v.label.update(row["Desc"])
            row["mnemonics"] = row["mnemonics"].split(",")
            return v, row
        else:
            raise ValueError(f"VARIABLE {row['new_var']} ALREADY EXISTS")
    except KeyError as e:
        raise KeyError("NEED NEW VARIABLE NAME IN CONFIG FILE")


def attach_samples(row, proj, args):
    if not row["Samples"]:
        if len(row["mnemonics"]) > 1:
            attached_samples = {
                mnem: search_samples(db.Variable(mnem, proj), row["Rectype"])
                for mnem in row["mnemonics"]
            }
        else:
            mnem_v = db.Variable(row["mnemonics"][0], proj)
            attached_samples = {mnem_v.name: search_samples(mnem_v, row["Rectype"])}
    elif args.collate:
        mnemonics = row["mnemonics"]
        samples = row["Samples"].split(",")
        if len(mnemonics) == len(samples):
            attached_samples = {
                mnemonic: [samples[i]] for i, mnemonic in enumerate(mnemonics)
            }
            for mnem, samp in attached_samples.items():
                mnem_v = db.Variable(mnem, proj)
                if samp[0] not in search_samples(mnem_v, row["Rectype"]):
                    raise ValueError(
                        f"INTEGRATION DOES NOT EXIST: Integration({mnem}, {samp})"
                    )
        else:
            raise ValueError(
                f"CANNOT COLLATE WITH UNEQUAL LENGTH MNEMONICS GIVEN AND SAMPLES\n{len(mnemonics)} MNEMONIC(S) GIVEN: {mnemonics}\n{len(samples)} SAMPLE(S) GIVEN: {samples}"
            )
    else:
        mnemonics = row["mnemonics"]
        attached_samples = {}
        for mnemonic in mnemonics:
            mnem_v = db.Variable(mnemonic, proj)
            samples = search_samples(mnem_v, row["Rectype"])
            attached_samples.update(
                {
                    mnemonic: [
                        sample
                        for sample in samples
                        if sample in row["Samples"].split(",")
                    ]
                }
            )

    return attached_samples


def add_integrations(var, row, attached_samples, proj, args):
    for mnemonic in attached_samples.keys():
        for sample in attached_samples[mnemonic]:
            var.add_integration(sample)
            new_i = var.get_integration(sample)
            new_i.norecode.update(row["NoRecode"])
            if not args.nv:
                old_i = db.Integration(mnemonic, sample, proj)
                for source in old_i.sources:
                    new_i.add_source(source)


def add_universe(var, row, attached_samples, proj):
    if row["Universe"]:
        u = db.Universe.new(
            var,
            proj,
            samp_statement=row["Universe"],
            univ_statement=row["UnivLabel"] if row["UnivLabel"] else "",
        )
        us = u.get_statement(id=1)
        for sample in list(set(chain.from_iterable(attached_samples.values()))):
            us.add_sample(sample)


def write_trans_table(var, proj, args):
    if args.m:
        tt = files.TranslationTable(var, proj, collapse=True)
    else:
        tt = files.TranslationTable(var, proj)
    if args.debug:
        print(tt.make_full_tt())
    else:
        os.makedirs("new_TTs", exist_ok=True)
        tt.write_excel(f"new_TTs/{tt.name}.xlsx")


def search_samples(var, rectype=None):
    q = (
        var.session.query(TtSamplevariables)
        .filter(TtSamplevariables.variable == var.name)
        .with_entities(TtSamplevariables.sample)
    )
    if rectype:
        samples = [r.sample for r in q.filter(TtSamplevariables.rectype == rectype)]
    else:
        samples = [r.sample for r in q]
    return samples


if __name__ == "__main__":
    main()
