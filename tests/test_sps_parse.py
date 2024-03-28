import pytest
import ipums.tools.sps_parse as sps
import polars as pl


def test_parser():
    parsed = sps.build_parser().parse_args(["test_file_name.kenclkncwkl"])
    assert parsed.filename == "test_file_name.kenclkncwkl"

    given_others = sps.build_parser().parse_args(["stat_trans.sps", "-stat_trans"])
    assert given_others.filename == "stat_trans.sps"


def test_parse_file_sections(
    audit_datalist, audit_varlabels, audit_vallabels, test_data_dir
):
    # This file contains a mix of Stat Transfer elements and normal ones
    test_file = test_data_dir / "statfile_parse/manualMulti.sps"

    # This calls the parse_file_section under the hood for each group in the setting that it actually will be anyways
    file_sections = sps.build_file_sections(test_file, sps.COMBOS)

    # 4 working vars for each data type
    assert len(file_sections["DataList"]) == 12
    audit_datalist(file_sections["DataList"][0:4], ".")
    audit_datalist(file_sections["DataList"][4:8], "B")
    audit_datalist(file_sections["DataList"][8:], "C")

    assert len(file_sections["VarLabels"]) == 12
    audit_varlabels(file_sections["VarLabels"])

    assert len(file_sections["ValueLabels"]) == 16
    audit_vallabels(file_sections["ValueLabels"])


def test_zipper(audit_datalist, test_data_dir):
    test_file = test_data_dir / "statfile_parse/manualMulti.sps"
    file_sections = sps.build_file_sections(test_file, sps.COMBOS)
    sps.zipper(file_sections)
    audit_datalist(file_sections["DataList"][:4], ".", True)
    audit_datalist(file_sections["DataList"][4:8], "B", True)
    audit_datalist(file_sections["DataList"][8:], "C", True)


def test_build_df(test_data_dir, ref_data_dir):
    test_file = test_data_dir / "statfile_parse/manualMulti.sps"
    file_sections = sps.build_file_sections(test_file, sps.COMBOS)
    sps.zipper(file_sections)
    df = sps.build_df(file_sections["DataList"])
    assert df.columns == [
        "RecordType",
        "Var",
        "Col",
        "Wid",
        "Frm",
        "Value",
        "VarLabel",
        "ValueLabel",
        "VarLabelOrig",
        "ValueLabelOrig",
        "Freq",
        "Sel",
        "Notes",
        "Svar",
        "ValueSvar",
        "VarLabelSvar",
        "ValueLabelSvar",
        "UnivSvar",
        "NoRec",
        "NonTab",
        "Hide",
        "Decim",
        "String",
        "CommP",
        "CodeTy",
        "DDoc1",
        "Dtag1",
        "JDoc1",
        "JTag1",
        "DDoc2",
        "DTag2",
        "JDoc2",
        "JTag2",
    ]
    assert df.shape == (28, 33)
    expected = pl.read_csv(ref_data_dir / "statfile_parse/expected.tsv", separator="\t")
    assert df.equals(expected.drop_nulls(subset="RecordType"))


def test_integration(test_data_dir):
    sps.build_file_sections(
        test_data_dir / "statfile_parse/manualMulti.sps", sps.COMBOS
    )
    with pytest.raises(FileNotFoundError):
        sps.build_file_sections("kjbcjkabscjas;lc", sps.COMBOS)
