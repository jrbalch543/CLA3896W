import pytest
import ipums.tools.tt_creator as ttc
from ipums.metadata import db
import shutil
import polars as pl


ROW1 = {
    "new_var": "NEWTESTVAR1",
    "mnemonics": "TESTVAR",
    "Samples": "test2023a,test2023b",
    "Universe": "FILL IN UNIVERSE",
    "Desc": "FILL IN DESC",
    "RecType": None,
    "NoRecode": 1,
    "UnivLabel": "FILL IN UNIV LABEL",
}

ROW2 = {
    "new_var": "NEWTESTVAR2",
    "mnemonics": "TESTVAR,TESTSVAR",
    "UnivLabel": "FILL IN UNIV LABEL",
}

ROW3 = {"mnemonics": "TESTVAR,TESTSVAR", "UnivLabel": "FILL IN UNIV LABEL"}

ROW4 = {"new_var": "NEWTESTVAR4", "mnemonics": "TESTVAR"}

ROW5 = {
    "new_var": "NEWTESTVAR5",
    "mnemonics": "TESTVAR,TESTSVAR",
    "Samples": "test2023b,test2023a",
}

ROW6 = {
    "new_var": "NEWTESTVAR6",
    "mnemonics": "TESTVAR,TESTSVAR",
    "Samples": "test2023a,test2023b",
}


class ExampleArgs:
    def __init__(self, m, c, nv, d):
        self.m = m
        self.collate = c
        self.nv = nv
        self.debug = d


def test_process_row(mock_project, test_int_populate):
    with pytest.raises(KeyError, match="Metadata Database cannot find"):
        dne = db.Variable("NEWTESTVAR", mock_project)

    v, row = ttc.process_row(ROW1, mock_project)
    now_exists = db.Variable("NEWTESTVAR1", mock_project)
    assert v == now_exists
    assert v.label.value == "FILL IN DESC"
    assert row["mnemonics"] == ["TESTVAR"]
    assert row["Samples"] == "test2023a,test2023b"

    v, row = ttc.process_row(ROW2, mock_project)
    now_exists = db.Variable("NEWTESTVAR2", mock_project)
    assert v == now_exists
    assert row["mnemonics"] == ["TESTVAR", "TESTSVAR"]
    assert row["Samples"] == None

    with pytest.raises(KeyError, match="NEED NEW VARIABLE NAME"):
        v, row = ttc.process_row(ROW3, mock_project)


def test_attach_samples(mock_project, test_int_populate):
    ta1 = ExampleArgs(False, False, False, False)

    # No samples, > 1 mnemonics
    v, r1 = ttc.process_row(ROW2, mock_project)
    as1 = ttc.attach_samples(r1, mock_project, ta1)
    assert as1["TESTVAR"] == ["test2023a", "test2023b"]
    assert as1["TESTSVAR"] == ["test2023a"]

    # No samples, 1 mnemonic
    v, r2 = ttc.process_row(ROW4, mock_project)
    as2 = ttc.attach_samples(r2, mock_project, ta1)
    assert as2 == {"TESTVAR": ["test2023a", "test2023b"]}

    ta2 = ExampleArgs(False, True, False, False)
    # collate, samp, samp == var, integrations exist
    v, r3 = ttc.process_row(ROW5, mock_project)
    as3 = ttc.attach_samples(r3, mock_project, ta2)
    assert as3 == {"TESTVAR": ["test2023b"], "TESTSVAR": ["test2023a"]}

    # collate, samp, samp == var, integrations DNE
    with pytest.raises(ValueError, match="INTEGRATION DOES NOT EXIST"):
        v, r4 = ttc.process_row(ROW6, mock_project)
        as4 = ttc.attach_samples(r4, mock_project, ta2)

    # collate, samp, samp != var
    v, r5 = ttc.process_row(ROW1, mock_project)
    with pytest.raises(ValueError, match="CANNOT COLLATE"):
        as5 = ttc.attach_samples(r5, mock_project, ta2)

    # Samples, no collate
    as6 = ttc.attach_samples(r5, mock_project, ta1)
    assert as6 == {"TESTVAR": ["test2023a", "test2023b"]}


def test_add_integrations(mock_project, test_int_populate):
    ta1 = ExampleArgs(False, False, False, False)
    v1, r1 = ttc.process_row(ROW2, mock_project)
    as1 = ttc.attach_samples(r1, mock_project, ta1)
    ttc.add_integrations(v1, r1, as1, mock_project, ta1)
    assert len(v1.integrations) == 2
    i1 = db.Integration(v1, "test2023a", mock_project)
    i2 = db.Integration(v1, "test2023b", mock_project)
    assert i1 in v1.integrations
    assert i2 in v1.integrations
    assert db.Variable("TESTSVAR", mock_project) in i1.sources

    ta2 = ExampleArgs(False, False, True, False)
    v2, r2 = ttc.process_row(ROW1, mock_project)
    as2 = ttc.attach_samples(r2, mock_project, ta2)
    ttc.add_integrations(v2, r2, as2, mock_project, ta2)
    assert len(v2.integrations) == 2
    i1 = db.Integration(v2, "test2023a", mock_project)
    i2 = db.Integration(v2, "test2023b", mock_project)
    assert i1 in v2.integrations
    assert i2 in v2.integrations
    assert db.Variable("TESTSVAR", mock_project) not in i1.sources


def test_add_universe(mock_project, test_int_populate):
    ta1 = ExampleArgs(False, False, False, False)
    v1, r1 = ttc.process_row(ROW2, mock_project)
    as1 = ttc.attach_samples(r1, mock_project, ta1)
    ttc.add_integrations(v1, r1, as1, mock_project, ta1)
    ttc.add_universe(v1, r1, as1, mock_project)
    with pytest.raises(KeyError, match="Metadata Database cannot find"):
        v1.universe

    ta2 = ExampleArgs(False, False, True, False)
    v2, r2 = ttc.process_row(ROW1, mock_project)
    as2 = ttc.attach_samples(r2, mock_project, ta2)
    ttc.add_integrations(v2, r2, as2, mock_project, ta2)
    ttc.add_universe(v2, r2, as2, mock_project)
    u = db.Universe("NEWTESTVAR1", mock_project)


def test_wtt(mock_project, test_int_populate, ref_data_dir):
    ta1 = ExampleArgs(True, False, False, False)
    v1, r1 = ttc.process_row(ROW2, mock_project)
    as1 = ttc.attach_samples(r1, mock_project, ta1)
    ttc.add_integrations(v1, r1, as1, mock_project, ta1)
    ttc.add_universe(v1, r1, as1, mock_project)
    ttc.write_trans_table(v1, mock_project, ta1)
    validate_tt(ROW2, ref_data_dir)

    ta2 = ExampleArgs(False, False, False, False)
    v2, r2 = ttc.process_row(ROW1, mock_project)
    as2 = ttc.attach_samples(r2, mock_project, ta2)
    ttc.add_integrations(v2, r2, as2, mock_project, ta2)
    ttc.add_universe(v2, r2, as2, mock_project)
    ttc.write_trans_table(v2, mock_project, ta2)
    validate_tt(ROW1, ref_data_dir)
    shutil.rmtree("new_TTs")


def test_search_samps(mock_project, test_int_populate):
    ta1 = ExampleArgs(True, False, False, False)
    v1, r1 = ttc.process_row(ROW2, mock_project)
    as1 = ttc.attach_samples(r1, mock_project, ta1)
    for k in as1:
        assert "test2023a" in ttc.search_samples(db.Variable(k, mock_project))
        assert ttc.search_samples(db.Variable(k, mock_project), "P") == []


def validate_tt(row, ref_data):
    written = pl.read_excel(f"new_TTs/{row['new_var']}.xlsx")
    expected = pl.read_excel(ref_data / f"tt_creator/{row['new_var']}.xlsx")
    assert written.equals(expected)
