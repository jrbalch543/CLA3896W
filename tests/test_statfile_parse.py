import pytest
import ipums.tools.statfile_parse as stp
import polars as pl
import sys


@pytest.mark.parametrize("suffix", ["do", "sps", "sas"])
def test_files(test_data_dir, ref_data_dir, suffix, capsys):
    file_name = f"statfile_parse/manualMulti.{suffix}"
    full = test_data_dir / file_name
    sys.argv = ["statfile_parse", str(full)]
    stp.main()
    out = capsys.readouterr().out
    with open("/tmp/out.tsv", "w") as f:
        f.write(out)
    df = pl.read_csv("/tmp/out.tsv", separator="\t")
    expected = pl.read_csv(ref_data_dir / "statfile_parse/expected.tsv", separator="\t")

    assert df.drop_nulls(subset="RecordType").equals(
        expected.drop_nulls(subset="RecordType")
    )


def test_err(capsys):
    file_name = "ajkbcjkacjkasc.py"
    sys.argv = ["statfile_parse", str(file_name)]
    stp.main()
    out = capsys.readouterr().out
    assert "ERROR: File did not end" in out
