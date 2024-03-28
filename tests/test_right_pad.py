import pytest
from ipums.tools.right_pad import *
from dataclasses import dataclass
from typing import List


@dataclass
class ExampleArgs:
    data_file: str = None
    len: int = None
    p: str = " "
    rt: List[str] = None
    rt_len: List[int] = None
    s: int = None
    w: int = None
    d: bool = False


def test_pad_line():
    assert pad_line("test_line", "x", 9) == "test_line"
    assert pad_line("test_line", "x", 15) == "test_linexxxxxx"
    with pytest.raises(ValueError, match="Given line leng"):
        pad_line("test_line", "x", 8)
    with pytest.raises(TypeError, match="Padding char"):
        pad_line("test_line", "xx", 15)


def test_asserter():
    ta = ExampleArgs(
        rt=["a", "b"],
        rt_len=[1, 2],
        s=1,
        w=1,
    )
    asserter(ta)  # No error

    ta = ExampleArgs(
        rt=["a", "b"],
        s=1,
        w=1,
    )
    with pytest.raises(AssertionError, match="Need to supply both"):
        asserter(ta)  # Missing rt_len

    ta = ExampleArgs(
        rt_len=[1, 2],
        s=1,
        w=1,
    )
    with pytest.raises(AssertionError, match="Need to supply both"):
        asserter(ta)  # Missing rt valuess

    ta = ExampleArgs(
        rt=["a"],
        rt_len=[1, 2],
        s=1,
        w=1,
    )
    with pytest.raises(AssertionError, match="Need to supply equal"):
        asserter(ta)  # Uneven rt and rt_len lengths

    ta = ExampleArgs(
        rt=["a", "b"],
        rt_len=[1, 2],
        w=1,
    )
    with pytest.raises(AssertionError, match="Need to specify"):
        asserter(ta)  # Missing s

    ta = ExampleArgs(
        rt=["a", "b"],
        rt_len=[1, 2],
        s=1,
    )
    with pytest.raises(AssertionError, match="Need to specify"):
        asserter(ta)  # Missing w


def test_rec_type_padder():
    wta = ExampleArgs(
        rt=["a", "b"],
        rt_len=[7, 8],
        s=1,
        w=1,
    )
    bta1 = ExampleArgs(  # Fail asserter
        rt=["a", "b"],
        rt_len=[7],
        s=1,
        w=1,
    )
    bta2 = ExampleArgs(  # S is out of range
        rt=["a", "b"],
        rt_len=[7, 8],
        s=60,
        w=1,
    )
    bta3 = ExampleArgs(  # Line too short
        rt=["a", "b"],
        rt_len=[3, 3],
        s=1,
        w=1,
    )

    assert rec_type_padder("a_line", wta) == "a_line "
    with pytest.raises(AssertionError, match="Need to supply"):
        rec_type_padder("a_line", bta1)
    with pytest.raises(ValueError, match="Could not find any"):
        rec_type_padder("a_line", bta2)
    with pytest.raises(ValueError, match="Given line len"):
        rec_type_padder("a_line", bta3)
    with pytest.raises(ValueError, match="Could not find any"):  # No rectype match
        rec_type_padder("c_line", wta)


def test_read_df(test_data_dir):
    df1 = ExampleArgs(data_file=test_data_dir / "right_pad/right_pad.txt", len=40)
    ol = read_data_file(df1)
    assert ol == [
        "a01110100011001010111001101110100       ",
        "b01110100011001010111001101110100       ",
        "a01110100011001010111001101110100       ",
    ]

    df2 = ExampleArgs(
        data_file=test_data_dir / "right_pad/right_pad.txt",
        len=40,
        rt=["a", "b"],
        rt_len=[36, 40],
        s=1,
        w=1,
    )
    ol = read_data_file(df2)
    assert ol == [
        "a01110100011001010111001101110100   ",
        "b01110100011001010111001101110100       ",
        "a01110100011001010111001101110100   ",
    ]

    df3 = ExampleArgs(
        data_file=test_data_dir / "right_pad/right_pad.txt", len=400, p="xx"
    )
    with pytest.raises(TypeError, match="Padding character"):
        ol = read_data_file(df3)

    df4 = ExampleArgs(
        data_file=test_data_dir / "right_pad/right_pad.txt",
        rt=["a", "b"],
        rt_len=[450, 680],
        s=60,
        w=1,
    )
    with pytest.raises(ValueError, match="Could not find any match"):
        ol = read_data_file(df4)

    df4 = ExampleArgs(
        data_file=test_data_dir / "right_pad/dne.txt",
        len=40,
    )
    with pytest.raises(FileNotFoundError, match="Unable to find"):
        ol = read_data_file(df4)
