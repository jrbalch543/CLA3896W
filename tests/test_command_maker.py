import pytest
from argparse import ArgumentError
from ipums.tools.command_maker import *


class ExampleArgs:
    def __init__(
        self,
        command=None,
        values=None,
        f=None,
        wc=None,
        r=None,
        n=False,
        sp=False,
        a=False,
    ) -> None:
        self.command = command
        self.values = values
        self.FILE = f
        self.X = wc
        self.r = r
        self.n = n
        self.sp = sp
        self.aro = a


def test_ppf(test_data_dir):
    ta = ExampleArgs(
        f=test_data_dir / "command_maker/command_maker.txt",
        command="1",
        values=["2", "3"],
    )
    process_potential_file(ta)
    assert ta.command == ["cp data**.dat tests/data**.dat"]
    assert ta.values == ["1", "2", "3"]

    ta = ExampleArgs(
        f=None, command="cp data**.dat tests/data**.dat", values=["1", "2", "3"]
    )
    process_potential_file(ta)
    assert ta.command == ["cp data**.dat tests/data**.dat"]
    assert ta.values == ["1", "2", "3"]


def test_spf():
    ta = ExampleArgs(
        command="cp data_%s.dat test/data%d.txt",
        values=["a", "1", "b", "2", "c", "3.0"],
        sp=True,
        a=False,
    )
    process_potential_file(ta)
    fin = sprintf(ta)
    assert fin == [
        "cp data_a.dat test/data1.txt",
        "cp data_b.dat test/data2.txt",
        "cp data_c.dat test/data3.txt",
    ]
    assert type(ta.values[0]) == str
    assert type(ta.values[1]) == int
    assert type(ta.values[5]) == float

    ta = ExampleArgs(
        command="cp data_%s.dat test/data%d.txt",
        values=["a", "1", "b", "2", "c", "3.0", "4"],
        sp=True,
        a=False,
    )
    process_potential_file(ta)
    with pytest.raises(ValueError, match="Too many"):
        fin = sprintf(ta)

    ta = ExampleArgs(
        command="cp data_%s.dat test/data%d.txt",
        values=["a", "1", "b", "2", "c"],
        sp=True,
        a=False,
    )
    process_potential_file(ta)
    with pytest.raises(ValueError, match="Too many"):
        fin = sprintf(ta)

    ta = ExampleArgs(
        command="cp data_%d.dat test/data%d.txt",
        values=["a", "1", "b", "2", "c", "3.0"],
        sp=True,
        a=False,
    )
    process_potential_file(ta)
    with pytest.raises(ValueError, match="Incorrect value types"):
        fin = sprintf(ta)


def test_aro():
    ta = ExampleArgs(
        command=["cp data_%s.dat test/data%d.txt"],
        values=["a", "b", "c", "1", "2", "3.0"],
        sp=True,
        a=True,
    )
    fin = sprintf(ta)
    assert fin == [
        "cp data_a.dat test/data1.txt",
        "cp data_b.dat test/data2.txt",
        "cp data_c.dat test/data3.txt",
    ]

    ta = ExampleArgs(
        command=["cp data_%s.dat test/data%d.txt"],
        values=["a", "b", "c", "1", "2", "3.0", "e"],
        sp=True,
        a=True,
    )
    with pytest.raises(ValueError, match="Invalid number"):
        fin = sprintf(ta)

    ta = ExampleArgs(
        command=["cp data_%s.dat test/data%d.txt"],
        values=["a", "b", "c", "1", "2", "3.0", "e", "4"],
        sp=True,
        a=True,
    )
    with pytest.raises(ValueError, match="Incorrect value type"):
        fin = sprintf(ta)

    ta = ExampleArgs(
        command=["cp data.dat test/data.txt"],
        values=["a", "b", "c", "1", "2", "3.0"],
        sp=True,
        a=True,
    )
    with pytest.raises(ValueError, match="Invalid number"):
        fin = sprintf(ta)


def test_normal():
    ta = ExampleArgs(
        command=["cp data_**.dat test/data_**.txt"], values=["a", "b", "c"], wc="**"
    )
    fin = normal(ta)
    assert fin == [
        "cp data_a.dat test/data_a.txt",
        "cp data_b.dat test/data_b.txt",
        "cp data_c.dat test/data_c.txt",
    ]

    ta = ExampleArgs(
        command=["cp data_xx.dat test/data_xx.txt"], values=["a", "b", "c"], wc="xx"
    )
    fin = normal(ta)
    assert fin == [
        "cp data_a.dat test/data_a.txt",
        "cp data_b.dat test/data_b.txt",
        "cp data_c.dat test/data_c.txt",
    ]

    ta = ExampleArgs(
        command=["cp data_**.dat test/data_**.txt"], values=["a", "b", "c"], wc="xx"
    )
    with pytest.warns(UserWarning, match="No wildcards"):
        fin = normal(ta)
        assert fin == [
            "cp data_**.dat test/data_**.txt",
            "cp data_**.dat test/data_**.txt",
            "cp data_**.dat test/data_**.txt",
        ]


def test_det_fin(capfd):
    ta = ExampleArgs(
        r=False,
        n=False,
    )
    determine_final(ta, ["date", "date"])
    assert capfd.readouterr().out == "date\ndate\n"

    ta = ExampleArgs(
        r=True,
        n=False,
    )
    determine_final(ta, ["basename /pkg/ipums"])
    assert capfd.readouterr().out == "ipums\n"

    ta = ExampleArgs(
        r=False,
        n=True,
    )
    determine_final(ta, ["date", "date"])
    assert capfd.readouterr().out == "date date \n"
