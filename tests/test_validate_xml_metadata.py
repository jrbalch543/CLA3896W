import argparse
import pytest
import sys
import ipums.tools.validate_xml_metadata as vxm
import tempfile as tf

temp_working = tf.NamedTemporaryFile(suffix=".xml")
temp_broken = tf.NamedTemporaryFile(suffix=".xml")
temp_working.write(
    b"""<testtag
	title="Test Title"
	date="Today"
	Reason="Test"
	>
</testtag>"""
)
temp_broken.write(b"<testtag>")
temp_working.read()
temp_broken.read()


## Add test.xml and test_broken.xml to this test directory
class test_arg:
    def __init__(self, fileMode, quiet, smartQuotes, filenameOrProject):
        self.fileMode = fileMode
        self.quiet = quiet
        self.smartQuotes = smartQuotes
        self.filenameOrProject = filenameOrProject


def test_parser(monkeypatch):
    test_args = ["VXM", "-f", temp_working.name, "-sq", "-q"]
    monkeypatch.setattr(sys, "argv", test_args)
    parser = vxm.make_parser()
    args = parser.parse_args()
    assert type(parser) == type(argparse.ArgumentParser())
    assert args.filenameOrProject == [temp_working.name]
    assert args.fileMode == True
    assert args.smartQuotes == True
    assert args.quiet == True

    test_args = ["VXM", "cps"]
    monkeypatch.setattr(sys, "argv", test_args)
    parser = vxm.make_parser()
    args = parser.parse_args()
    assert type(parser) == type(argparse.ArgumentParser())
    assert args.filenameOrProject == ["cps"]
    assert args.fileMode == False
    assert args.smartQuotes == False
    assert args.quiet == False


def test_try_to_parse_files():
    test_arguments_one = test_arg(True, False, False, [temp_working.name])
    assert vxm.try_to_parse_files(
        "from_command_line", test_arguments_one.filenameOrProject, test_arguments_one
    ) == [temp_working.name]
    test_arguments_two = test_arg(True, False, False, [temp_broken.name])
    assert (
        vxm.try_to_parse_files(
            "from_command_line",
            test_arguments_two.filenameOrProject,
            test_arguments_two,
        )
        == []
    )
    test_arguments_three = test_arg(
        True, False, False, [temp_working.name, temp_broken.name]
    )
    assert vxm.try_to_parse_files(
        "from_command_line",
        test_arguments_three.filenameOrProject,
        test_arguments_three,
    ) == [temp_working.name]


def test_check_for_smart_quotes():
    assert vxm.check_for_smart_quotes(temp_working.name) == False
    assert vxm.check_for_smart_quotes(temp_broken.name) == False


def test_args_parser():
    parser = vxm.make_parser()
    test_args1 = test_arg(True, True, True, [temp_working.name])
    assert vxm.args_parser([], parser, test_args1) == [[temp_working.name]]
    test_args2 = test_arg(True, False, False, [temp_broken.name])
    assert vxm.args_parser([], parser, test_args2) == [[]]
