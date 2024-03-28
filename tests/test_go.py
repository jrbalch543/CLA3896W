import pytest
import argparse
import os
import re
import getpass
from collections import OrderedDict
import ipums.tools.go as go  ## Fix this
import tempfile as tf


class ExampleClass:
    def __init__(self, KEY, PATH, OLDKEY, NEWKEY, WORKSPACE, parser):
        self.KEY = KEY
        self.PATH = PATH
        self.OLDKEY = OLDKEY
        self.NEWKEY = NEWKEY
        self.WORKSPACE = WORKSPACE
        self.parser = parser


temp_godata = tf.NamedTemporaryFile(suffix=".txt")
temp_godata.write(
    b"""	IPUMS	/pkg/ipums     
TestKeyForPKG	/pkg  
cps	/pkg/ipums/cps    
cps/rr-test	/mnt/rds/ipumsi-cps/workspaces/rr-test   
"""
)
temp_godata.read()

fake_go_data = tf.NamedTemporaryFile()
fake_go_data.write(
    b"""packange --> cbkdb
testKey != jbdjbdc
435\tjcjbc"""
)
fake_go_data.read()

test_go_data = tf.NamedTemporaryFile(suffix=".txt")
test_go_data.write(
    b"""testKey testPath
testKey2 testPath2"""
)
test_go_data.read()

test_go_data2 = tf.NamedTemporaryFile()
test_go_data2.write(
    b"""	IPUMS	/pkg/ipums    
cps/nn_test	/pkg/ipums/cps/workspaces/nn_test  
testKey	../go_balch027.dat 
"""
)
test_go_data2.read()

test = tf.NamedTemporaryFile(suffix=".txt")

fake_go_data2 = tf.NamedTemporaryFile(suffix=".txt")


def test_make_and_check_go_data_works():
    match_str = "^\s*(.+)\t(.+)\s+$"  ## RegEx pattern for how Go file should exist
    assert (
        go.make_and_check_go_data(temp_godata.name, match_str) != {}
    )  ## Works as normal
    assert (
        go.make_and_check_go_data(fake_go_data.name, match_str) == {}
    )  ## Bad go file, good matchStr
    assert (
        go.make_and_check_go_data(fake_go_data2.name, match_str) == {}
    )  ## No go file, good matchStr
    assert (
        go.make_and_check_go_data(test_go_data.name, "^s*(.+)\s(.+)\s+$") != {}
    )  ## Good go file, different matchStr, should work

    assert (
        go.make_and_check_go_data(fake_go_data.name, "^s*(.+) --> (.+)\s+$") != {}
    )  ## Bad go file, but match_str should match
    assert (
        go.make_and_check_go_data(temp_godata.name, "^s*(.+) --> (.+)\s+$") == {}
    )  ## Good go, mismatch matchStr

    assert (
        go.make_and_check_go_data(test_go_data2.name, "hcbcjkb") == {}
    )  ## Good go data, bad matchStr


def test_show_go_paths_works():
    assert go.show_go_paths(temp_godata.name) != []  ## Works as normal
    assert go.show_go_paths(fake_go_data.name) != []  ## Bad go file, good matchStr
    assert go.show_go_paths(fake_go_data2.name) == []  ## No go file, good matchStr
    assert go.show_go_paths(test_go_data.name) != []  ## Good go file, should work


def test_write_go_paths():
    assert (
        go.write_go_paths(test.name, {"testKey": "testPath", "testKey2": "testPath2"})
        == True
    )
    assert go.show_go_paths(test.name) == [
        "\ttestKey\ttestPath \n",
        "\ttestKey2\ttestPath2 \n",
    ]
    assert go.write_go_paths(test.name, ["testKey"]) == False


def test_run_clear_go():
    go.write_go_paths(test.name, {"testKey": "testPath", "testKey2": "testPath2"})
    assert go.run_clear_go(test.name, "yes") == True
    assert go.show_go_paths(test.name) == []
    go.write_go_paths(test.name, {"testKey": "testPath", "testKey2": "testPath2"})
    assert go.run_clear_go(test.name, "Yes") == True
    assert go.show_go_paths(test.name) == []
    go.write_go_paths(test.name, {"testKey": "testPath", "testKey2": "testPath2"})
    assert go.run_clear_go(test.name, "y") == True
    assert go.show_go_paths(test.name) == []
    go.write_go_paths(test.name, {"testKey": "testPath", "testKey2": "testPath2"})
    assert go.run_clear_go(test.name, "Y") == True
    assert go.show_go_paths(test.name) == []
    go.write_go_paths(test.name, {"testKey": "testPath", "testKey2": "testPath2"})
    assert go.run_clear_go(test.name, "clear") == True
    assert go.show_go_paths(test.name) == []
    go.write_go_paths(test.name, {"testKey": "testPath", "testKey2": "testPath2"})
    assert go.run_clear_go(test.name, "no") == False
    assert go.show_go_paths(test.name) != []
    go.write_go_paths(test.name, {"testKey": "testPath", "testKey2": "testPath2"})
    assert go.run_clear_go(test.name, "hkdvbjhdvh") == False
    assert go.show_go_paths(test.name) != []


def test_add_go():
    parser = go.run_parser(test.name)
    a = ExampleClass("testKey3", "testPath3", "a", "a", "a", "a")
    assert go.add_go(test.name, {}, parser, a) == None
    b = ExampleClass("pkg", "/pkg", "a", "a", "a", "a")
    assert go.add_go(test.name, {}, parser, b) == {"pkg": "/pkg"}


def test_ren_go():
    parser = go.run_parser(test.name)
    a = ExampleClass("a", "a", "cps", "cpsPkg", "a", "a")
    assert go.ren_go(test.name, {"cps": "/pkg/ipums/cps"}, parser, a) == {
        "cpsPkg": "/pkg/ipums/cps"
    }
    b = ExampleClass("a", "a", "prog", "myTestScripts", "a", "a")
    assert (
        go.ren_go(
            test.name,
            {"programming": "/pkg/ipums/programming/script-revamp"},
            parser,
            b,
        )
        == None
    )


def test_del_go():
    parser = go.run_parser(test.name)
    a = ExampleClass("testKey2", "a", "pkg", "package", "a", "a")
    assert go.del_go(
        test.name, {"testKey": "testPath", "testKey2": "testPath2"}, parser, a
    ) == {"testKey": "testPath"}
    b = ExampleClass("testKey3", "a", "pkg", "package", "a", "a")
    assert (
        go.del_go(
            test.name, {"testKey": "testPath", "testKey2": "testPath2"}, parser, b
        )
        == None
    )


def test_ws_go():
    parser = go.run_parser(test.name)
    a = ExampleClass("testKey2", "a", "pkg", "package", "cps/master", "a")
    assert go.ws_go(test.name, {}, parser, a) == {
        "cps/master": "/pkg/ipums/cps/workspaces/master"
    }
    b = ExampleClass("testKey2", "a", "pkg", "package", "cps/rr-failTest", "a")
    assert go.ws_go(test.name, {}, parser, b) == None
    c = ExampleClass("testKey2", "a", "pkg", "package", "testKey", "a")
    assert go.ws_go(test.name, {}, parser, c) == None


def test_go_go():
    parser = go.run_parser(test.name)
    a = ExampleClass(
        [["testKey", "testKey2"]], "a", "pkg", "package", "cps/rr-test", "a"
    )
    assert go.go_go(
        test.name, {"testKey": "testPath", "testKey2": "testPath2"}, parser, a
    ) == ["testPath", "testPath2"]
    b = ExampleClass([["testKey3"]], "a", "pkg", "package", "cps/rr-test", "a")
    assert (
        go.go_go(test.name, {"testKey": "testPath", "testKey2": "testPath2"}, parser, b)
        == []
    )
    c = ExampleClass([[]], "a", "pkg", "package", "cps/rr-test", "a")
    assert go.go_go(
        test.name, {"testKey": "testPath", "testKey2": "testPath2"}, parser, c
    ) == go.show_go_paths(test.name)
