import sys
from pathlib import Path
import pytest


@pytest.fixture
def check_help_msg(monkeypatch, capsys):
    """Returns a function that can be used to check the help message of a given script.

    The returned function takes three arguments:
        script_name: str - the name of the script being tested
        main_func: Callable[[], Any] - the main function to call (without arguments)
        expected_in_output: list[str] - a list of strings to check for in the help message output

    When called, the returned function runs the given script with --help as its argument, checks
    that it raises a SystemExit with return code 0, and then checks that each phrase in
    expected_in_output appears in the help message output. It indicates failures with
    AssertionErrors.
    """

    def inner_checker(script_name, main_func, expected_in_output):
        new_args = [script_name, "--help"]
        monkeypatch.setattr(sys, "argv", new_args)

        with pytest.raises(SystemExit) as excinfo:
            main_func()

        assert excinfo.value.code == 0

        output = capsys.readouterr().out
        for phrase in expected_in_output:
            assert phrase in output

    return inner_checker


@pytest.fixture
def test_data_dir():
    return Path(__file__).parent / "test_data"


@pytest.fixture
def ref_data_dir():
    return Path(__file__).parent / "reference_data"


@pytest.fixture
def out_data_dir():
    return Path(__file__).parent / "out_data"


@pytest.fixture
def audit_datalist():
    def audit(dl, rectype, zipped=False):
        assert all([d.rectype == rectype for d in dl])
        assert not all([d.name.startswith("F") for d in dl])
        assert dl[0].fmt == dl[2].fmt == "a"
        assert dl[1].fmt == "2"
        assert dl[3].fmt is None
        if zipped:
            if rectype != "C":
                assert all([len(d.valuelabels) == 2 for d in dl])
            else:
                assert all([(d.valuelabels == None or d.valuelabels == []) for d in dl])

    return audit


@pytest.fixture
def audit_varlabels():
    def audit(vl):
        assert all([v.label.startswith("Match case ") for v in vl])
        assert not any([v.name.startswith("F") for v in vl])

    return audit


@pytest.fixture
def audit_do_varlabels():
    def audit(vl):
        assert all(v.startswith("Match case ") for v in vl.values())
        assert not any([k.startswith("F") for k in vl.keys()])

    return audit


@pytest.fixture
def audit_vallabels():
    def audit(vl):
        assert not any([v.parent.startswith("F") for v in vl])
        assert len([v for v in vl if v.parent.startswith("A")]) == 8
        assert len([v for v in vl if v.parent.startswith("B")]) == 8
        assert len([v for v in vl if v.parent.startswith("C")]) == 0
        assert len([v for v in vl if v.parent.startswith("F")]) == 0

    return audit


@pytest.fixture
def audit_do_vallabels():
    def audit(vl):
        assert not any([k.startswith("F") for k in vl.keys()])
        assert len([k for k in vl.keys() if k.startswith("A")]) == 4
        assert len([k for k in vl.keys() if k.startswith("B")]) == 4
        assert len([k for k in vl.keys() if k.startswith("C")]) == 0
        assert len([k for k in vl.keys() if k.startswith("F")]) == 0
        assert all([len(v) == 2 for v in vl.values()])

    return audit


from ipums.metadata import db
from sqlalchemy import MetaData, Table, String, Column, DateTime, Integer
from datetime import datetime

DB_FILE = "test.db"
PROJECTS_CONFIG = """
{
    "test": {
        "path": "./"
    }
}
"""
PROJECT_NAME = "test"


@pytest.fixture
def tmp_db(tmp_path):
    return tmp_path / DB_FILE


@pytest.fixture
def test_engine(tmp_db):
    engine = db.engine.get_engine(tmp_db, automap=False)
    return engine


@pytest.fixture(autouse=True)
def clean_out_projects():
    # This fixture is for resetting the Project class to have
    # no instances at the end of any test that uses this fixture.
    yield
    db.Project.drop_all()


@pytest.fixture
def mock_files_project(monkeypatch):
    monkeypatch.setattr("ipums.metadata.db.project.FilesProject", MockFilesProject)


@pytest.fixture
def mock_project(mock_files_project, test_db, tmp_db):
    use_this_engine = db.engine.get_engine(tmp_db)
    db.models.Base.metadata.create_all(use_this_engine)
    mock_project = db.Project("test", engine=use_this_engine)
    return mock_project


@pytest.fixture
def mock_session_project(mock_files_project, test_db, tmp_db):
    with db.engine.session(tmp_db) as session:
        yield db.Project("test", session=session)


@pytest.fixture
def test_db(test_engine):
    db_metadata_obj = MetaData()
    Table(
        "samples",
        db_metadata_obj,
        Column("sample", String(255), primary_key=True),
        Column("country", String(16)),
        Column("svarstem", String(60)),
        Column("hide", String(1), nullable=True),
        Column("long_name", String(120)),
        extend_existing=True,
    )
    Table(
        "variables",
        db_metadata_obj,
        Column("variable", String(255), primary_key=True),
        Column("sample", String(255), nullable=True),
        Column("display_variable", String(255), nullable=True),
        Column("label", String(60)),
        Column("hide", String(1), nullable=True),
        Column("group", String(120)),
        Column("svar", Integer, nullable=True),
        Column("rec", String(1), nullable=True),
        Column("varlong", String(125), nullable=True),
        Column("incol", Integer, nullable=True),
        Column("inwid", Integer, nullable=True),
        Column("nontab", Integer, nullable=True),
        Column("decim", Integer, nullable=True),
        Column("string", Integer, nullable=True),
        Column("ddoc1", Integer, nullable=True),
        Column("dtag1", Integer, nullable=True),
        Column("jdoc1", Integer, nullable=True),
        Column("jtag1", Integer, nullable=True),
        Column("ddoc2", Integer, nullable=True),
        Column("dtag2", Integer, nullable=True),
        Column("jdoc2", Integer, nullable=True),
        Column("jtag2", Integer, nullable=True),
        Column("data_type", Integer, nullable=True),
        Column("ddorder", Integer, nullable=True),
        extend_existing=True,
    )
    db_metadata_obj.create_all(test_engine)


class MockFilesProject:
    def __init__(
        self, name=PROJECT_NAME, projects_config=PROJECTS_CONFIG, db_path=DB_FILE
    ):
        self.db_path = db_path
        self.projects_config = projects_config
        self.name = name


@pytest.fixture
def db_test2023a(test_db, mock_project):
    test_sample = db.models.Samples(
        sample="test2023a",
        country="test",
        svarstem="TEST2023A_",
        hide=None,
        long_name="This is only a test",
    )
    with mock_project.session as session:
        session.add(test_sample)
        session.commit()


@pytest.fixture
def db_test2023b(test_db, mock_project):
    test_sample2 = db.models.Samples(
        sample="test2023b",
        country="test",
        svarstem="TEST2023B_",
        hide=None,
        long_name="This is only b test",
    )
    with mock_project.session as session:
        session.add(test_sample2)
        session.commit()


@pytest.fixture
def db_test2023c(test_db, mock_project):
    test_sample2 = db.models.Samples(
        sample="test2023c",
        country="test",
        svarstem="TEST2023C_",
        hide=None,
        long_name="This is only c test",
    )
    with mock_project.session as session:
        session.add(test_sample2)
        session.commit()


@pytest.fixture
def test_var(test_db, mock_project):
    test_var = db.models.Variables(
        variable="TESTVAR",
        sample="MOCK_PROJ",
        display_variable=None,
        svar=0,
        label="Testing testing",
        group="p_test",
    )
    test_var2 = db.models.Variables(
        variable="TESTVAR2",
        display_variable=None,
        svar=0,
        label="Testing testing 2",
        group="p_test",
    )
    with mock_project.session as session:
        session.add_all([test_var, test_var2])
        session.commit()


@pytest.fixture
def testvar_universe(test_var, db_test2023a, db_test2023b, mock_project):
    universe_statement_1 = db.models.TtVariableUniversedisplayids(
        universedisplayid=1,
        variable="TESTVAR",
        nosampstatement=0,
        makesampstatement=0,
        sampstatement="TEST 2023",
        univstatement="1 2 3",
    )

    universe_statement_2 = db.models.TtVariableUniversedisplayids(
        universedisplayid=2,
        variable="TESTVAR",
        nosampstatement=0,
        makesampstatement=0,
        sampstatement="HAS NO SAMPLES",
        univstatement="4 5 6",
    )

    universe_sample_1 = db.models.TtVariableUniversedisplayidSamples(
        sample="test2023a",
        variable="TESTVAR",
        universedisplayid=1,
    )

    with mock_project.session as session:
        session.add_all(
            [
                universe_statement_1,
                universe_statement_2,
                universe_sample_1,
            ]
        )
        session.commit()


@pytest.fixture
def test_int_populate(
    test_db, test_var, test_svar, db_test2023a, db_test2023b, mock_project
):
    integration_1 = db.models.TtSamplevariables(
        sample="test2023a",
        variable="TESTVAR",
    )

    integration_2 = db.models.TtSamplevariables(
        sample="test2023b",
        variable="TESTVAR",
    )

    integration_3 = db.models.TtSamplevariables(
        sample="test2023a",
        variable="TESTSVAR",
        is_svar="1",
        univ="Test universe",
    )

    source_1 = db.models.TtSamplevariablesSources(
        sample="test2023a",
        variable="TESTVAR",
        source="TESTSVAR",
        is_svar=1,
        source_order=1,
    )

    recoding_1 = db.models.TtSamplevariablesRecodings(
        sample="test2023a",
        variable="TESTVAR",
        output_code="0",
        input_code="0",
        date_created=datetime.now(),
    )

    recoding_2 = db.models.TtSamplevariablesRecodings(
        sample="test2023a",
        variable="TESTVAR",
        output_code="1",
        input_code="1",
        date_created=datetime.now(),
    )

    recoding_3 = db.models.TtSamplevariablesRecodings(
        sample="test2023b",
        variable="TESTVAR",
        output_code="0",
        input_code="1",
        date_created=datetime.now(),
    )

    with mock_project.session as session:
        session.add_all(
            [
                integration_1,
                integration_2,
                integration_3,
                source_1,
                recoding_1,
                recoding_2,
                recoding_3,
            ]
        )
        session.commit()


@pytest.fixture
def testvar_recodings(test_db, test_int_populate, mock_project):
    variable_label_0 = db.models.TtVariableLabels(
        label_id=111,
        variable="TESTVAR",
        labelonly=1,
        label="test_label",
        indent=0,
        genlab="test",
        indentgen=0,
        syntax="test",
        codetype="test",
        output_code="0",
        missing="False",
        date_created=datetime.now(),
    )
    variable_label_00 = db.models.TtVariableLabels(
        label_id=1100,
        variable="TESTVAR",
        labelonly=0,
        label="test_label_1",
        indent=0,
        genlab="test",
        indentgen=0,
        syntax="test",
        codetype="test",
        output_code="1",
        missing="False",
        date_created=datetime.now(),
    )

    variable_label_000 = db.models.TtVariableLabels(
        label_id=110,
        variable="TESTVAR",
        labelonly=0,
        label="test_label_0",
        indent=0,
        genlab="test",
        indentgen=0,
        syntax="test",
        codetype="test",
        output_code="0",
        missing="False",
        date_created=datetime.now(),
    )

    variable_label_1 = db.models.TtVariableLabels(
        label_id=111,
        variable="TESTSVAR",
        labelonly=1,
        label="test_label",
        indent=0,
        genlab="test",
        indentgen=0,
        syntax="test",
        codetype="test",
        output_code="0",
        missing="False",
        date_created=datetime.now(),
    )
    variable_label_2 = db.models.TtVariableLabels(
        label_id=112,
        variable="TESTSVAR",
        labelonly=0,
        label="test_label",
        indent=0,
        genlab="test",
        indentgen=0,
        syntax="test",
        codetype="test",
        output_code="1",
        missing="False",
        date_created=datetime.now(),
    )
    with mock_project.session as session:
        session.add_all(
            [
                variable_label_0,
                variable_label_00,
                variable_label_000,
                variable_label_1,
                variable_label_2,
            ]
        )
        session.commit()


@pytest.fixture
def test_sample_vars(test_db, test_var, db_test2023a, db_test2023b, mock_project):
    test_sample_var1 = db.models.TtSamplevariables(
        variable="TESTVAR",
        sample="test2023a",
    )
    test_sample_var2 = db.models.TtSamplevariables(
        variable="TESTVAR",
        sample="test2023b",
    )
    with mock_project.session as session:
        session.add_all([test_sample_var1, test_sample_var2])
        session.commit()


@pytest.fixture
def test_svar(test_db, mock_project):
    test_svar = db.models.Variables(
        variable="TESTSVAR",
        sample="TEST2023A",
        display_variable=None,
        label="Test source variable",
        group="p_test",
        svar=1,
        varlong="test2023a_testsvar",
        rec="P",
    )
    test_svar2 = db.models.Variables(
        variable="TESTSOURCEVAR",
        sample="TEST2023A",
        display_variable=None,
        label="Test source variable2",
        group="p_test",
        svar=1,
    )
    test_svar3 = db.models.Variables(
        variable="TEST2023A_SVAR",
        sample="TEST2023A",
        display_variable=None,
        label="Test source variable3",
        group="p_test",
        svar=1,
    )
    with mock_project.session as session:
        session.add(test_svar)
        session.add(test_svar2)
        session.add(test_svar3)
        session.commit()


@pytest.fixture
def testvar_recodings_w_no_samps(test_db, mock_project):
    variable_label_0 = db.models.TtVariableLabels(
        label_id=110,
        variable="TESTVAR",
        labelonly=0,
        label="test_label",
        indent=0,
        genlab="test",
        indentgen=0,
        syntax="test",
        codetype="test",
        output_code="0",
        missing="False",
        date_created=datetime.now(),
    )
    variable_label_00 = db.models.TtVariableLabels(
        label_id=1100,
        variable="TESTVAR",
        labelonly=0,
        label="test_label",
        indent=0,
        genlab="test",
        indentgen=0,
        syntax="test",
        codetype="test",
        output_code="0",
        missing="False",
        date_created=datetime.now(),
    )
    variable_label_1 = db.models.TtVariableLabels(
        label_id=111,
        variable="TESTVAR",
        labelonly=1,
        label="test_label",
        indent=0,
        genlab="test",
        indentgen=0,
        syntax="test",
        codetype="test",
        output_code="1",
        missing="False",
        date_created=datetime.now(),
    )
    variable_label_2 = db.models.TtVariableLabels(
        label_id=112,
        variable="TESTSVAR",
        labelonly=0,
        label="test_label",
        indent=0,
        genlab="test",
        indentgen=0,
        syntax="test",
        codetype="test",
        output_code="1",
        missing="False",
        date_created=datetime.now(),
    )
    with mock_project.session as session:
        session.add_all(
            [variable_label_0, variable_label_00, variable_label_1, variable_label_2]
        )
        session.commit()


@pytest.fixture
def var_object(test_var, mock_project):
    return db.Variable("TESTVAR", mock_project)


@pytest.fixture
def svar_object(
    test_svar, db_test2023a, test_int_populate, testvar_recodings, mock_project
):
    return db.Svar("TESTSVAR", mock_project)


@pytest.fixture
def val_label_object(test_var, testvar_recodings, mock_project):
    return db.ValueLabel(111, "TESTVAR", mock_project)


@pytest.fixture
def val_label0_object(test_var, testvar_recodings, mock_project):
    return db.ValueLabel(110, "TESTVAR", mock_project)


@pytest.fixture
def mock_versioned_db(monkeypatch):
    class MockVersioning:
        def __init__(self, path, use_scratch=True) -> None:
            if path == "no_versioning":
                raise FileNotFoundError("nope")

            self.db_file = path
            self.use_scratch = use_scratch
            self.current_version = "v0.0.1"

        def get_current_version(self):
            return self.current_version

        def dvc_file_is_dirty(self):
            return False

        def reset_to_current_version(self):
            return self.current_version

        def reset_to_version(self, version, force=False):
            print(f"Resetting to version {version}")
            return version

    monkeypatch.setattr(
        "ipums.metadata.db.engine.VersioningMetadataDatabase", MockVersioning
    )
