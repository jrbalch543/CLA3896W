from ipums.tools.freq_quick import main


def test_help_smoke(check_help_msg):
    check_help_msg("freq_quick", main, ["--help", "freq_quick_rust.rs", "frequencies"])
