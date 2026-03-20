from src.cli import build_parser


def test_run_command_is_processing_only_help_text():
    parser = build_parser()

    run_parser = next(action for action in parser._subparsers._group_actions if action.dest == "command")

    assert "run" in run_parser.choices
    assert "ingest" in run_parser.choices
    assert "process" in run_parser.choices
    assert run_parser.choices["run"].description is None
    assert run_parser.choices["run"].format_usage().startswith("usage: ")