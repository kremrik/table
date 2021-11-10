from utils.formats import Colors, Styles

from dataclasses import dataclass
from typing import Callable, List


HELP_ARGS = ["-h", "--help", "help"]


@dataclass
class Arg:
    arg: str
    callback: Callable[[], None]
    help: str


def cli(cli_args: List[str], opts: List[Arg]) -> str:
    hlp = generate_help(opts)

    if not cli_args:
        print(hlp)
        exit(0)

    for d in HELP_ARGS:
        if d in cli_args:
            print(hlp)
            exit(0)

    arg_names = [a.arg for a in opts]
    bad_args = list(set(cli_args) - set(arg_names))
    if bad_args:
        msg = Colors.red(
            f"Incorrect argument(s): {bad_args}"
        )
        print(msg)
        exit(1)

    arg = cli_args[0]
    callback = [
        o
        for o in opts
        if o.arg == arg
    ][0].callback

    callback()


def generate_help(args: List[Arg]) -> str:
    header = Styles.bold("COMMANDS")
    hlp = "\n".join(map(_format_argument, args))
    output = header + "\n" + hlp
    return output


def _format_argument(arg: Arg) -> str:
    opt = Styles.bold(arg.arg)
    hlp = arg.help
    output = f"    {opt}\n        {hlp}"
    return output
