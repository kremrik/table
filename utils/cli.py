from dataclasses import dataclass
from textwrap import dedent
from typing import Callable, List, Optional


BOLD = "\033[1m"
RED = "\033[31m"
NORM = "\033[0m"


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
        msg = format_error(
            f"Incorrect arguments: {bad_args}"
        )
        print(msg)
        print(hlp)
        exit(1)

    arg = cli_args[0]
    callback = [
        o
        for o in opts
        if o.arg == arg
    ][0].callback

    callback()


def generate_help(args: List[Arg]) -> str:
    hlp = "\n".join(map(_format_argument, args))
    return hlp


def _format_argument(arg: Arg) -> str:
    opt = format_option(arg.arg)
    hlp = arg.help
    output = f"{opt}\n    {hlp}"
    return output


def format_option(opt: str) -> str:
    return f"{BOLD}{opt}{NORM}"


def format_error(msg: str) -> str:
    return f"{RED}{msg}{NORM}"
