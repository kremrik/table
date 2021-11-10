from utils.cli import Arg, cli

from sys import argv


opts = [
    Arg("setup", lambda: print("SETUP"), "Set up DB"),
    Arg("teardown", lambda: print("TEARDOWN"), "Tear down db")
]


if __name__ == "__main__":
    cli(argv[1:], opts)
