from enum import Enum


__all__ = ["Colors", "Styles"]


BOLD = "\033[1m"

GREEN = "\033[32m"
RED = "\033[31m"

CLEAR = "\033[0m"


class Colors(Enum):
    green = lambda txt: f"{GREEN}{txt}{CLEAR}"
    red = lambda txt: f"{RED}{txt}{CLEAR}"


class Styles(Enum):
    bold = lambda txt: f"{BOLD}{txt}{CLEAR}"
