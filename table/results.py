from enum import Enum
from typing import List, Tuple


__all__ = ["Results"]


class Results:
    def __init__(self, rows: List[tuple]) -> None:
        self.rows = rows

    def __repr__(self) -> str:
        return pretty_results(self.rows)


class Justify(Enum):
    left = ">"
    right = "<"
    center = "^"


def pretty_results(rows: List[tuple]) -> str:
    sizes = max_column_sizes(rows)
    row_templ = row_template(sizes, Justify.left)
    header_templ = row_template(sizes, Justify.center)

    border = horiz_border(sizes)
    fields = rows[0]._fields
    header = header_templ.format(*fields)

    # TODO: map(str, _) needs to be a fnc that can do stuff
    #  like transform None => NULL
    fmtd_rows = "\n".join(
        [row_templ.format(*map(str, row)) for row in rows]
    )

    output = (
        border
        + "\n"
        + header
        + "\n"
        + border
        + "\n"
        + fmtd_rows
        + "\n"
        + border
    )

    return output


def horiz_border(sizes: Tuple[int]) -> str:
    b = ["-" * (size + 2) for size in sizes]
    b = "+" + "+".join(b) + "+"
    return b


def row_template(
    sizes: Tuple[int], justify: Justify
) -> str:
    just = justify.value
    fmts = [f"{{:{just}{size}}}" for size in sizes]
    fmts = "| " + " | ".join(fmts) + " |"
    return fmts


def max_column_sizes(rows: List[tuple]) -> Tuple[int]:
    header = rows[0]._fields
    max_col_sizes = list(column_sizes(header))

    for row in rows:
        col_sizes = column_sizes(row)
        for idx, size in enumerate(col_sizes):
            if size > max_col_sizes[idx]:
                max_col_sizes[idx] = size

    return tuple(max_col_sizes)


def column_sizes(row: tuple) -> Tuple[int]:
    return tuple(map(len, map(str, row)))
