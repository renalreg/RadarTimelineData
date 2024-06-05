import polars as pl


def max_with_nulls(column: pl.Expr) -> pl.Expr:
    return column.sort(descending=True, nulls_last=False).first()
