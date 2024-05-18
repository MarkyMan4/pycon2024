import polars as pl
import polars.selectors as cs
from great_tables import GT, loc, style


def load_data() -> pl.DataFrame:
    # load coffee dataset and summarize it by getting the 
    # total salesand profit by product
    df = pl.read_csv("data/a3-CoffeeData.csv")
    df = (
        df.with_columns(
            pl.col("date").str.to_date("%d-%b-%y").alias("purchase_date")
        )
        .drop("date")
        .group_by("type")
        .agg(
            pl.col("sales").sum(),
            pl.col("profit").sum(),
        )
    )

    return df

def create_table(df: pl.DataFrame):
    sel_profit = cs.starts_with("profit")

    gt_tbl = GT(df)
    gt_tbl = (
        # structure
        gt_tbl
        .tab_header("Sales and profit by product")
        .tab_spanner(label="metric", columns=["sales", "profit"])

        # formatting
        .fmt_currency(columns=["sales", "profit"])

        # style
        .tab_style(
            style=style.fill(color="papayawhip"),
            locations=loc.body(columns=sel_profit)
        )
    )

    with open("table.html", "w") as f:
        f.write(gt_tbl.as_raw_html())

def main():
    df = load_data()
    create_table(df)
    print(df)


if __name__ == "__main__":
    main()
