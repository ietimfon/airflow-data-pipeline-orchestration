
import pandas as pd
from datetime import date
from pathlib import Path

from airflow.providers.postgres.hooks.postgres import PostgresHook
from include import config


CONN_ID = "orders_local_postgres"
DATA_PATH = Path(config.DATA_PATH)


def get_engine():
    return PostgresHook(
        postgres_conn_id=CONN_ID
    ).get_sqlalchemy_engine()


def extract():
    print("Extracting data from source")

    DATA_PATH.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(config.DATA_SOURCE)
    df.to_csv(DATA_PATH / "raw_sales.csv", index=False)

    print(f"Source data extracted successfully: {len(df)} rows")


def load():
    df = pd.read_csv(DATA_PATH / "raw_sales.csv")
    engine = get_engine()

    with engine.begin() as connection:
        record_no = df.to_sql(
            "raw_sales",
            connection,
            if_exists="replace",
            index=False
        )

    print(f"{record_no} rows loaded to raw_sales")


def transform():
    engine = get_engine()

    with engine.connect() as connection:
        df = pd.read_sql("SELECT * FROM raw_sales", connection)

    print("Starting transformation")

    df = df.drop_duplicates().copy()

    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["order_date"] = pd.to_datetime(
        df["order_date"],
        errors="coerce"
    )

    df = df.dropna(subset=["quantity", "price"])
    df = df.fillna({
        "customer_name": "Unknown",
        "product": "Unknown",
        "region": "Unknown"
    })

    df["total_sale"] = df["quantity"] * df["price"]

    with engine.begin() as connection:
        record_no = df.to_sql(
            "sales",
            connection,
            if_exists="replace",
            index=False
        )

    print(f"{record_no} rows loaded to sales")


def notify():
    engine = get_engine()

    with engine.connect() as connection:
        df = pd.read_sql("SELECT * FROM sales", connection)

    if df.empty:
        raise ValueError("The sales table is empty")

    total_revenue = df["total_sale"].sum()
    order_count = len(df)
    total_quantity = df["quantity"].sum()

    top_customer = (
        df.groupby("customer_name")["total_sale"]
        .sum()
        .idxmax()
    )

    top_product = (
        df.groupby("product")["total_sale"]
        .sum()
        .idxmax()
    )

    message = (
        f"Orders report for {date.today()}\n"
        f"Order Count: {order_count}\n"
        f"Total Quantity: {total_quantity}\n"
        f"Top Customer: {top_customer}\n"
        f"Top Product: {top_product}\n"
        f"Total Revenue: ${total_revenue:,.2f}\n"
    )

    report_path = DATA_PATH / "report.txt"
    report_path.write_text(message, encoding="utf-8")

    print(message)
    print(f"Report saved to {report_path}")
