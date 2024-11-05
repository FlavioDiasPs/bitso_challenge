import pandas as pd
import datetime
from sqlalchemy import create_engine
import deltalake


# Create the dim_date table
creation_query = """
        CREATE TABLE dim_date (
            full_date DATE PRIMARY KEY,
            day_of_week STRING,
            month INTEGER,
            quarter INTEGER,
            year INTEGER
        );
"""

# Generate date range using pandas
start_date = datetime.date(2000, 1, 1)
end_date = datetime.date(2090, 12, 31)
date_range = pd.date_range(start_date, end_date)

# Create a DataFrame with the date attributes
date_data = {
    "full_date": date_range,
    "day_of_week": date_range.strftime("%A"),
    "month": date_range.month,
    "quarter": date_range.quarter,
    "year": date_range.year,
}
df = pd.DataFrame(date_data)


# Write to Delta Lake
deltalake.write_deltalake("data/delta/gold/dim_date", df, mode="overwrite")


engine = create_engine(f"sqlite:///data/bitso.db")
with engine.begin() as connection:
    df.to_sql("dim_date", con=connection, if_exists="replace", index=False)
