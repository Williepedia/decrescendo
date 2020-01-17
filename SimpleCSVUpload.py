import pandas as pd
import os
from sqlalchemy import create_engine
import configparser

config = configparser.ConfigParser()
config.read('config.ini')

path = r""

df = pd.read_csv(path, encoding="cp")

# df['file'] = path

# cleans column names
df.columns = df.columns = (
    df.columns.str.strip()
    .str.lower()
    .str.replace(" ", "_")
    .str.replace("(", "")
    .str.replace(")", "")
)

df.dropna(how="all", axis="columns")
# uploads to database
engine = create_engine(
    config['server_credentials']['url'],
    encoding="utf-8",
    )
db = engine.connect()
table = ""
df.to_sql(table, db, if_exists="append", index=False)
db.close()
