import os
import sys
import pandas as pd
from dotenv import load_dotenv
from userHandler import userUpdateHandler

load_dotenv()
required_env_vars = [
    "DB_NAME",
    "DB_USER",
    "DB_PASSWORD",
    "DB_HOST",
    "DB_PORT",
]

if not all(os.getenv(var) for var in required_env_vars):
    print("Required environment variables not found.")
    sys.exit(1)

CSV_FILE = "AdiraCheckListTrainingP2Telesales_1.csv"
IS_GENESYS = False
USER_TYPE = "Branch"
FORCE_UPDATE = {"users": False, "master_branch": False, "groups": False, "roles": False}
DIVISION_ID = "12cc5dae-d360-4e4a-a690-d41e0d2bdc98"
DEFAULT_PASSWORD = "Adira123@"


def insert_csv_to_postgres(csv_file):
    duar = userUpdateHandler(
        os.getenv("DB_NAME"),
        os.getenv("DB_USER"),
        os.getenv("DB_PASSWORD"),
        os.getenv("DB_HOST"),
        os.getenv("DB_PORT"),
        FORCE_UPDATE,
        USER_TYPE,
        IS_GENESYS,
        DEFAULT_PASSWORD,
        DIVISION_ID,
    )
    df = pd.read_csv(csv_file)
    userUpdate = duar.update_all(df)
    print(userUpdate)


if __name__ == "__main__":
    insert_csv_to_postgres(CSV_FILE)
