import os
import sys
import pandas as pd
from dotenv import load_dotenv
from userHandler import userUpdateHandler

# Get the path to the executable (if frozen) or the current script (if not frozen)
if getattr(sys, "frozen", False):
    # If frozen, get the path to the executable
    app_path = os.path.dirname(sys.executable)
else:
    # If not frozen, get the current script's directory
    app_path = os.path.dirname(__file__)

# Path to the .env file
dotenv_path = os.path.join(app_path, ".env")

# Function to reload environment variables
def reload_env():
    """Force reload of environment variables from .env file."""
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path, override=True)  # Override existing environment variables
    else:
        print(f".env file not found at {dotenv_path}")
        sys.exit(1)

# Reload the environment variables at the beginning of execution
reload_env()

# Validate that required environment variables are set
required_env_vars = [
    "DB_NAME",
    "DB_USER",
    "DB_PASSWORD",
    "DB_HOST",
    "DB_PORT",
    "FORCE_UPDATE_USER",
    "FORCE_UPDATE_BRANCH",
    "FORCE_UPDATE_GROUPS",
    "FORCE_UPDATE_ROLES",
]

if not all(os.getenv(var) for var in required_env_vars):
    print("Required environment variables not found.")
    sys.exit(1)

CSV_FILE = os.path.join(app_path, "AdiraCheckListTrainingP2Telesales_1.csv")
IS_GENESYS = False
USER_TYPE = "Branch"
FORCE_UPDATE = {
    "users": bool(os.getenv("FORCE_UPDATE_USER")),
    "master_branch": bool(os.getenv("FORCE_UPDATE_BRANCH")),
    "groups": bool(os.getenv("FORCE_UPDATE_GROUPS")),
    "roles": bool(os.getenv("FORCE_UPDATE_ROLES")),
}
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
