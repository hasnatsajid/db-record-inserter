from fastapi import FastAPI, File, UploadFile, HTTPException
import pandas as pd

# from database import engine, Base
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Date,
    Text,
    DECIMAL,
    ForeignKey,
)
from sqlalchemy.orm import relationship
# from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker,declarative_base


# app = FastAPI(debug=True)
app = FastAPI()

# Database setup
DATABASE_URL = "postgresql://postgres_user:db_password@localhost/inserter"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class Account(Base):
    __tablename__ = "accounts"

    account_id = Column(Integer, primary_key=True)
    account_number = Column(String(100), nullable=False, unique=True)
    account_name = Column(String(200), nullable=False)

    transactions = relationship("Transaction", back_populates="account")


class Department(Base):
    __tablename__ = "departments"

    dept_id = Column(String, primary_key=True)
    dept_name = Column(String(200), unique=True, nullable=False)


class Location(Base):
    __tablename__ = "locations"

    location_id = Column(String, primary_key=True)
    location_name = Column(String(200), nullable=False)


class AccountGroup(Base):
    __tablename__ = "account_groups"

    group_id = Column(Integer, primary_key=True)
    group_members = Column(String(200), nullable=False)
    group_name = Column(String(200), unique=True, nullable=False)


class Transaction(Base):
    __tablename__ = "transactions"

    transaction_id = Column(Integer, primary_key=True)
    posted_date = Column(Date, nullable=False)
    doc_date = Column(Date, nullable=False)
    doc_number = Column(String)
    memo = Column(Text)
    department_name = Column(String)
    location_name = Column(String)
    jnl_code = Column(String)
    # debit = Column(DECIMAL(precision=10, scale=2))
    debit = Column(DECIMAL(precision=10, scale=2), default=0)  # Provide a default value
    credit = Column(DECIMAL(precision=10, scale=2))
    account_number = Column(String(100), ForeignKey("accounts.account_number"))
    balance = Column(DECIMAL(precision=10, scale=2))

    account = relationship("Account", back_populates="transactions")


# Base.metadata.create_all(bind=engine)


table_names = ["account_groups", "accounts", "locations", "departments", "transactions"]


# Function to insert data into a table
def insert_data(session, model, data):
    for record in data:
        # Replace nan with None in the record
        record = {
            key: None if pd.isna(value) else value for key, value in record.items()
        }
        session.add(model(**record))
    session.commit()

    # for record in data:
    #     session.add(model(**record))
    # session.commit()


# Sample data dictionaries obtained from CSV files
# data_accounts = [
#     {"account_number": "786", "account_name": "Savings"},
#     {"account_number": "456", "account_name": "Checking"},
# ]

# data_departments = [{"dept_name": "HR"}, {"dept_name": "IT"}]

# data_locations = [{"location_name": "New York"}, {"location_name": "San Francisco"}]

# data_account_groups = [
#     {"group_members": "Members1", "group_name": "Group1"},
#     {"group_members": "Members2", "group_name": "Group2"},
# ]

# data_transactions = [
#     {
#         "posted_date": "2022-01-01",
#         "doc_date": "2022-01-01",
#         "doc_number": "Doc1",
#         "debit": 100.0,
#         "credit": 0.0,
#         "account_id": "456",
#         "balance": 100.0,
#     },
#     {
#         "posted_date": "2022-01-02",
#         "doc_date": "2022-01-02",
#         "doc_number": "Doc2",
#         "debit": 0.0,
#         "credit": 50.0,
#         "account_id": "786",
#         "balance": 50.0,
#     },
# ]


def transform_location_data(location_data):
    # Mapping of CSV keys to database fields
    mapping = {
        "Location ID": "location_id",
        "Location name": "location_name",
    }

    transformed_data = []

    for location_entry in location_data:
        # Rename keys based on mapping
        renamed_entry = {
            mapping.get(old_key, old_key): value
            for old_key, value in location_entry.items()
        }

        # Filter out keys not present in the database table
        filtered_entry = {
            key: val for key, val in renamed_entry.items() if key in mapping.values()
        }

        transformed_data.append(filtered_entry)

    return transformed_data


def transform_department_data(department_data):
    # Mapping of CSV keys to database fields
    mapping = {
        "Department ID": "dept_id",
        "Department name": "dept_name",
    }

    transformed_data = []

    for department_entry in department_data:
        # Rename keys based on mapping
        renamed_entry = {
            mapping.get(old_key, old_key): value
            for old_key, value in department_entry.items()
        }

        # Filter out keys not present in the database table
        filtered_entry = {
            key: val for key, val in renamed_entry.items() if key in mapping.values()
        }

        transformed_data.append(filtered_entry)

    return transformed_data


def transform_account_groups_data(account_groups_data):
    # Mapping of CSV keys to database fields
    mapping = {
        "GROUP_NAME": "group_name",
        "MEMBERS": "group_members",
    }

    transformed_data = []

    for account_group_entry in account_groups_data:
        # Rename keys based on mapping
        renamed_entry = {
            mapping.get(old_key, old_key): value
            for old_key, value in account_group_entry.items()
        }

        # Filter out keys not present in the database table
        filtered_entry = {
            key: val for key, val in renamed_entry.items() if key in mapping.values()
        }

        transformed_data.append(filtered_entry)

    return transformed_data


def transform_transactions_data(transactions_data):
    # Mapping of CSV keys to database fields
    mapping = {
        "Account": "account_number",
        "Posted dt.": "posted_date",
        "Doc dt.": "doc_date",
        "Doc": "doc_number",
        "Memo/Description": "memo",
        "Department name": "department_name",
        "Location name": "location_name",
        "JNL": "jnl_code",
        "Debit": "debit",
        "Credit": "credit",
        "Balance": "balance",
    }

    transformed_data = []

    for transaction_entry in transactions_data:
        # Rename keys based on mapping
        renamed_entry = {
            mapping.get(old_key, old_key): value
            for old_key, value in transaction_entry.items()
        }

        # Filter out keys not present in the database table
        filtered_entry = {
            key: val for key, val in renamed_entry.items() if key in mapping.values()
        }

        transformed_data.append(filtered_entry)

    return transformed_data


@app.get("/")
async def root():
    return {"message": "Welcome to Record Inserter :)"}


@app.get("/inserter")
async def insert_records():
    try:
        # 1. Define table names and their corresponding csv files

        db_tables = {
            "accounts": "Accounts.csv",
            "departments": "Departments.csv",
            "locations": "Locations.csv",
            "account_groups": "account_groups.csv",
            "transactions": "Transactions.csv",
        }

        table_data = {}

        # 2. Read all csv files and store their data in a dict w.r.t table name
        csv_directory = "/Users/hasnat/Workspace/Fiverr/dduhamel14/db-inserter/"

        for table, file in db_tables.items():
            csv_file = f"{csv_directory}{file}"
            df = pd.read_csv(csv_file)
            # Convert DataFrame to a list of dictionaries
            table_data[table] = df.to_dict(orient="records")

        data_accounts = table_data["accounts"]
        data_departments = table_data["departments"]
        data_locations = table_data["locations"]
        data_account_groups = table_data["account_groups"]
        data_transactions = table_data["transactions"]

        # 3. Connect to the database and drop all the tables in correct order
        Base.metadata.drop_all(bind=engine)
        Base.metadata.create_all(bind=engine)

        data_locations = transform_location_data(data_locations)
        data_departments = transform_department_data(data_departments)
        data_account_groups = transform_account_groups_data(data_account_groups)
        data_transactions = transform_transactions_data(data_transactions)

        # 4. Create the tables in correct order and insert the records in them in the correct flow.
        # Establish a session
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        session = SessionLocal()

        # Insert data into tables
        insert_data(session, Location, data_locations)
        insert_data(session, Department, data_departments)
        insert_data(session, Account, data_accounts)
        insert_data(session, AccountGroup, data_account_groups)
        insert_data(session, Transaction, data_transactions)

        # Close the session
        session.close()

        return {"message": "Data inserted successfully. :)"}

    except pd.errors.EmptyDataError:
        return JSONResponse(
            content={"message": "Uploaded file is empty"}, status_code=400
        )

    except pd.errors.ParserError:
        return JSONResponse(content={"message": "Invalid CSV file"}, status_code=400)
