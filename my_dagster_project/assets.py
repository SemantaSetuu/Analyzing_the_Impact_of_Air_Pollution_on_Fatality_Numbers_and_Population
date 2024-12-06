from dagster import asset
import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine
import json

# Configuration
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "semanta"
COLLECTION_NAME = "semanta"
POSTGRES_URI = "postgresql://postgres:BroMe@localhost:5432/checking"


# Step 1: Load JSON data
@asset
def loading_data2(context):
    json_file_path = "C:/Users/seman/Desktop/death.json"
    try:
        with open(json_file_path, 'r') as file:
            data = [json.loads(line) for line in file]
        context.log.info(f"Loaded {len(data)} records from JSON file.")
        return data
    except Exception as e:
        context.log.error(f"Error loading JSON data: {e}")
        raise


# Step 2: Connect to MongoDB
@asset
def connect_to_mongodb2(context):
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        db.list_collection_names()  # Test connection
        context.log.info(f"Connected to MongoDB database: {DB_NAME}.")
        return {"status": "connected", "db_name": DB_NAME}
    except Exception as e:
        context.log.error(f"Error connecting to MongoDB: {e}")
        raise


# Step 3: Insert Data into MongoDB
@asset
def insert_data_into_mongodb2(context, loading_data2, connect_to_mongodb2):
    try:
        # Check MongoDB connection status
        if connect_to_mongodb2["status"] != "connected":
            raise ValueError("MongoDB connection failed.")

        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]

        result = collection.insert_many(loading_data2)
        context.log.info(f"Inserted {len(result.inserted_ids)} records into MongoDB.")
        return {"status": "success", "inserted_records": len(result.inserted_ids)}
    except Exception as e:
        context.log.error(f"Error inserting data into MongoDB: {e}")
        raise


# Step 4: Fetch Data from MongoDB
@asset
def fetching_data2(context, insert_data_into_mongodb2):
    try:
        # Check if data was inserted successfully
        if insert_data_into_mongodb2["status"] != "success":
            raise ValueError("Data insertion into MongoDB was not successful.")

        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]

        cursor = collection.find()
        data = pd.DataFrame(list(cursor))
        if '_id' in data.columns:
            data.drop('_id', axis=1, inplace=True)
        context.log.info(f"Fetched {len(data)} records from MongoDB.")
        return data
    except Exception as e:
        context.log.error(f"Error fetching data from MongoDB: {e}")
        raise


# Step 5: Clean the Data
@asset
def cleaning_data2(context, fetching_data2):
    try:
        df_cleaned = fetching_data2.dropna()
        df_cleaned = df_cleaned.drop_duplicates()
        context.log.info(f"Cleaned data: {len(df_cleaned)} rows remain after cleaning.")
        return df_cleaned
    except Exception as e:
        context.log.error(f"Error cleaning data: {e}")
        raise


# Step 6: Connect to PostgreSQL
@asset
def connect_to_postgres2(context):
    try:
        # Test connection
        engine = create_engine(POSTGRES_URI)
        with engine.connect() as connection:
            context.log.info("Connected to PostgreSQL database successfully.")
        return {"status": "connected", "db_name": "checking"}
    except Exception as e:
        context.log.error(f"Error connecting to PostgreSQL: {e}")
        raise


# Step 7: Insert Cleaned Data into PostgreSQL
@asset
def insert_data_into_postgres2(context, cleaning_data2, connect_to_postgres2):
    try:
        # Check PostgreSQL connection status
        if connect_to_postgres2["status"] != "connected":
            raise ValueError("PostgreSQL connection failed.")

        # Create a new connection
        engine = create_engine(POSTGRES_URI)

        # Insert data
        cleaning_data2.to_sql("cleaned", engine, if_exists="replace", index=False)
        context.log.info("Inserted cleaned data into PostgreSQL.")
        return {"status": "success", "table_name": "cleaned"}
    except Exception as e:
        context.log.error(f"Error inserting data into PostgreSQL: {e}")
        raise


# Step 8: Fetch Cleaned Data from PostgreSQL
@asset
def fetching_data_from_postgres2(context, insert_data_into_postgres2):
    try:
        # Check if data was inserted successfully
        if insert_data_into_postgres2["status"] != "success":
            raise ValueError("Data insertion into PostgreSQL was not successful.")

        # Create a new connection
        engine = create_engine(POSTGRES_URI)

        # Fetch data
        query = "SELECT * FROM cleaned"
        df_postgre = pd.read_sql(query, engine)
        context.log.info("Fetched cleaned data from PostgreSQL.")
        return df_postgre
    except Exception as e:
        context.log.error(f"Error fetching data from PostgreSQL: {e}")
        raise
#for testing purpose
#another line
