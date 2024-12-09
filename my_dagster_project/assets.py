from multiprocessing import context
import pandas as pd
import numpy as np
from pymongo import MongoClient
import json
from dagster import asset, resource, Output, MetadataValue, OutputContext
from dagster import Output, OpDefinition
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from pathlib import Path
import plotly.graph_objects as go
import webbrowser
from pathlib import Path
from dash import dcc, html
import seaborn as sns
import os
import uuid
import pycountry
from fuzzywuzzy import process

countries = [country.name for country in pycountry.countries]
client = MongoClient('mongodb://localhost:27017/')
db = client['database']
collection = db['gap_collection']
collection2 = db['city_collection']
collection3 = db['deaths_collection']
DB_URI = "postgresql://postgres:BroMe@localhost:5432/dagster"
graph = {}


def replace_country_name(country_name, countries):
    closest_match, _ = process.extractOne(country_name, countries)
    return closest_match


@asset
def loading_data(context):
    try:
        df = pd.read_csv('C:/Users/seman/my-dagster-project/gab.csv')
        context.log.info(f"Data loaded successfully. Here's a preview:\n{df.head()}")
        context.log.info(f"Dataset info:\n{df.info()}")
        df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
        return df
    except Exception as e:
        context.log.error(f"Error loading data: {e}")
        raise Exception(f"Error loading data")


@asset
def inserting_data(context, loading_data):
    try:
        df = loading_data
        context.log.info(f"Data to be inserted into MongoDB:\n{df}")
        records = json.loads(df.to_json(orient='records'))
        client = MongoClient('mongodb://localhost:27017/')
        db = client['database']
        collection = db['gap_collection']
        collection.insert_many(records)
        context.log.info("Data successfully inserted into MongoDB.")
        # return{'collection': collection}
    except Exception as e:
        context.log.error(f"Error connecting to MongoDB: {e}")
        raise Exception(f"Error connecting to MongoDB: {e}")


@asset
def data_cleaning_and_extraction(context, inserting_data):
    # collection=inserting_data['collection']
    try:
        extracted_data = list(collection.find())
        context.log.info("Data successfully extracted from MongoDB.")
    except Exception as e:
        context.log.error(f"Error extracting data from MongoDB: {e}")
        raise Exception(f"Error extracting data from MongoDB: {e}")
    try:
        df_extracted = pd.DataFrame(extracted_data)
        df_extracted.drop(columns=['_id'], inplace=True, errors='ignore')
        df_extracted = df_extracted.apply(lambda x: x.str.lower() if x.dtype == "object" else x)
        mapping = {
            "good": 0,
            "moderate": 1,
            "unhealthy for sensitive groups": 2,
            "unhealthy": 3,
            "very unhealthy": 4,
            "hazardous": 5
        }
        df_extracted["aqi_category"] = df_extracted["aqi_category"].replace(mapping)
        df_extracted['country'] = df_extracted['country'].fillna('unknown')
        if 'city' in df_extracted.columns:
            mode_value = df_extracted['city'].mode()[0]
            df_extracted['city'] = df_extracted['city'].fillna(mode_value)
        context.log.info("Data cleaning and transformation completed successfully.")
        return df_extracted
    except Exception as e:
        context.log.error(f"Error during data transformation: {e}")
        raise Exception(f"Error during data transformation: {e}")


@asset
def saving_transformed_data_in_sql(context, data_cleaning_and_extraction):
    df_extracted = data_cleaning_and_extraction
    engine = create_engine(DB_URI)
    try:
        df_extracted.to_sql('transfored_data', engine, if_exists='replace', index=False)
        context.log.info("Transformed data successfully inserted into postgre.")
        return {'table': 'transfored_data'}
    except Exception as e:
        context.log.error(f"Error inserting transformed data into postgre: {e}")
        raise Exception(f"Error inserting transformed data into postgre: {e}")


@asset
def plot_avg_country_aqi(context, saving_transformed_data_in_sql):
    try:
        engine = create_engine(DB_URI)
        query = f"SELECT * FROM {saving_transformed_data_in_sql['table']}"
        df_extracted = pd.read_sql(query, engine)
        avg_per_country = df_extracted.groupby('country')['aqi_value'].mean().reset_index()
        plt.figure(figsize=(18, 12))
        plt.bar(avg_per_country['country'], avg_per_country['aqi_value'])
        plt.title('Average AQI Value per Country')
        plt.xlabel('Country')
        plt.ylabel('Average AQI Value')
        plt.xticks(rotation=90)
        plt.tight_layout()
        graph_path = "C:/Users/seman/my-dagster-project/graph/plot_avg_country_aqi.png"
        plt.savefig(graph_path)
        plt.close()
        context.log.info("Data visualization for Average AQI Value completed successfully.")
        context.log.info(f"Visualization saved at: {graph_path}")

        # Save graph path to PostgreSQL
        graph_paths = [
            {'graph_name': 'plot_avg_country_aqi', 'graph_path': graph_path}
        ]
        df_graph_paths = pd.DataFrame(graph_paths)
        try:
            df_graph_paths.to_sql('Graph', engine, if_exists='append', index=False)
            context.log.info("Graph paths successfully saved into the Graph table in PostgreSQL.")
        except SQLAlchemyError as e:
            context.log.error(f"Error inserting graph paths into PostgreSQL: {e}")
            raise Exception(f"Error inserting graph paths into PostgreSQL: {e}")

        return graph_path
    except Exception as e:
        context.log.error(f"Error during data visualization: {e}")
        raise Exception(f"Error during data visualization: {e}")


@asset
def plot_avg_country_ozone(context, saving_transformed_data_in_sql):
    try:
        engine = create_engine(DB_URI)
        query = f"SELECT * FROM {saving_transformed_data_in_sql['table']}"
        df_extracted = pd.read_sql(query, engine)
        avg_per_country = df_extracted.groupby('country')['ozone_aqi_value'].mean().reset_index()
        plt.figure(figsize=(18, 12))
        plt.plot(avg_per_country['country'], avg_per_country['ozone_aqi_value'])
        plt.title('Average ozone Value per Country')
        plt.xlabel('Country')
        plt.ylabel('Average ozone Value')
        plt.xticks(rotation=90)
        plt.tight_layout()
        graph_path = "C:/Users/seman/my-dagster-project/graph/plot_avg_country_ozone.png"
        plt.savefig(graph_path)
        plt.close()
        context.log.info("Data visualization for Average ozone Value completed successfully.")
        context.log.info(f"Visualization saved at: {graph_path}")

        graph_paths = [
            {'graph_name': 'plot_avg_country_ozone', 'graph_path': graph_path}
        ]
        df_graph_paths = pd.DataFrame(graph_paths)
        try:
            df_graph_paths.to_sql('Graph', engine, if_exists='append', index=False)
            context.log.info("Graph paths successfully saved into the Graph table in PostgreSQL.")
        except SQLAlchemyError as e:
            context.log.error(f"Error inserting graph paths into PostgreSQL: {e}")
            raise Exception(f"Error inserting graph paths into PostgreSQL: {e}")

        return graph_path
    except Exception as e:
        context.log.error(f"Error during data visualization: {e}")
        raise Exception(f"Error during data visualization: {e}")


@asset
def plot_avg_country_CO(context, saving_transformed_data_in_sql):
    try:
        engine = create_engine(DB_URI)
        query = f"SELECT * FROM {saving_transformed_data_in_sql['table']}"
        df_extracted = pd.read_sql(query, engine)
        avg_per_country = df_extracted.groupby('country')['co_aqi_value'].mean().reset_index()
        plt.figure(figsize=(18, 12))
        plt.plot(avg_per_country['country'], avg_per_country['co_aqi_value'])
        plt.title('Average CO Value per Country')
        plt.xlabel('Country')
        plt.ylabel('Average CO Value')
        plt.xticks(rotation=90)
        plt.tight_layout()
        graph_path = "C:/Users/seman/my-dagster-project/graph/plot_avg_country_CO.png"
        plt.savefig(graph_path)
        plt.close()
        context.log.info("Data visualization for Average CO Value completed successfully.")
        context.log.info(f"Visualization saved at: {graph_path}")

        # Save graph path to PostgreSQL
        graph_paths = [
            {'graph_name': 'plot_avg_country_CO', 'graph_path': graph_path}
        ]
        df_graph_paths = pd.DataFrame(graph_paths)
        try:
            df_graph_paths.to_sql('Graph', engine, if_exists='append', index=False)
            context.log.info("Graph paths successfully saved into the Graph table in PostgreSQL.")
        except SQLAlchemyError as e:
            context.log.error(f"Error inserting graph paths into PostgreSQL: {e}")
            raise Exception(f"Error inserting graph paths into PostgreSQL: {e}")

        return graph_path
    except Exception as e:
        context.log.error(f"Error during data visualization: {e}")
        raise Exception(f"Error during data visualization: {e}")


@asset
def plot_avg_country_NO2(context, saving_transformed_data_in_sql):
    try:
        engine = create_engine(DB_URI)
        query = f"SELECT * FROM {saving_transformed_data_in_sql['table']}"
        df_extracted = pd.read_sql(query, engine)
        avg_per_country = df_extracted.groupby('country')['no2_aqi_value'].mean().reset_index()
        plt.figure(figsize=(18, 12))
        plt.plot(avg_per_country['country'], avg_per_country['no2_aqi_value'])
        plt.title('Average NO2 Value per Country')
        plt.xlabel('Country')
        plt.ylabel('Average NO2 Value')
        plt.xticks(rotation=90)
        plt.tight_layout()
        graph_path = "C:/Users/seman/my-dagster-project/graph/plot_avg_country_NO2.png"
        plt.savefig(graph_path)
        plt.close()
        context.log.info("Data visualization for Average NO2 Value completed successfully.")
        context.log.info(f"Visualization saved at: {graph_path}")

        # Save graph path to PostgreSQL
        graph_paths = [
            {'graph_name': 'plot_avg_country_NO2', 'graph_path': graph_path}
        ]
        df_graph_paths = pd.DataFrame(graph_paths)
        try:
            df_graph_paths.to_sql('Graph', engine, if_exists='append', index=False)
            context.log.info("Graph paths successfully saved into the Graph table in PostgreSQL.")
        except SQLAlchemyError as e:
            context.log.error(f"Error inserting graph paths into PostgreSQL: {e}")
            raise Exception(f"Error inserting graph paths into PostgreSQL: {e}")

        return graph_path
    except Exception as e:
        context.log.error(f"Error during data visualization: {e}")
        raise Exception(f"Error during data visualization: {e}")


@asset
def plot_avg_country_PM25(context, saving_transformed_data_in_sql):
    try:
        engine = create_engine(DB_URI)
        query = f"SELECT * FROM {saving_transformed_data_in_sql['table']}"
        df_extracted = pd.read_sql(query, engine)
        avg_per_country = df_extracted.groupby('country')['pm2.5_aqi_value'].mean().reset_index()
        plt.figure(figsize=(18, 12))
        plt.plot(avg_per_country['country'], avg_per_country['pm2.5_aqi_value'])
        plt.title('Average PM2.5 Value per Country')
        plt.xlabel('Country')
        plt.ylabel('Average PM2.5 Value')
        plt.xticks(rotation=90)
        plt.tight_layout()
        graph_path = "C:/Users/seman/my-dagster-project/graph/plot_avg_country_PM25.png"
        plt.savefig(graph_path)
        plt.close()
        context.log.info("Data visualization for Average PM2.5 Value completed successfully.")
        context.log.info(f"Visualization saved at: {graph_path}")

        # Save graph path to PostgreSQL
        graph_paths = [
            {'graph_name': 'plot_avg_country_PM25', 'graph_path': graph_path}
        ]
        df_graph_paths = pd.DataFrame(graph_paths)
        try:
            df_graph_paths.to_sql('Graph', engine, if_exists='append', index=False)
            context.log.info("Graph paths successfully saved into the Graph table in PostgreSQL.")
        except SQLAlchemyError as e:
            context.log.error(f"Error inserting graph paths into PostgreSQL: {e}")
            raise Exception(f"Error inserting graph paths into PostgreSQL: {e}")

        return graph_path
    except Exception as e:
        context.log.error(f"Error during data visualization: {e}")
        raise Exception(f"Error during data visualization: {e}")


@asset
def loading_data2(context):
    try:
        df = pd.read_csv('C:/Users/seman/my-dagster-project/worldcities.csv')
        context.log.info(df.head())
        context.log.info(df.info())
        df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
        return df
    except Exception as e:
        context.log.info(f"Error loading data: {e}")
        return None


@asset
def inserting_data2(context, loading_data2):
    if loading_data2 is None:
        raise ValueError("loading_data2 is None. Please check the data source or upstream process.")
    else:
        context.log.info("data is here")
    try:
        df = loading_data2
        context.log.info(f"Data to be inserted into MongoDB:\n{df}")
        records = json.loads(df.to_json(orient='records'))
        client = MongoClient('mongodb://localhost:27017/')
        db = client['database']
        collection2 = db['city_collection']
        collection2.insert_many(records)
        context.log.info("Data successfully inserted into MongoDB.")
    except Exception as e:
        context.log.error(f"Error connecting to MongoDB: {e}")
        raise Exception(f"Error connecting to MongoDB: {e}")


@asset
def data_cleaning_and_extraction2(context, inserting_data2):
    try:
        extracted_data = list(collection2.find())
        context.log.info("Data successfully extracted from MongoDB.")
    except Exception as e:
        context.log.error(f"Error extracting data from MongoDB: {e}")
        raise Exception(f"Error extracting data from MongoDB: {e}")
    try:
        df_extracted = pd.DataFrame(extracted_data)
        df_extracted.drop(columns=['_id'], inplace=True, errors='ignore')
        df_extracted = df_extracted.apply(lambda x: x.str.lower() if x.dtype == "object" else x)
        df_extracted = df_extracted.dropna(subset=["lat", "lng", "population"])
        return df_extracted
    except Exception as e:
        context.log.error(f"Error during data transformation: {e}")
        raise Exception(f"Error during data transformation: {e}")


@asset
def saving_transformed_data_in_sql2(context, data_cleaning_and_extraction2):
    df_extracted = data_cleaning_and_extraction2
    engine = create_engine(DB_URI)
    try:
        df_extracted.to_sql('transfored_data2', engine, if_exists='replace', index=False)
        context.log.info("Transformed data successfully inserted into postgre.")
        return {'table': 'transfored_data2'}
    except Exception as e:
        context.log.error(f"Error inserting transformed data into postgre: {e}")
        raise Exception(f"Error inserting transformed data into postgre: {e}")


@asset
def plot_10_top_populated_cities(context, saving_transformed_data_in_sql2):
    try:
        engine = create_engine(DB_URI)
        query = f"SELECT * FROM {saving_transformed_data_in_sql2['table']}"
        df_extracted = pd.read_sql(query, engine)
        cities = df_extracted.nlargest(10, 'population')
        plt.figure(figsize=(10, 6))
        plt.barh(cities['city'], cities['population'], color='skyblue')
        plt.xlabel('Population')
        plt.ylabel('City')
        plt.title('Top 10 Most Populated Cities')
        plt.gca().invert_yaxis()
        graph_path = "C:/Users/seman/my-dagster-project/graph/plot_10_top_populated_cities.png"
        plt.savefig(graph_path)
        plt.close()
        context.log.info("Data visualization for Top 10 Most Populated Cities completed successfully.")
        context.log.info(f"Visualization saved at: {graph_path}")

        # Save graph path to PostgreSQL
        graph_paths = [
            {'graph_name': 'plot_10_top_populated_cities', 'graph_path': graph_path}
        ]
        df_graph_paths = pd.DataFrame(graph_paths)
        try:
            df_graph_paths.to_sql('Graph', engine, if_exists='append', index=False)
            context.log.info("Graph paths successfully saved into the Graph table in PostgreSQL.")
        except SQLAlchemyError as e:
            context.log.error(f"Error inserting graph paths into PostgreSQL: {e}")
            raise Exception(f"Error inserting graph paths into PostgreSQL: {e}")

        return graph_path
    except Exception as e:
        context.log.error(f"Error during data visualization: {e}")
        raise Exception(f"Error during data visualization: {e}")


@asset
def plot_to_visualize_cities(context, saving_transformed_data_in_sql2):
    try:
        engine = create_engine(DB_URI)
        query = f"SELECT * FROM {saving_transformed_data_in_sql2['table']}"
        df_extracted = pd.read_sql(query, engine)
        plt.figure(figsize=(12, 8))
        plt.scatter(df_extracted['lng'], df_extracted['lat'], alpha=0.5, c='blue', s=10)
        plt.xlabel('Longitude')
        plt.ylabel('Latitude')
        plt.title('Geographical Distribution of Cities')
        graph_path = "C:/Users/seman/my-dagster-project/graph/plot_to_visualize_cities.png"
        plt.savefig(graph_path)
        plt.close()
        context.log.info("Data visualization for Geographical Distribution of Cities completed successfully.")
        context.log.info(f"Visualization saved at: {graph_path}")

        graph_paths = [
            {'graph_name': 'plot_to_visualize_cities', 'graph_path': graph_path}
        ]
        df_graph_paths = pd.DataFrame(graph_paths)
        try:
            df_graph_paths.to_sql('Graph', engine, if_exists='append', index=False)
            context.log.info("Graph paths successfully saved into the Graph table in PostgreSQL.")
        except SQLAlchemyError as e:
            context.log.error(f"Error inserting graph paths into PostgreSQL: {e}")
            raise Exception(f"Error inserting graph paths into PostgreSQL: {e}")

        return graph_path
    except Exception as e:
        context.log.error(f"Error during data visualization: {e}")
        raise Exception(f"Error during data visualization: {e}")


@asset
def Plot_heatmap_for_population_by_regions(context, saving_transformed_data_in_sql2):
    try:
        engine = create_engine(DB_URI)
        query = f"SELECT * FROM {saving_transformed_data_in_sql2['table']}"
        df_extracted = pd.read_sql(query, engine)
        region_population = df_extracted.groupby('admin_name')['population'].sum().dropna().sort_values(ascending=False)
        region_population_df = region_population.reset_index()
        heatmap_data = pd.pivot_table(region_population_df, values='population', index='admin_name')
        heatmap_data = heatmap_data.sort_values('population', ascending=False)
        plt.figure(figsize=(10, 16))
        sns.heatmap(heatmap_data, cmap='viridis', annot=False, cbar_kws={'label': 'Total Population'}, linewidths=0.5)
        plt.title('Population Heatmap by Region')
        plt.xlabel('Total Population')
        plt.ylabel('Region')
        graph_path = "C:/Users/seman/my-dagster-project/graph/Plot_heatmap_for_population_by_regions.png"
        plt.savefig(graph_path)
        plt.close()
        context.log.info("Data visualization for Population Heatmap by Region completed successfully.")
        context.log.info(f"Visualization saved at: {graph_path}")
        graph_paths = [
            {'graph_name': 'Plot_heatmap_for_population_by_regions', 'graph_path': graph_path}
        ]
        df_graph_paths = pd.DataFrame(graph_paths)
        try:
            df_graph_paths.to_sql('Graph', engine, if_exists='append', index=False)
            context.log.info("Graph paths successfully saved into the Graph table in PostgreSQL.")
        except SQLAlchemyError as e:
            context.log.error(f"Error inserting graph paths into PostgreSQL: {e}")
            raise Exception(f"Error inserting graph paths into PostgreSQL: {e}")

        return graph_path
    except Exception as e:
        context.log.error(f"Error during data visualization: {e}")
        raise Exception(f"Error during data visualization: {e}")


@asset
def loading_data3(context):
    try:
        df = pd.read_json("C:/Users/seman/my-dagster-project/death.json", lines=True)
        context.log.info(df.head())
        context.log.info(df.info())
        df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
        return df
    except Exception as e:
        context.log.info(f"Error loading data: {e}")
        return None


@asset
def inserting_data3(context, loading_data3):
    try:
        df = loading_data3
        context.log.info(f"Data to be inserted into MongoDB:\n{df}")
        records = json.loads(df.to_json(orient='records'))
        client = MongoClient('mongodb://localhost:27017/')
        db = client['database']
        collection3 = db['deaths_collection']
        collection3.insert_many(records)
        context.log.info("Data successfully inserted into MongoDB.")
    except Exception as e:
        context.log.error(f"Error connecting to MongoDB: {e}")
        raise Exception(f"Error connecting to MongoDB: {e}")


@asset
def data_cleaning_and_extraction3(context, inserting_data3):
    try:
        extracted_data = list(collection3.find())
        context.log.info("Data successfully extracted from MongoDB.")
    except Exception as e:
        context.log.error(f"Error extracting data from MongoDB: {e}")
        raise Exception(f"Error extracting data from MongoDB: {e}")
    try:
        df_extracted = pd.DataFrame(extracted_data)
        df_extracted.drop(columns=['_id'], inplace=True, errors='ignore')
        df_extracted = df_extracted.apply(lambda x: x.str.lower() if x.dtype == "object" else x)
        # df_extracted = df_extracted.dropna(subset=["Outdoor air pollution", "Smoking", "Year"])
        return df_extracted
    except Exception as e:
        context.log.error(f"Error during data transformation: {e}")
        raise Exception(f"Error during data transformation: {e}")


@asset
def saving_transformed_data_in_sql3(context, data_cleaning_and_extraction3):
    df_extracted = data_cleaning_and_extraction3
    engine = create_engine(DB_URI)
    try:
        df_extracted.to_sql('transform_data3', engine, if_exists='replace', index=False)
        context.log.info("Transformed data successfully inserted into postgre.")
        return {'table': 'transform_data3'}
    except Exception as e:
        context.log.error(f"Error inserting transformed data into postgre: {e}")
        raise Exception(f"Error inserting transformed data into postgre: {e}")


@asset
def visualize_stacked_area_chart(context, saving_transformed_data_in_sql3):
    try:
        engine = create_engine(DB_URI)
        query = f"SELECT * FROM {saving_transformed_data_in_sql3['table']}"
        data = pd.read_sql(query, engine)
        required_columns = ["year", "outdoor_air_pollution", "smoking", "tuberculosis_fatalities",
                            "cardiovascular_fatalities"]
        if not all(col in data.columns for col in required_columns):
            raise ValueError(f"Data is missing required columns. Expected columns: {required_columns}")
        grouped_data = data.groupby("year")[["outdoor_air_pollution", "smoking", "tuberculosis_fatalities",
                                             "cardiovascular_fatalities"]].sum().reset_index()
        plt.figure(figsize=(12, 8))
        plt.stackplot(
            grouped_data["year"],
            grouped_data["outdoor_air_pollution"],
            grouped_data["smoking"],
            grouped_data["tuberculosis_fatalities"],
            grouped_data["cardiovascular_fatalities"],
            labels=["outdoor Air Pollution", "smoking", "tuberculosis fatalities", "cardiovascular fatalities"]
        )
        plt.title("Stacked Area Chart: Contribution of Factors to Total Fatalities Over Time", fontsize=14)
        plt.xlabel("Year", fontsize=12)
        plt.ylabel("Fatalities", fontsize=12)
        plt.legend(loc="upper left")
        plt.tight_layout()
        graph_path = "C:/Users/seman/my-dagster-project/graph/visualize_stacked_area_chart.png"
        plt.savefig(graph_path)
        plt.close()
        context.log.info("Data visualization for stacked_area_chart completed successfully.")
        context.log.info(f"Stacked area chart saved at: {graph_path}")
        graph_paths = [
            {'graph_name': 'visualize_stacked_area_chart', 'graph_path': graph_path}
        ]
        df_graph_paths = pd.DataFrame(graph_paths)
        try:
            df_graph_paths.to_sql('Graph', engine, if_exists='append', index=False)
            context.log.info("Graph paths successfully saved into the Graph table in PostgreSQL.")
        except SQLAlchemyError as e:
            context.log.error(f"Error inserting graph paths into PostgreSQL: {e}")
            raise Exception(f"Error inserting graph paths into PostgreSQL: {e}")
        return graph_path

    except Exception as e:
        context.log.error(f"Error during visualize_stacked_area_chart: {e}")
        raise Exception(f"Error during visualize_stacked_area_chart: {e}")

@asset
def plot_multi_line_chart(context, saving_transformed_data_in_sql3):
    try:
        engine = create_engine(DB_URI)
        query = f"SELECT * FROM {saving_transformed_data_in_sql3['table']}"
        df_extracted = pd.read_sql(query, engine)
        required_columns = ["year", "entity", "outdoor_air_pollution"]
        if not all(col in df_extracted.columns for col in required_columns):
            raise ValueError(f"Data is missing required columns: {required_columns}")
        pivot_data = df_extracted.pivot(index="year", columns="entity", values="outdoor_air_pollution").reset_index()
        plt.figure(figsize=(12, 8))
        for country in pivot_data.columns[1:]:
            plt.plot(pivot_data["year"], pivot_data[country], label=country)
        plt.title("Multi-Line Chart: Outdoor Air Pollution Trends Across Countries", fontsize=14)
        plt.xlabel("Year", fontsize=12)
        plt.ylabel("Outdoor air pollution", fontsize=12)
        plt.legend(title="Country", loc="upper left")
        plt.tight_layout()
        graph_path = "C:/Users/seman/my-dagster-project/graph/plot_multi_line_chart.png"
        plt.savefig(graph_path)
        plt.close()
        context.log.info("Multi-Line Chart visualization for Outdoor Air Pollution completed successfully.")
        context.log.info(f"Visualization saved at: {graph_path}")
        graph_paths = [
            {'graph_name': 'plot_multi_line_chart', 'graph_path': graph_path}
        ]
        df_graph_paths = pd.DataFrame(graph_paths)
        try:
            df_graph_paths.to_sql('Graph', engine, if_exists='append', index=False)
            context.log.info("Graph paths successfully saved into the Graph table in PostgreSQL.")
        except SQLAlchemyError as e:
            context.log.error(f"Error inserting graph paths into PostgreSQL: {e}")
            raise Exception(f"Error inserting graph paths into PostgreSQL: {e}")
        return graph_path
    except Exception as e:
        context.log.error(f"Error during multi-line chart visualization: {e}")
        raise Exception(f"Error during multi-line chart visualization: {e}")

@asset
def preparing_data_for_merging_data(context, saving_transformed_data_in_sql, saving_transformed_data_in_sql2,
                                    saving_transformed_data_in_sql3):
    context.log.info("preparing dat for merging in proceess")
    engine = create_engine(DB_URI)
    query1 = f"SELECT * FROM {saving_transformed_data_in_sql['table']}"
    query2 = f"SELECT * FROM {saving_transformed_data_in_sql2['table']}"
    query3 = f"SELECT * FROM {saving_transformed_data_in_sql3['table']}"
    gab_df = pd.read_sql(query1, engine)
    worldcities_df = pd.read_sql(query2, engine)
    death_df = pd.read_sql(query3, engine)
    death_df.rename(columns={'entity': 'country'}, inplace=True)

    numeric_columns = ['co_aqi_value', 'ozone_aqi_value', 'no2_aqi_value', 'pm2.5_aqi_value', 'aqi_value']
    gab_df = gab_df.groupby('country')[numeric_columns].mean().reset_index()
    numeric_columns2 = ['outdoor_air_pollution', 'smoking', 'tuberculosis_fatalities', 'cardiovascular_fatalities']
    death_df = death_df.groupby('country')[numeric_columns2].mean().reset_index()
    worldcities_df = worldcities_df.groupby('country')['population'].sum().reset_index()

    gab_df['country'] = gab_df['country'].apply(lambda x: replace_country_name(x, countries))
    worldcities_df['country'] = worldcities_df['country'].apply(lambda x: replace_country_name(x, countries))
    death_df['country'] = death_df['country'].apply(lambda x: replace_country_name(x, countries))
    gab_df['country'] = gab_df['country'].apply(lambda x: replace_country_name(x, countries))

    gab_df = gab_df.drop_duplicates(subset='country', keep='first')

    worldcities_df['country'] = worldcities_df['country'].apply(lambda x: replace_country_name(x, countries))
    worldcities_df = worldcities_df.drop_duplicates(subset='country', keep='first')

    death_df['country'] = death_df['country'].apply(lambda x: replace_country_name(x, countries))
    death_df = death_df.drop_duplicates(subset='country', keep='first')

    return {'data1': gab_df, 'data2': worldcities_df, 'data3': death_df}


@asset
def merging_data(context, preparing_data_for_merging_data):
    gab_df = preparing_data_for_merging_data['data1']
    worldcities_df = preparing_data_for_merging_data['data2']
    death_df = preparing_data_for_merging_data['data3']

    merged_df1 = pd.merge(gab_df, worldcities_df, on='country', how='inner')
    final_merged_df = pd.merge(merged_df1, death_df, on='country', how='inner')
    return {'merge': fal_merged_df}


@asset
def save_merged_data(context, merging_data):
    final_merged_df = merging_data['merge']
    engine = create_engine(DB_URI)
    try:
        final_merged_df.to_sql('merged_data', engine, if_exists='replace', index=False)
        context.log.info("Merged data successfully inserted into postgre.")
        return {'table': 'merged_data'}
    except Exception as e:
        context.log.error(f"Error inserting transformed data into postgre: {e}")
        raise Exception(f"Error inserting transformed data into postgre: {e}")


@asset
def plot_population_vs_aqi_value(context, save_merged_data):
    engine = create_engine(DB_URI)
    query = f"SELECT * FROM {save_merged_data['table']}"
    merge = pd.read_sql(query, engine)
    try:
        plt.figure(figsize=(10, 6))
        plt.scatter(merge['population'], merge['aqi_value'], alpha=0.6, edgecolors='w', linewidth=0.5)
        plt.xlabel('Population')
        plt.ylabel('AQI Value')
        plt.title('Population vs AQI Value')
        plt.grid(True)
        graph_path = "C:/Users/seman/my-dagster-project/graph/plot_population_vs_aqi_value.png"
        plt.savefig(graph_path)
        plt.close()
        context.log.info("plot_population_vs_aqi_value completed successfully.")
        context.log.info(f"Visualization saved at: {graph_path}")
        graph_paths = [{'graph_name': 'plot_population_vs_aqi_value', 'graph_path': graph_path}]
        df_graph_paths = pd.DataFrame(graph_paths)
        try:
            df_graph_paths.to_sql('Graph', engine, if_exists='append', index=False)
            context.log.info("Graph paths successfully saved into the Graph table in PostgreSQL.")
        except SQLAlchemyError as e:
            context.log.error(f"Error inserting graph paths into PostgreSQL: {e}")
            raise Exception(f"Error inserting graph paths into PostgreSQL: {e}")
        return graph_path
    except Exception as e:
        context.log.error(f"Error during violin plot visualization: {e}")
        raise Exception(f"Error during violin plot visualization: {e}")


@asset
def generate_html_report(context, plot_avg_country_aqi,
                         plot_avg_country_ozone,
                         plot_avg_country_CO,
                         plot_avg_country_NO2,
                         plot_avg_country_PM25,
                         plot_10_top_populated_cities,
                         plot_to_visualize_cities,
                         Plot_heatmap_for_population_by_regions,
                         visualize_stacked_area_chart,
                         plot_multi_line_chart,
                         plot_population_vs_aqi_value
                         ):
    try:
        html_content = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title> DASHBOARD FOR PROJECT </title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 40px; }}
                img {{ max-width: 80%; height: auto; display: block; margin: 20px auto; }}
                h2 {{ text-align: center; }}
            </style>
        </head>
        <body>
            <h1>DASHBOARD FOR PROJECT</h1>
            <h2>Average AQI Value per Country</h2>
            <img src="{plot_avg_country_aqi}" alt="Average AQI Value per Country">

            <h2>Average Ozone Value per Country</h2>
            <img src="{plot_avg_country_ozone}" alt="Average Ozone Value per Country">

            <h2>Average CO Value per Country</h2>
            <img src="{plot_avg_country_CO}" alt="Average CO Value per Country">

            <h2>Average NO2 Value per Country</h2>
            <img src="{plot_avg_country_NO2}" alt="Average NO2 Value per Country">

            <h2>Average PM2.5 Value per Country</h2>
            <img src="{plot_avg_country_PM25}" alt="Average PM2.5 Value per Country">

            <h2>Top 10 Most Populated Cities</h2>
            <img src="{plot_10_top_populated_cities}" alt="Top 10 Most Populated Cities">

            <h2>Geographical Distribution of Cities</h2>
            <img src="{plot_to_visualize_cities}" alt="Geographical Distribution of Cities">

            <h2>Population Heatmap by Region</h2>
            <img src="{Plot_heatmap_for_population_by_regions}" alt="Population Heatmap by Region">

            <h2>Population Heatmap by Region</h2>
            <img src="{visualize_stacked_area_chart}" alt="Visualize_stacked_area_chart">

            <h2>Population Heatmap by Region</h2>
            <img src="{plot_multi_line_chart}" alt="plot_multi_line_chart">

            <h2>Population Heatmap by Region</h2>
            <img src="{plot_population_vs_aqi_value}" alt="plot_violin_chart">


        </body>
        </html>
        """

        report_path = "C:/Users/seman/my-dagster-project/report.html"
        report_directory = Path(report_path).parent
        report_directory.mkdir(parents=True, exist_ok=True)

        with open(report_path, "w", encoding="utf-8") as file:
            file.write(html_content)

        context.log.info("HTML report generated successfully.")
        context.log.info(f"HTML report saved at: {report_path}")
        webbrowser.open(f"file://{Path(report_path).absolute()}")
        return report_path
    except Exception as e:
        context.log.error(f"Error generating HTML report: {e}")
        raise Exception(f"Error generating HTML report: {e}")

