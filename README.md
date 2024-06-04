# Stock Market ETL with Apache Airflow and AWS S3

This project demonstrates a simple ETL (Extract, Transform, Load) pipeline using Apache Airflow to fetch stock market data from the Alpha Vantage API, transform it using pandas, and load the resulting data into an AWS S3 bucket.

## Project Structure

- **DAG Definition**: The main Airflow DAG that defines the ETL workflow.
- **Python Functions**: Functions for extracting data from Alpha Vantage, transforming the data using pandas, and loading the data to an S3 bucket.

## Airflow Workflow

  ![airflow workflow](https://github.com/raghul3/Stock_market_ETL/assets/81759525/55d0b8de-15ba-403a-b8b4-115a8bcab0b0)

## S3 loading

![Screenshot 2024-06-04 153714](https://github.com/raghul3/Stock_market_ETL/assets/81759525/3f5cf786-5348-4565-af30-8d371efe6778)

## Architecture Diagram

![Stock_market Architecture](https://github.com/raghul3/Stock_market_ETL/assets/81759525/bf472085-2ed1-463c-a129-02f9b94cb7ba)
