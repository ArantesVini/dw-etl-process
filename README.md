# Data Warehouse ETL Process using Apache Airflow

This repository contains the code and documentation for an ETL process that loads data into a Data Warehouse (DW) in real-time using Apache Airflow.

## Objective

The objective of this project is to implement an ETL process that reads CSV files from a specified folder, cleans, processes, and loads the data into a PostgreSQL-based Data Warehouse following a star schema model.

## Prerequisites

- Python
- Apache Airflow
- PostgreSQL

## Repository Structure

- `/database/` the SQL scripts used to create the DW;
- TODO add the repository strutucre

## About the project

Consider the questions below from a Logistics Manager in a retail network:
• How many deliveries are made per month?
• Out of the total number of deliveries per month, how many are on time?
• What is the percentage of on-time deliveries versus delayed deliveries?
• Which months had the highest number of on-time deliveries?
• Did a specific customer receive more deliveries on time or delayed?

These are typical business questions that managers seek answers to. To answer these and other questions, we will build a Data Warehouse (DW). However, let's automate the DW loading process from the data source in CSV format. Our ETL (Extract, Transform, Load) process will verify the data, perform cleaning tasks, remove inconsistencies, prepare the schema, and then load it into the DW. As the icing on the cake, we will create SQL queries to answer some business questions.

All the data used in this project are fictional, for practice and research purposes only

## DataWarehouse modelling

### Dimensions Tables

- D_CUSTOMER;
- D_SHIPP_COMPANY;
- D_PRODUCT;
- D_DATE;
- D_WAREHOUSE;
- D_DELIVERY;
- D_PAYMENT;

### Fact Table

- **F_LOGISTIC**

## License

This project is licensed under the MIT License - see the [MIT LICENSE](LICENSE) file for details.
