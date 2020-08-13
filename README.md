# Airflow Data Pipeline
### Udacity Data Engineering Nanodegree Data Pipelines Project

## Purpose

The purpiose of this project was to build a data pipeline to enhance Sparkify's data anlaytics process. This process was automated with the use of Apache Aiflow that also will allow us to monitor the ETL process

## Process

The pipeline begins by loading song and log data from s3 where it is stored in JSON format, into staging tables inside a redshift database. From here this data is converted into a star schema that allouws the Sparkify analytics team to easily analyse and work with the data. This process is schedlued in Airflow to run every hour

## The Project

Inside this project we have the following:
* dags
	* main_dag.py: this file sits isnide the dags directory of the airflow set up and is what determines the tasks and dependencies for the job
* operators: these should all be stored in the operators directory inside the plugins directory of the airflow set up
	* data_quality.py: this operator runs a data quality check after the fact and dimension tables have been loaded
    * load_dimension.py: this operator loads the data from the staging tables into teh individual dimension table
    * load_fact.py: this operator loads data from the staging table into the songplays fact table
    * stag_redshift.py: this operator loads data from s3 in JSON format into the staging tables in redshift
* helpers
	* sql_queries: contains the insert statements for all of the redhsift tables (staging, dimension and fact tables)

