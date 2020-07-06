## Data Modeling with Postgres

This repository contains files for the Project 1 of Udacity Data Engineering Nanodegree.

### Project files

Project consists of two Jupyter Notebooks and three Python scripts:

* [etl.ipynb](./etl.ipynb) – Notebook that illustrates main components of ETL pipeline by loading two example files into Postgres database
* [test.ipynb](./test.ipynb) – notebook that uses SQL extension to show current state of all project database tables, that can be used for debugging the ETL pipeline
* [sql_queries.py](./sql_queries.py) – script, containing DDL and DML queries used to drop and create database tables and populate them with data
* [create_tables.py](./create_tables.py) – helper script that used to recreate empty tables according to schema, defined in `sql_queries.py`
* [etl.py](./etl.py) – script containing the implementation of ETL pipeline
* [data](./data) folder contains datasets copied from the workspace



### Usage

To create database tables in their initial state, make sure that there are no open connections to the `sparkifydb` (e.g. all IPython kernels are shut down) and run the following command:

```bash
python ./create_tables.py
```

Alternetively, you can combine two scripts to re-create database tables and run the ETL pipeline on all files by issuing:

```bash
python ./create_tables.py && python ./etl.py
```

