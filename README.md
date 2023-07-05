# Airflow song data pipeline 

This repo creates a data pipeline in Airflow based on starter code and instructions from Udacity. 

The Airflow DAG `final_project` does the following: 
* Create staging tables by coping song play and song data from S3 
* Create and fill final tables from staging tables using Redshift queries 
* Check that key final tables (`users` and `songplays`) have no missing data. 

The DAG uses operators to hold shared logic for Redshift operations. 

The DAG uses Variables and Connections to store S3 locations and permissions. 

The DAG uses the `TRUNCATE` variable to determine whether tables should be emptied and re-created before load. 