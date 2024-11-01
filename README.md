# AWS-SpotifyDB-ETL

In this project an ETL was created ingesting data to a S3 bucket and then transforming it using AWS Glue, keeping transformed data into a S3 Bucket Datawarehouse then tracked and taken into AWS Glue Data Catalog to finally use Athena to query transformed data with complete availability to be used by AWS Quicksight.
Data was extracted from:
https://www.kaggle.com/datasets/tonygordonjr/spotify-dataset-2023

I took as guide the next video: https://www.youtube.com/watch?v=yIc5a7C8aHs
