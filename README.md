# Data Engineering on video streaming dataset

**Project Summary**

This project leverages a suite of AWS services to build a scalable, efficient, and secure data pipeline for managing and analysing YouTube video data. By implementing a robust ETL process, creating a centralised data lake, and developing insightful dashboards, we aim to provide valuable insights into YouTube video trends and categories, supporting data-driven decision-making.

**AWS Services Descriptions:**
* Amazon S3: Scalable object storage service offering high availability, security, and performance for any amount of data.
* AWS IAM: Manages secure access to AWS services and resources, controlling permissions for users and applications.
* Amazon QuickSight: Cloud-based BI service for creating interactive dashboards and reports, powered by machine learning.
* AWS Glue: Serverless data integration service for discovering, preparing, and combining data for analytics and machine learning.
* AWS Lambda: Serverless compute service that runs code in response to events without provisioning or managing servers.
* AWS Athena: Interactive query service that uses standard SQL to analyse data directly in Amazon S3, with no data loading required.

**Dataset Used:**

The dataset from Kaggle (https://www.kaggle.com/datasets/datasnaek/youtube-new) includes statistics on daily popular YouTube videos over several months, provided as CSV files. Each day, up to 200 trending videos are recorded for various locations, with separate files for each region. The data includes video title, channel title, publication time, tags, views, likes, dislikes, description, and comment count. Additionally, a category_id field, specific to each region, is included in the accompanying JSON file.

**Architecture:**

<img width="1129" alt="image" src="https://github.com/tripats6/dataengineering-on-raw-data-from-source/assets/168261501/a83b6395-a475-45a0-8d43-53d287f6a8ea">


