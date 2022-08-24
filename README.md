# Source to Target Validator
The primary aim of these Python modules is to provide automated tests that validate source data and target data. That is, the modules will automatically compare source data and target data and provide an anomly report that summarizes the differences or lack thereof between the two. Note that these modules only cover structured data.

These modules are intended to be run on Google Cloud data sources such as Google Cloud Storage and Google BigQuery. However, this does not mean that the validator is confined to GCP. It is easily extendable to Amazon Web Services (AWS), Microsoft Azure, and virtually any platform with open source Python client libraries.

The following parameters will be tested:
1. Row count - the number of rows in the table
2. Schema - the name and data type of each column
3. Duplicate rows - two or more rows that are exactly the same
4. Diff - entry by entry comparison between two tables

## Pre-requisites
There are 2 ways to run the modules: (1) Locally and (2) within a cloud environment
1. Running the modules locally
   a. Please follow the appropriate authorization steps. For Google Cloud, the steps can be found here: https://cloud.google.com/docs/authentication/getting-started.
   b. Make sure to save the authenticator json key on the same folder as your project.
   c. Run this command on terminal: $env:GOOGLE_APPLICATION_CREDENTIALS="<filename_of_authenticator_key>.json"

2. Running the modules within a cloud environment
   a. Ensure that you have IAM permissions to run Python scripts via Cloud Shell. 
      Note that these modules can also be run within Cloud Functions in the event that testing needs to be automated and scalable.

## Running the modules
Currently, there are three modules:
1. gcs_to_gcs - compares a source file on GCS with a target file on GCS
2. gcs_to_bq - compares a source file on GCS with a target table on BQ
3. bq_to_bq - comparies a source table on bq with a target table on BQ

To run a module, the command pattern is as follows:
`python main.py [path to config file]`

### Config
The config file is a YAML file stored locally. It contains the list of jobs to be run. Each job should contain three parameters:
1. module to run 
2. sources - a list of all the source data
3. targets - a list of all the target data
4. mode - either 'default' or 'strict'. this is relevant to compare_schema wherein the 'default' mode only considers the names of the columns and 'strict' mode also considers the data type of each column.

A sample config file is included as part of the repository.

## Anomaly Report
After running the commands, an anomaly report will be generated locally. It will be in the form of a .txt file with the following file format: `anomaly_report_[timestamp].txt`

The anomaly report will contain a summary of all the aforementioned tests. Additionally, it will also contain a full diff -- meaning it will include the cell-by-cell comparison of the source data and target data.

Information contained within the report includes:
1. source data
2. target data
3. row count check results
4. schema check results - it also includes the "mode". mode can either be default or strict. default mode means it will only check if the column names are the same and ignore the data types. strict mode will also consider the data type of each column.
5. duplicate check results
6. diff check results