# DPMon
Differentially-private query engine for Passive Network Measurements

### Objective

The goal of DPMon is not run privacy-preserving queries to a dataset of passive network measurements.
It exploits differential privacy to the results of the query to control the contribution of a single individual to the query value.
Thus, it mitigates the privacy risk in running and spreading network measurrments, allowing network practictioners and stakeholder to share valuable information about network operation, Quality of Service and cyber security threats.

DPMon is designed to operate on flow records, a data format where each TCP or UDP flow consitutes and entry. Each flow is identified by its 5-tuple (IP addresses, port numbers and L4 protocol used) and described by a (rich set) of features, such as packet number and size, domain name, performance metrics (e.g., TCP Round-Trip Time).

## Operation

DPMon exploits the mechanisms of Differential Privacy to add noise to a query's result. In particular, it uses differentially-private mean, sum and histogram as offered by the IBM DiffPrivLib library.
As network measurements are typically large, DPMon is designed to work above Apache Spark for scalable data processing, even if local processing is supported (but suitable only for small datasets).
DPMon can read data in various formats, including NetFlow-like CSV files and Tstat log files

## Pre-requisites

DPMon is a Python package and needs the following packages: `diffprivlib pandas numpy pandasql pyasn cachetools`.
Moreover, when operating on Spark, it is necessary to pass a `pyspark.sql.SparkSession` object as a Spark entrypoint

## Supported data format

To be done

## Usage

To be done

## Limitations

To be done
