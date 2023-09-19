# DPMon
Differentially-private query engine for Passive Network Measurements

### Objective

The goal of DPMon is run privacy-preserving queries to a dataset of passive network measurements.
It applies Differential Privacy to the results of the query to control the contribution of a single individual to the query value.
Thus, it mitigates the privacy risk of running and spreading network measurements, allowing network practitioners and stakeholders to share valuable information about network operation, Quality of Service and cyber security threats.

DPMon is designed to operate on flow records, a data format where each TCP or UDP flow constitutes and entry. Each flow is identified by its 5-tuple (IP addresses, port numbers and L4 protocol used) and described by a (rich set) of features, such as packet number and size, domain name, performance metrics (e.g., TCP Round-Trip Time).

## Operation

DPMon exploits the mechanisms of Differential Privacy to add noise to a query's result. In particular, it uses differentially-private mean, sum and histogram as offered by the IBM DiffPrivLib library.
As network measurements are typically large, DPMon is designed to work above Apache Spark for scalable data processing, even if local processing is supported (but suitable only for small datasets).
DPMon can read data in various formats, including NetFlow-like CSV files and Tstat log files

**Autonomous System Mapping:** DPMon supports query on the server IP's autonomous system. To this end, it leverages the `pyasn` library. You must provide DPMon with a `pyasn` RIB file that contains the mapping between IPs and ASN. Please refer to [PyASN documentation](https://github.com/hadiasghari/pyasn) to obtain an up-to-date data file.

## Pre-requisites

DPMon is a Python package and needs the following packages: `diffprivlib pandas numpy pandasql pyasn cachetools sphinx myst-parser linkify-it-py sphinx-rtd-theme`.
Moreover, when operating on Spark, it is necessary to pass a `pyspark.sql.SparkSession` object as a Spark entrypoint

## Supported data format

DPMon can read log files in two formats (so far):

- Tstat: log files created by [Tstat](http://tstat.polito.it/), a passive meter exporting rich flow records with hundreds of features, including packet and byte volume, TCP RTT, domain name, etc.
- NFDump: NetFlow records converted in `csv` using the standard NFDump tool. To convert a NetFlow database (created by NFCapd) in CSV, we assume you use the command `nfdump -r file -o extended -o csv`.

Files must be available on the running machine in case of local processing and available to Spark Executors in case of Spark Execution mode.


### Flow direction

DPMon protects the privacy of users, thus, the operator must specify who are the users. You must pay attention to this step, as misconfiguration may lead to undesired privacy leaks.

For both supported data formats, the logs specify flow direction: in case of Tstat, there are the `c_isint` and `s_isint` columns, while for NFDump, there is the `dir` field. DPMon assumes users to be protected are those flagged as clients in Tstat and source of outgoing flows (and destination of incoming ones) in NFDump. Verify your data follows these conventions.

As a consequence of the design, DPMon cannot operate at the same time with incoming and outgoing flows. When starting DPMon, you must specify if you want to run queries on incoming or outgoing flows. Then, DPMon will operate to protect the privacy of internal clients (i.e., your users).



## Supported engines

By default DPMon processes data locally using Pandas. This is call as `local` engine.

It is possible to use the `spark` engine, so that one can leverage a big data cluster to process large quantities of data. Cluster set up and configuration is not part of this guide.

## Usage

To use DPMon, you must instantiate an object of the `dpmon.DPMon`, setting the total privacy budget. Then, you can use the class methods to run queries on the data, specifying the privacy budget to allocate to each query.
The various methods allow you to run different queries on the data, such as byte-wise volume, number of flows, etc.
You can make global queries or filter data by server IP address, ASN or domain.

Check the module [documentation](https://marty90.github.io/DPMon/index.html) for details and the exhaustive list.



## Limitations

DPMon is prototype, thus inspect it carefully before using it in a production environment. Moreover, it is a library to be used by the system administrator to extract data. It shall not be used directly by an external analyst. In other words, consider to build a web service on top of it, to allow external use and enforce policies and privileges.

