import diffprivlib
import pandas as pd
import pandasql as pds
import cachetools
import numpy as np
import pyasn

FORMATS=["tstat", "nfdump"]
TSTAT_BUILTIN_FILTER_OUTGOING = "c_isint == 1 and s_isint == 0"
TSTAT_USER_COL_OUTGOING="c_ip"

TSTAT_BUILTIN_FILTER_INGOING = "c_isint == 0 and s_isint == 1"
TSTAT_USER_COL_INGOING="s_ip"

NFDUMP_BUILTIN_FILTER_OUTGOING = "dir == 1"
NFDUMP_USER_COL_OUTGOING="sa"

NFDUMP_BUILTIN_FILTER_INGOING = "dir == 0"
NFDUMP_USER_COL_INGOING="da"

class DPMon():

    """
    The main class for DPMon. Create on object of the DPMon class to make private queries.
    
    :param path: The path do the data to be analyzed. Can be a string. When using the ``local`` engine, ``path`` can be a list of paths. When using the ``spark`` engine, the path is in the Spark format, thus can include ``*`` and ``{...}`` expression
    :type path: str
    
    :param data_format: Must specify the data format: ``tstat`` of ``nfdump``
    :type data_format: str
    
    :param accountant: A DiffPrivLib ``BudgetAccountant`` that specifies the privacy budget to limit the information it is possible to extract from the data. Create, for example, with: ``diffprivlib.BudgetAccountant(epsilon=1.0)``
    :type accountant: diffprivlib.BudgetAccountant
    
    :param engine: Engine to be used: ``local`` or ``spark``.
        Default: ``"local"``
    :type engine: str
    
    :param spark: In case ``engine = "spark"``, you must provide a ``SparkSession`` as a Spark entrypoint.
                  Default: ``None``
    :type spark: spark.sql.SparkSession
    
    :param direction: Whether to focus on ``outgoing`` flows (those issued by internal clients to the Internet) or ``ingoing`` flows (those issued by any Internet endpoint towards an internal client). See documentation for an explaination.
                      Default: ``"outgoing"``
    :type direction: str
    
    :param ipasn_db: the path of a file in ``pyasn`` format, used to map IP addresses to the corresponding ASN. If the file is provided, it is possible to make queries based on ASN - e.g., the volume to a specific ASN.
                    Default: ``None``
    :type ipasn_db: str
    
    :param head: Truncate the data to ``head`` lines. Useful for debugging.
                    Default: ``None``
    :type head: int
    """
    
    def __init__(self, path, data_format, accountant, engine = "local", spark=None, direction="outgoing", ipasn_db=None, head=None):
        """Constructor method
        """        
        self.path = path
        self.data_format = data_format
        self.engine = engine
        self.spark = spark
        self.accountant = accountant
        self.ipasn_db = ipasn_db
        self.direction = direction
        self.head=head
        
        if not isinstance(path, str) and not isinstance(path, list):
            raise TypeError('path must be a string or a list')
            
        if not isinstance(data_format, str):
            raise TypeError('data_format must be a string')
        if not data_format in FORMATS:
            raise TypeError('data_format must be one of: ' + FORMATS.join(",") )

        if not isinstance(engine, str):
            raise TypeError('engine must be a string')
        if not engine in ["spark", "local"]:
            raise TypeError('engine must be one of: ' + ",".join( ["spark", "local"]) )

        if not isinstance(direction, str):
            raise TypeError('direction must be a string')
        if not direction in ["ingoing", "outgoing"]:
            raise TypeError('direction must be one of: ' + ",".join(["ingoing", "outgoing"]) )
            
        if engine == "spark" and spark==None:
            raise TypeError('Must provide the spark argument when engine is spark')
        
        if not isinstance(accountant, diffprivlib.accountant.BudgetAccountant):
            raise TypeError('accountant must be a instance of diffprivlib.accountant.BudgetAccountant')
            
        if ipasn_db is not None and not isinstance(ipasn_db, str):
            raise TypeError('ipasn_db must be a string')
            
        if data_format=="tstat":
            self.user_col =  TSTAT_USER_COL_INGOING  if direction == "ingoing" else TSTAT_USER_COL_OUTGOING
            self.ipasn_col = TSTAT_USER_COL_OUTGOING if direction == "ingoing" else TSTAT_USER_COL_INGOING
            self.builtin_filter = TSTAT_BUILTIN_FILTER_INGOING if direction == "ingoing" else TSTAT_BUILTIN_FILTER_OUTGOING
            if engine=="spark":
                self.prepare_tstat_spark()
            else:
                self.prepare_tstat_local()
                
        elif data_format == "nfdump":
            self.user_col =  NFDUMP_USER_COL_INGOING  if direction == "ingoing" else NFDUMP_USER_COL_OUTGOING
            self.ipasn_col = NFDUMP_USER_COL_OUTGOING if direction == "ingoing" else NFDUMP_USER_COL_INGOING
            self.builtin_filter = NFDUMP_BUILTIN_FILTER_INGOING if direction == "ingoing" else NFDUMP_BUILTIN_FILTER_OUTGOING
            if engine=="spark":
                self.prepare_nfdump_spark()
            else:
                self.prepare_nfdump_local()     
                
            
    def prepare_tstat_spark(self):
        """
        
        :meta private:
        """    
    
        self.df = self.spark.read.csv(self.path, header = True, inferSchema=True, sep=' ')
        self.df = self.df.toDF ( *[ c.split("#")[-1].split(":")[0] for c in self.df.columns] )
        
    def prepare_tstat_local(self):
        """
        
        :meta private:
        """    
    
        if isinstance(self.path, str):
            self.df = pd.read_csv(self.path, sep=' ', nrows=self.head)  
        elif isinstance(self.path, list):
            self.df = pd.concat((pd.read_csv(f, sep=' ') for f in self.path), ignore_index=True)
        self.df.columns =  [ c.split("#")[-1].split(":")[0] for c in self.df.columns]
        
        
    def prepare_nfdump_spark(self):
        """
        
        :meta private:
        """    
    
        self.df = self.spark.read.csv(self.path, header = True, inferSchema=True)
        
    def prepare_nfdump_local(self):
        """
        
        :meta private:
        """    
    
        if isinstance(self.path, str):
            self.df = pd.read_csv(self.path, nrows=self.head)  
        elif isinstance(self.path, list):
            self.df = pd.concat((pd.read_csv(f) for f in self.path), ignore_index=True)        
        
    def private_query_spark(self, aggregation, metric, epsilon=1.0, bins=10, range=None, bounds=None):
        """
        
        :meta private:
        """
        
        filtered = self.df.filter(self.builtin_filter)
        
        if self.ipasn_db:

            ipasn_db  = self.ipasn_db
            ipasn_col = self.ipasn_col
            def map_partition(rows):
                from pyspark.sql import Row
                asndb = pyasn.pyasn(ipasn_db)
                @cachetools.cached(cache={})
                def get_asn(s_ip):
                    try:
                        return asndb.lookup(s_ip)[0]
                    except:
                        return 0
                
                for row in rows:
                    d = row.asDict()
                    d["asn"] = get_asn(d[ipasn_col])
                    yield Row(**d)
                    
            filtered = filtered.rdd.mapPartitions(map_partition).toDF()
        
        filtered.createOrReplaceTempView("filtered")
        query = self.spark.sql(f"SELECT {self.user_col}, {aggregation} FROM filtered  GROUP BY {self.user_col}")
        query_local = query.toPandas()
        query_local_clean = query_local.drop(columns=self.user_col)
        if len(query_local_clean.columns) == 1:
            values = query_local_clean.iloc[:,0].values
            if metric == "sum":
                return diffprivlib.tools.sum(values, epsilon=epsilon, bounds=bounds, accountant=self.accountant)
            elif metric == "mean":
                return diffprivlib.tools.mean(values, epsilon=epsilon, bounds=bounds, accountant=self.accountant)
            elif metric == "histogram":
                return diffprivlib.tools.histogram(values, epsilon=epsilon, bins=bins, range=range, accountant=self.accountant)
            else:
                raise TypeError('Invalid metric')
        else:
            values = query_local_clean.values
            if metric == "sum":
                return diffprivlib.tools.sum(values, epsilon=epsilon, bounds=bounds, axis=0, accountant=self.accountant)
            if metric == "mean":
                return diffprivlib.tools.mean(values, epsilon=epsilon, bounds=bounds, axis=0, accountant=self.accountant)
            else:
                raise TypeError('Invalid metric')
        
        
    def private_query_local(self, aggregation, metric, epsilon=1.0, bins=10, range=None, bounds=None):
        """
        
        :meta private:
        """
        
        df = self.df
        filtered = pds.sqldf(f"SELECT * from df WHERE {self.builtin_filter}",           locals())
        
        if self.ipasn_db:
            asndb = pyasn.pyasn(self.ipasn_db)
            
            @cachetools.cached(cache={})
            def get_asn(s_ip):
                try:
                    return str(asndb.lookup(s_ip)[0])
                except:
                    return "error"
   
            filtered["asn"] =  filtered[self.ipasn_col].apply(get_asn)
            
        query =    pds.sqldf(f" SELECT {self.user_col}, {aggregation} FROM filtered GROUP BY {self.user_col}", locals())
        del query [self.user_col]
        if len(query.columns) == 1:
            values = query.iloc[:,0].values
            if metric == "sum":
                return diffprivlib.tools.sum(values, epsilon=epsilon, bounds=bounds, accountant=self.accountant)
            elif metric == "mean":
                return diffprivlib.tools.mean(values, epsilon=epsilon, bounds=bounds, accountant=self.accountant)
            elif metric == "histogram":
                return diffprivlib.tools.histogram(values, epsilon=epsilon, bins=bins, range=range, accountant=self.accountant)
            else:
                raise TypeError('Invalid metric')
        else:
            values = query.values
            if metric == "sum":
                return diffprivlib.tools.sum(values, epsilon=epsilon, bounds=bounds, axis=0, accountant=self.accountant)
            if metric == "mean":
                return diffprivlib.tools.mean(values, epsilon=epsilon, bounds=bounds, axis=0, accountant=self.accountant)
            else:
                raise TypeError('Invalid metric')
        
    def private_query(self, aggregation, metric, epsilon=1.0, bins=10, range=None, bounds=None):
        """
        
        :meta private:
        """
        if self.engine=="spark":
            return self.private_query_spark(aggregation=aggregation, metric=metric, epsilon=epsilon, bins=bins, range=range, bounds=bounds)
        elif self.engine=="local":
            return self.private_query_local(aggregation=aggregation, metric=metric, epsilon=epsilon, bins=bins, range=range, bounds=bounds)
        
    def volume_on_ip(self, ip, volume_direction="ingoing", count_flows=False, epsilon=1.0):
        """
        Obtain the traffic volume to a specific external IP address

        :param ip: The IP address to query
        :type ip: str
        
        :param volume_direction: Whether to compute ingress (``"ingoing"``) or egress (``"outgoing"``) volume, in bytes.
                                 Default: ``"ingoing"``
        :type volume_direction: str

        :param count_flows: Count the number of flows instead of volume. If set, ``"volume_direction"`` is ignored.
                            Default: ``False``
        :type count_flows: bool
        
        :param epsilon: The privacy budget to allocate for the query. Default: ``1.0``
        :type epsilon: float
        
        :return: The volume in bytes of number of flows
        :rtype: int

        """            
        if not volume_direction in {"ingoing", "outgoing"}:
            raise TypeError('volume_direction must be one of: ' + ",".join(["ingoing", "outgoing"]) )
            
        if self.data_format=="tstat":
            volume_col = "s_bytes_all" if self.direction != volume_direction else "c_bytes_all"
        elif self.data_format=="nfdump":
            volume_col = "obyt" if self.direction != volume_direction else "ibyt"
            # Assuming ibyt are generated by source and opkt by destination
        
        if count_flows:
            volume_col=1
            
        return int(self.private_query(aggregation = f"sum(CASE WHEN {self.ipasn_col} == '{ip}' THEN {volume_col} ELSE 0 END)", \
                                  metric="sum", epsilon = epsilon))
    
    def volume_on_asn(self, asn, volume_direction="ingoing", count_flows=False, epsilon=1.0):
        """
        Obtain the traffic volume to a specific Autonomous System

        :param asn: The AS number to query
        :type asn: int
        
        :param volume_direction: Whether to compute ingress (``"ingoing"``) or egress (``"outgoing"``) volume, in bytes.
                                 Default: ``"ingoing"``
        :type volume_direction: str

        :param count_flows: Count the number of flows instead of volume. If set, ``"volume_direction"`` is ignored.
                            Default: ``False``
        :type count_flows: bool
        
        :param epsilon: The privacy budget to allocate for the query. Default: ``1.0``
        :type epsilon: float
        
        :return: The volume in bytes of number of flows
        :rtype: int

        """        
        if not volume_direction in {"ingoing", "outgoing"}:
            raise TypeError('volume_direction must be one of: ' + ",".join(["ingoing", "outgoing"]) )

        if not self.ipasn_db:
            raise RuntimeError("Must provide ipasn_db to use volume_on_asn")
            
        if not isinstance(asn, int):
            raise TypeError('asn must be an integer')
            
        if self.data_format=="tstat":
            volume_col = "s_bytes_all" if self.direction != volume_direction else "c_bytes_all"
        elif self.data_format=="nfdump":
            volume_col = "obyt" if self.direction != volume_direction else "ibyt"
            # Assuming ibyt are generated by source and opkt by destination
        
        if count_flows:
            volume_col=1
            
        return int(self.private_query(aggregation = f"sum(CASE WHEN asn == '{asn}' THEN {volume_col} ELSE 0 END)", \
                                  metric="sum", epsilon = epsilon))


    
    def volume_on_domain(self, domain, volume_direction="ingoing", count_flows=False, epsilon=1.0):
        """
        Obtain the traffic volume to a specific domain

        :param domain: The domain name to query
        :type domain: str
        
        :param volume_direction: Whether to compute ingress (``"ingoing"``) or egress (``"outgoing"``) volume, in bytes.
                                 Default: ``"ingoing"``
        :type volume_direction: str

        :param count_flows: Count the number of flows instead of volume. If set, ``"volume_direction"`` is ignored.
                            Default: ``False``
        :type count_flows: bool
        
        :param epsilon: The privacy budget to allocate for the query. Default: ``1.0``
        :type epsilon: float
        
        :return: The volume in bytes of number of flows
        :rtype: int

        """
        
        if not volume_direction in {"ingoing", "outgoing"}:
            raise TypeError('volume_direction must be one of: ' + ",".join(["ingoing", "outgoing"]) )

        if not self.data_format=="tstat":
            raise RuntimeError("Must run volume_on_domain on tstat data")
            
        if not isinstance(domain, str):
            raise TypeError('domain must be a string')
            
        volume_col = "s_bytes_all" if self.direction != volume_direction else "c_bytes_all"

        if count_flows:
            volume_col=1
            
        return int(self.private_query(aggregation = f"sum(CASE WHEN c_tls_SNI == '{domain}' THEN {volume_col} ELSE 0 END)", \
                                  metric="sum", epsilon = epsilon))
