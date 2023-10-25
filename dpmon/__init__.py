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

TSTAT_ALLOWED_FEATURES="c_pkts_all c_rst_cnt c_ack_cnt c_ack_cnt_p c_bytes_uniq c_pkts_data c_bytes_all c_pkts_retx c_bytes_retx c_pkts_ooo c_syn_cnt c_fin_cnt s_pkts_all s_rst_cnt s_ack_cnt s_ack_cnt_p s_bytes_uniq s_pkts_data s_bytes_all s_pkts_retx s_bytes_retx s_pkts_ooo s_syn_cnt s_fin_cnt durat c_first s_first c_last s_last c_first_ack s_first_ack c_rtt_avg c_rtt_min c_rtt_max c_rtt_std c_rtt_cnt c_ttl_min c_ttl_max s_rtt_avg s_rtt_min s_rtt_max s_rtt_std s_rtt_cnt s_ttl_min s_ttl_max c_pkts_push s_pkts_push c_last_handshakeT s_last_handshakeT c_appdataT s_appdataT c_appdataB s_appdataB".split()

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
        _ = self.df.cache()
        
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
        
    def private_query_spark(self, aggregation, metric, epsilon=1.0, bins=10, range=None, bounds=None, percent=None):
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
            bounds = bounds if bounds is not None else (np.nanmin(values), np.nanmax(values)) # Workaround for diffprilib bug
            if metric == "sum":
                return diffprivlib.tools.nansum(values, epsilon=epsilon, bounds=bounds, accountant=self.accountant)
            elif metric == "mean":
                return diffprivlib.tools.nanmean(values, epsilon=epsilon, bounds=bounds, accountant=self.accountant)
            elif metric == "std":
                return diffprivlib.tools.nanstd(values, epsilon=epsilon, bounds=bounds, accountant=self.accountant)
            elif metric == "percentile":
                values = values[~np.isnan(values)]
                return diffprivlib.tools.percentile(values, percent, epsilon=epsilon, bounds=bounds, accountant=self.accountant)
            elif metric == "histogram":
                return diffprivlib.tools.histogram(values, epsilon=epsilon, bins=bins, range=range, accountant=self.accountant)
            else:
                raise TypeError('Invalid metric')
        else:
            values = query_local_clean.values
            bounds = bounds if bounds is not None else (np.nanmin(values), np.nanmax(values)) # Workaround for diffprilib bug
            if metric == "sum":
                return diffprivlib.tools.nansum(values, epsilon=epsilon, bounds=bounds, axis=0, accountant=self.accountant)
            if metric == "mean":
                return diffprivlib.tools.nanmean(values, epsilon=epsilon, bounds=bounds, axis=0, accountant=self.accountant)
            elif metric == "std":
                return diffprivlib.tools.nanstd(values, epsilon=epsilon, bounds=bounds, accountant=self.accountant)
            elif metric == "percentile":
                values = values[~np.isnan(values)]
                return diffprivlib.tools.percentile(values, percent, epsilon=epsilon, bounds=bounds, accountant=self.accountant)
            else:
                raise TypeError('Invalid metric')
        
        
    def private_query_local(self, aggregation, metric, epsilon=1.0, bins=10, range=None, bounds=None, percent=None):
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
            bounds = bounds if bounds is not None else (np.nanmin(values), np.nanmax(values)) # Workaround for diffprilib bug
            if metric == "sum":
                return diffprivlib.tools.nansum(values, epsilon=epsilon, bounds=bounds, accountant=self.accountant)
            elif metric == "mean":
                return diffprivlib.tools.nanmean(values, epsilon=epsilon, bounds=bounds, accountant=self.accountant)
            elif metric == "std":
                return diffprivlib.tools.nanstd(values, epsilon=epsilon, bounds=bounds, accountant=self.accountant)
            elif metric == "percentile":
                values = values[~np.isnan(values)]
                return diffprivlib.tools.percentile(values, percent, epsilon=epsilon, bounds=bounds, accountant=self.accountant)
            elif metric == "histogram":
                return diffprivlib.tools.histogram(values, epsilon=epsilon, bins=bins, range=range, accountant=self.accountant)
            else:
                raise TypeError('Invalid metric')
        else:
            values = query.values
            bounds = bounds if bounds is not None else (np.nanmin(values), np.nanmax(values)) # Workaround for diffprilib bug
            if metric == "sum":
                return diffprivlib.tools.nansum(values, epsilon=epsilon, bounds=bounds, axis=0, accountant=self.accountant)
            if metric == "mean":
                return diffprivlib.tools.nanmean(values, epsilon=epsilon, bounds=bounds, axis=0, accountant=self.accountant)
            elif metric == "std":
                return diffprivlib.tools.nanstd(values, epsilon=epsilon, bounds=bounds, accountant=self.accountant)
            elif metric == "percentile":
                values = values[~np.isnan(values)]
                return diffprivlib.tools.percentile(values, percent, epsilon=epsilon, bounds=bounds, accountant=self.accountant)
            else:
                raise TypeError('Invalid metric')
        
    def private_query(self, aggregation, metric, epsilon=1.0, bins=10, range=None, bounds=None, percent=None):
        """
        
        :meta private:
        """
        if self.engine=="spark":
            return self.private_query_spark(aggregation=aggregation, metric=metric, epsilon=epsilon,
                                            bins=bins, range=range, bounds=bounds, percent=percent)
        elif self.engine=="local":
            return self.private_query_local(aggregation=aggregation, metric=metric, epsilon=epsilon,
                                            bins=bins, range=range, bounds=bounds, percent=percent)
        
    def volume_on_ip(self, ip, volume_direction="ingoing", count_flows=False, epsilon=1.0):
        """
        Obtain the traffic volume to/from a specific external IP address

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
        Obtain the traffic volume to/from a specific Autonomous System

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
        Obtain the traffic volume to/from a specific domain

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
    
    def volume_on_domain_pattern(self, pattern, volume_direction="ingoing", count_flows=False, epsilon=1.0):
        """
        Obtain the traffic volume to/from a specific domain pattern.
        The function searches for flows to a domain that matches the given pattern.
        Pattern are defined in the SQL style, thus for example the ``%`` character represents any string of zero or more characters,
        while ``_`` represents any single character. 
        This function is useful to obtain the traffic volume by second level domain, e.g., ``%.googlevideo.com``

        :param pattern: The domain pattern (in SQL syntax) to query
        :type pattern: str
        
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
            raise RuntimeError("Must run volume_on_domain_pattern on tstat data")
            
        if not isinstance(pattern, str):
            raise TypeError('pattern must be a string')
            
        volume_col = "s_bytes_all" if self.direction != volume_direction else "c_bytes_all"

        if count_flows:
            volume_col=1
            
        return int(self.private_query(aggregation = f"sum(CASE WHEN c_tls_SNI LIKE '{pattern}' THEN {volume_col} ELSE 0 END)", \
                                  metric="sum", epsilon = epsilon))

    
    def volume_historam(self, volume_direction="ingoing", count_flows=False, bins=10, range=None, epsilon=1.0):
        """
        Compute a histogram of the per-user traffic volume

        :param volume_direction: Whether to compute ingress (``"ingoing"``) or egress (``"outgoing"``) volume, in bytes.
                                 Default: ``"ingoing"``
        :type volume_direction: str

        :param count_flows: Count the number of flows instead of volume. If set, ``"volume_direction"`` is ignored.
                            Default: ``False``
        :type count_flows: bool

        :param bins: Number of bins of the histogram.
                            Default: ``10``
        :type bins: int

        :param range: The lower and upper range of the bins. 
                            Default: ``None``
        :type range: (float,float)
        
        :param epsilon: The privacy budget to allocate for the query. Default: ``1.0``
        :type epsilon: float
        
        :return: A tuple ``(histo, bin_edges)``, where ``histo`` is the histogram and  ``bin_edges`` the boundaries
        :rtype: tuple
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
            
        return self.private_query(aggregation = f"sum( {volume_col} )", \
                                  metric="histogram", bins=bins, range=range, epsilon = epsilon)
    
    def volume_historam_specific(self, volume_direction="ingoing", count_flows=False, bins=10, range=None, 
                                 ip=None, asn=None, domain=None, epsilon=1.0):
        """
        Compute a histogram of the per-user traffic volume on given server IP, domain or ASN.
        The three filters are considered together (i.e., they form an AND clause).

        :param volume_direction: Whether to compute ingress (``"ingoing"``) or egress (``"outgoing"``) volume, in bytes.
                                 Default: ``"ingoing"``
        :type volume_direction: str

        :param count_flows: Count the number of flows instead of volume. If set, ``"volume_direction"`` is ignored.
                            Default: ``False``
        :type count_flows: bool

        :param bins: Number of bins of the histogram.
                            Default: ``10``
        :type bins: int

        :param range: The lower and upper range of the bins. 
                            Default: ``None``
        :type range: (float,float)
        
        :param ip: The server IP to filter. 
                            Default: ``None``
        :type ip: str
        
        :param asn: The server ASN to filter. 
                            Default: ``None``
        :type asn: int
        
        :param domain: The server domain to filter. 
                            Default: ``None``
        :type domain: str
        
        :param epsilon: The privacy budget to allocate for the query. Default: ``1.0``
        :type epsilon: float
        
        :return: A tuple ``(histo, bin_edges)``, where ``histo`` is the histogram and  ``bin_edges`` the boundaries
        :rtype: tuple
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
            
        if asn is not None and not self.ipasn_db:
            raise RuntimeError("Must provide ipasn_db to use this function")
            
        if domain is not None and not self.data_format=="tstat":
            raise RuntimeError("Must run on tstat data")
            
        condition_list = []
        if domain is not None:
            condition_list.append( f"c_tls_SNI == '{domain}'")
        if ip is not None:
            condition_list.append( f"{self.ipasn_col} == '{ip}'")
        if asn is not None:
            condition_list.append( f"asn == '{asn}'")

        if len(condition_list) > 0:
            condition = " AND ".join(condition_list)
        else:
            condition = "TRUE"
        
        return self.private_query(aggregation = f"sum( CASE WHEN {condition} THEN {volume_col} ELSE 0 END)", \
                                  metric="histogram", bins=bins, range=range, epsilon = epsilon)
    
    
    def flow_feature(self, feature, metric, ip=None, asn=None, domain=None, epsilon=1.0, percent=None, bins=10, range=None):
        """
        Extract statistics on a flow feature (i.e., a log's column).
        Notice that this can be used only with Tstat data and on a subset of columns.
        Notice that DPMon first computes the average per-user value of the flow features.
        Then it applies the requested statistic (mean, standard deviation or percentile) among the average per-user value.
        This behavior is mandatory as Differential Privacy must protect users (not flows).
        You can filter by server IP, ASN or Domain.
        The three filters are considered together (i.e., they form an AND clause).

        :param feature: The flow feature to compute statistics on.
        :type feature: str

        :param metric: The metric to compute. Can be ``mean``, ``std``, ``histogram`` or ``percentile``
        :type metric: str

        :param percent: The percentile to compute in case ``metric==percentile``
        :type percent: float

        :param ip: The server IP to filter. 
                            Default: ``None``
        :type ip: str
        
        :param asn: The server ASN to filter. 
                            Default: ``None``
        :type asn: int
        
        :param domain: The server domain to filter. 
                            Default: ``None``
        :type domain: str
        
        :param epsilon: The privacy budget to allocate for the query. Default: ``1.0``
        :type epsilon: float
        
        :param bins: Number of bins of the histogram in case ``metric==histogram``. .
                            Default: ``10``
        :type bins: int

        :param range: The lower and upper range of the bins of the histogram in case ``metric==histogram``. 
                            Default: ``None``
        :type range: (float,float)
    
        :return: The desired statistic
        """
          
        if asn is not None and not self.ipasn_db:
            raise RuntimeError("Must provide ipasn_db to use this function")
            
        if not self.data_format=="tstat":
            raise RuntimeError("Must run on tstat data")

        if not metric in {"mean", "std", "percentile", "histogram"}:
            raise RuntimeError("metric must be: 'mean', 'std' or 'percentile'")
        if metric == "percentile" and percent is None:
            raise RuntimeError("must specify percent when metric=percentile")            
            
        if not feature in TSTAT_ALLOWED_FEATURES:
            raise RuntimeError("feature not allowed")        
        
        condition_list = []
        if domain is not None:
            condition_list.append( f"c_tls_SNI == '{domain}'")
        if ip is not None:
            condition_list.append( f"{self.ipasn_col} == '{ip}'")
        if asn is not None:
            condition_list.append( f"asn == '{asn}'")
            
        if len(condition_list) > 0:
            condition = " AND ".join(condition_list)
        else:
            condition = "TRUE"
            
        return self.private_query(aggregation = f"avg( CASE WHEN {condition} THEN {feature} ELSE NULL END)", \
                                  metric=metric, epsilon = epsilon, percent = percent, bins=bins, range=range)
                                  
                                  
    def user_count_specific(self, ip=None, asn=None, domain=None, epsilon=1.0):
        """
        Compute a the number of users on given server IP, domain or ASN.
        In other words, it computes the number of users who, at least once, issued a flow to the an enpoint with the given characteristics.
        The three filters are considered together (i.e., they form an AND clause).
        It returns the number of user matching and the number of user non matching the filter.

        :param ip: The server IP to filter. 
                            Default: ``None``
        :type ip: str
        
        :param asn: The server ASN to filter. 
                            Default: ``None``
        :type asn: int
        
        :param domain: The server domain to filter. 
                            Default: ``None``
        :type domain: str
        
        :param epsilon: The privacy budget to allocate for the query. Default: ``1.0``
        :type epsilon: float
        
        :return: A tuple ``(a, b)``, where ``a`` is the number of users who never matched the filter and  ``n`` the number of users who did at least once.
        :rtype: tuple
        """
        
            
        if asn is not None and not self.ipasn_db:
            raise RuntimeError("Must provide ipasn_db to use this function")
            
        if domain is not None and not self.data_format=="tstat":
            raise RuntimeError("Must run on tstat data")
            
        condition_list = []
        if domain is not None:
            condition_list.append( f"c_tls_SNI == '{domain}'")
        if ip is not None:
            condition_list.append( f"{self.ipasn_col} == '{ip}'")
        if asn is not None:
            condition_list.append( f"asn == '{asn}'")

        if len(condition_list) > 0:
            condition = " AND ".join(condition_list)
        else:
            condition = "TRUE"
        
        return self.private_query(aggregation = f"max( CASE WHEN {condition} THEN 1 ELSE 0 END)", \
                                  metric="histogram", bins=2, epsilon = epsilon, range=[0,1] )[0] 