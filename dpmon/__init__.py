import diffprivlib
import pandas as pd
import numpy as np

FORMATS=["tstat"]
TSTAT_BUILTIN_FILTER = "c_isint == 1"

class DPMon():
    
    def __init__(self, path, data_format, accountant, engine = "local", spark=None):
        self.path = path
        self.data_format = data_format
        self.engine = engine
        self.spark = spark
        self.accountant = accountant
        
        if not isinstance(path, str):
            raise TypeError('path must be a string')
            
        if not isinstance(data_format, str):
            raise TypeError('data_format must be a string')
        if not data_format in FORMATS:
            raise TypeError('data_format must be one of: ' + FORMATS.join(",") )

        if not isinstance(engine, str):
            raise TypeError('engine must be a string')
        if not engine in ["spark", "local"]:
            raise TypeError('engine must be one of: ' + ["spark", "local"].join(",") )
        
        if engine == "spark" and spark==None:
            raise TypeError('Must provide the spark argument when engine is spark')
        
        if not isinstance(accountant, diffprivlib.accountant.BudgetAccountant):
            raise TypeError('accountant must be a instance of diffprivlib.accountant.BudgetAccountant')
            
        if data_format=="tstat":
            self.prepare_tstat()
            
    def prepare_tstat(self):
        self.df = self.spark.read.csv(self.path, header = True, inferSchema=True, sep=' ')
        self.df = self.df.toDF ( *[ c.split("#")[-1].split(":")[0] for c in self.df.columns] )
        
    def private_query(self, aggregation, metric, epsilon=1.0, bins=10, range=None, bounds=None, ):

        filtered = self.df.filter(TSTAT_BUILTIN_FILTER)
        filtered.createOrReplaceTempView("filtered")
        query = self.spark.sql(""" SELECT c_ip, {}
                              FROM filtered 
                              GROUP BY c_ip
                              """.format(aggregation))
        query_local = query.toPandas()
        query_local_clean = query_local.drop(columns="c_ip")
        if len(query_local_clean.columns) == 1:
            values = query_local_clean.iloc[:,0].values
            if metric == "sum":
                return diffprivlib.tools.sum(values, epsilon=epsilon, bounds=bounds, accountant=self.accountant)
            elif metric == "mean":
                return diffprivlib.tools.mean(values, epsilon=epsilon, bounds=bounds, accountant=self.accountant)
            elif metric == "histogram":
                return diffprivlib.tools.histogram(values, epsilon=epsilon, bins=bins, range=range, accountant=self.accountant)
        else:
            values = query_local_clean.values
            if metric == "sum":
                return diffprivlib.tools.sum(values, epsilon=epsilon, bounds=bounds, axis=0, accountant=self.accountant)
            if metric == "mean":
                return diffprivlib.tools.mean(values, epsilon=epsilon, bounds=bounds, axis=0, accountant=self.accountant)
            return None