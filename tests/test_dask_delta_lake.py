import dask.dataframe as dd
import os 

os.environ['HADOOP_USER_NAME'] = 'hadoopuser'


df = dd.read_parquet(
    'hdfs://localhost:9000/user/hadoopuser/landingzone/tb_lineitem/',
    engine='pyarrow'
)
print(df.head())