# %%
pip --version

# %%
import sys   
print('python version:', sys.version)

# %%
print(sys.executable)

# %%
!python --version

# %%
!where python

# %%
!java --version

# %%
!where java

# %%
os.environ.get("JAVA_HOME")

# %%
from pyspark.sql import SparkSession 
spark = SparkSession.builder.master("local").appName("test2").getOrCreate()

print(spark.version)

# %%
print(os.environ.get("SPARK_HOME"))

# %%
import pyspark 

print(pyspark.__version__)

# %%
print(os.environ.get("HADOOP_HOME"))

# %%
import os 
print(os.getcwd())
print(os.chdir("H://pyspark_SQL_python_advanced-coding_interview//CTE"))

# %%
for k,v in os.environ.items():
    print(f"{k}: {v}")

# %%
print(os.environ.get("PATH"))

# %%
!pip list

# %%
print(spark.sparkContext.appName)
print(spark.sparkContext.master)

# %%



