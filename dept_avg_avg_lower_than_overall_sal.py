# %%
import os 
print(os.getcwd())

# %%


# %%
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("CreateTable").getOrCreate()

# Create the data for the table
data = [
    (1, "Ankit", 100, 10000),
    (2, "Mohit", 100, 15000),
    (3, "Vikas", 100, 10000),
    (4, "Rohit", 100, 5000),
    (5, "Mudit", 200, 12000),
    (6, "Agam", 200, 12000),
    (7, "Sanjay", 200, 9000),
    (8, "Ashish", 200, 5000),
    (9, "Mukesh", 300, 6000),
    (10, "Rakesh", 300, 7000)
]

# Define the schema
columns = ["emp_id", "emp_name", "department_id", "salary"]

# Create a DataFrame
df = spark.createDataFrame(data, schema=columns)

# Show the DataFrame
df.show()

# Register the DataFrame as a temporary table
df.createOrReplaceTempView("employee")


# %%
df.show()

# %%
spark.sql(""" select * from employee""").show()

# %%
df_query = spark.sql("""  
-- Calculate department-wise average salary                 
WITH DeptAvg AS (    
    SELECT 
        department_id, 
        AVG(salary) AS dep_avg_salary
    FROM 
        employee 
    GROUP BY  
        department_id
),

-- Calculate company-wide average salary excluding the current department 
CompanyAvg AS (
    SELECT 
        department_id,
        AVG(salary) OVER (PARTITION BY department_id) AS dep_avg_salary,
        AVG(salary) OVER () - AVG(salary) OVER (PARTITION BY department_id) AS company_avg_salary_excluding_dept
    FROM 
        employee
)

-- Join the two subqueries and filter 
SELECT 
    DeptAvg.department_id, 
    DeptAvg.dep_avg_salary, 
    CompanyAvg.company_avg_salary_excluding_dept
FROM 
    DeptAvg 
JOIN 
    CompanyAvg 
ON 
    DeptAvg.department_id = CompanyAvg.department_id
    WHERE DeptAvg.dep_avg_salary < CompanyAvg.company_avg_salary_excluding_dept

""")

df_query.show()


# %% [markdown]
# ##  Pyspark

# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

# Create a SparkSession
spark = SparkSession.builder \
    .appName("PySpark Query Example") \
    .getOrCreate()

# Sample Data
data = [
    (1, "Ankit", 100, 10000),
    (2, "Mohit", 100, 15000),
    (3, "Vikas", 100, 10000),
    (4, "Rohit", 100, 5000),
    (5, "Mudit", 200, 12000),
    (6, "Agam", 200, 12000),
    (7, "Sanjay", 200, 9000),
    (8, "Ashish", 200, 5000),
    (9, "Mukesh", 300, 6000),
    (10, "Rakesh", 300, 7000),
]

columns = ["emp_id", "emp_name", "department_id", "salary"]

# Create DataFrame
employee_df = spark.createDataFrame(data, columns)

# Show the DataFrame
employee_df.show()


# %%
# department wise average salary 

dept_avg_salary_df = employee_df.groupBy("department_id") \
    .agg(avg("salary").alias("dep_avg_salary"))

dept_avg_salary_df.show()

                            

# %%
# Overall average salary for the entire company
overall_avg_salary = employee_df.select(avg("salary").alias("overall_avg_salary")).collect()[0][0]

# Exclude each department's salary from the overall calculation dynamically
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as sum_, count, when

# Calculate the sum and count for each department
dept_sum_count = employee_df.groupBy("department_id") \
    .agg(sum_("salary").alias("dept_sum_salary"),
         count("salary").alias("dept_count"))

# Calculate company-wide average excluding the current department
company_avg_excl_dept = dept_sum_count.withColumn(
    "company_avg_salary_excluding_dept",
    (overall_avg_salary * employee_df.count() - col("dept_sum_salary")) /
    (employee_df.count() - col("dept_count"))
)

company_avg_excl_dept.show()


# %%
# Join Department Averages with Company-Wide Exclusion Averages
result_df = dept_avg_salary_df.join(
    company_avg_excl_dept,
    on="department_id"
).filter(col("dep_avg_salary") < col("company_avg_salary_excluding_dept"))

# Select only relevant columns
final_result_df = result_df.select("department_id", "dep_avg_salary")
final_result_df.show()


# %%



