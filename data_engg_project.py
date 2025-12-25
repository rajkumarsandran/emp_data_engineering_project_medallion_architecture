# %%
from dotenv import load_dotenv
load_dotenv(override=True)

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()


# %%
spark.sql("create schema if not exists bronze")
spark.sql("create schema if not exists silver")
spark.sql("create schema if not exists gold")
spark.sql("""
create table if not exists bronze.processed_files (load_date date, bronze_table_name string, source_file_name string, status string) 
using delta
location 'file:/C:/Apps/spark-warehouse/bronze/processed_files'
""")
spark.sql("""
create table if not exists bronze.emp (empno integer,ename string,job string,mgr integer,hiredate date,sal double,comm double,deptno integer,source_file_name string,load_date date) 
using delta
location 'file:/C:/Apps/spark-warehouse/bronze/emp'
""")
spark.sql("""
create table if not exists silver.emp (emp_sk bigint,emp_hash string,empno integer,ename string,job string,mgr integer,hiredate date,sal double,comm double,deptno integer,load_date date, effective_start_date date, effective_end_date date, is_active boolean) 
using delta
location 'file:/C:/Apps/spark-warehouse/silver/emp'
""")
spark.sql("""
create table if not exists gold.dim_emp (empno integer,ename string,job string,mgr integer,hiredate date,sal double,comm double,deptno integer,emp_hash string,last_updated_date date) 
using delta
location 'file:/C:/Apps/spark-warehouse/gold/dim_emp'
""")
spark.sql("""
create table if not exists gold.fact_emp_headcount (deptno integer,total_emps long, as_of_date date) 
using delta
location 'file:/C:/Apps/spark-warehouse/gold/fact_emp_headcount'
""")
spark.sql("""
create table if not exists gold.fact_emp_salary (deptno integer,total_sal double, as_of_date date) 
using delta
location 'file:/C:/Apps/spark-warehouse/gold/fact_emp_salary'
""")



# %%
load_date = spark.range(1).select(f.date_add(f.current_date(),-2).alias('load_date')).collect()[0][0]
print(load_date)

# %% [markdown]
# ## Bronze

# %%
processed_files = spark.table('bronze.processed_files')

emp_schema = "empno integer,ename string,job string,mgr integer,hiredate date,sal double,comm double,deptno integer"

emp = spark.read.load(format='csv', path="emp", header=True, schema=emp_schema)

emp = emp.withColumn('source_file_name',f.input_file_name())

unprocessed_data = emp.alias('e').join(processed_files.alias('p'), 'source_file_name', 'left_anti')

bronze_emp = unprocessed_data.select(f.col('e.*')).withColumn('load_date',f.lit(load_date))

try:
    bronze_emp.write.saveAsTable(name='bronze.emp', format='delta', mode='append')
    status = 'PROCESSED'
except Exception as e:
    status = 'FAILED'

status_df = bronze_emp.select(f.col('source_file_name'), f.col('load_date')).distinct()\
    .withColumn('bronze_table_name',f.lit('emp')).withColumn('status', f.lit(status))

status_df.write.saveAsTable(name='bronze.processed_files', format='delta', mode='append')


# %% [markdown]
# ## Silver

# %%
bronze_emp = spark.table('bronze.emp')

target_silver_emp = spark.table('silver.emp')

max_sk = target_silver_emp.select(f.coalesce(f.max(f.col('emp_sk')),f.lit(0))).collect()[0][0]

hash_cols = ['empno','ename','job','mgr','hiredate','sal','comm','deptno']

required_silver_cols = ['empno','ename','job','mgr','hiredate','sal','comm','deptno','load_date']

source_silver_emp = bronze_emp.where(f.col('load_date')==load_date)\
    .withColumn('rnk', f.row_number().over(Window.partitionBy(f.col('empno')).orderBy(f.col('load_date').desc())))\
    .where(f.col('rnk')==1)\
    .select(*[f.col(c) for c in required_silver_cols])\
    .withColumn('effective_start_date',f.lit(load_date))\
    .withColumn('effective_end_date',f.lit(None).cast('date'))\
    .withColumn('is_active', f.lit(True))\
    .withColumn('emp_hash', f.sha2(f.concat_ws('||',*[f.coalesce(f.col(c).cast('string'), f.lit('NULL')) for c in hash_cols]), 256))


update_rows = source_silver_emp.alias('s')\
    .join(
        target_silver_emp.where(f.col('is_active')==True).alias('t'), 
        [f.col('s.empno')==f.col('t.empno'), f.col('s.emp_hash')!=f.col('t.emp_hash')],
        'left_semi'
    ).withColumn('action', f.lit('update')).withColumn('emp_sk', f.lit(None).cast('bigint'))

insert_rows = source_silver_emp.alias('s')\
    .join(
        target_silver_emp.where(f.col('is_active')==True).alias('t'), 
        'emp_hash',
        'left_anti'
    ).withColumn('action', f.lit('insert')).withColumn('emp_sk', f.row_number().over(Window.orderBy(f.col('empno')))+max_sk)

new_source_silver_emp = update_rows.unionByName(insert_rows)


DeltaTable.forName(spark,'silver.emp').alias('t').merge(
    source=new_source_silver_emp.alias('s'),
    condition="s.empno=t.empno and t.is_active=true and s.action='update'"
).whenMatchedUpdate(
    set={
        "effective_end_date": f.date_add(f.lit(load_date),-1),
        "is_active": "false"
    }
).whenNotMatchedInsert(
    values={
        "empno": "s.empno",
        "ename": "s.ename",
        "job": "s.job",
        "mgr": "s.mgr",
        "hiredate": "s.hiredate",
        "sal": "s.sal",
        "comm": "s.comm",
        "deptno": "s.deptno",
        "load_date": "s.load_date",
        "effective_start_date": "s.effective_start_date",
        "effective_end_date": "s.effective_end_date",
        "is_active": "s.is_active",
        "emp_sk": "s.emp_sk",
        "emp_hash": "s.emp_hash"
    }
).execute().show()


# %% [markdown]
# ## Gold

# %%
silver_emp = spark.table('silver.emp')

hash_cols = ['empno','ename','job','mgr','hiredate','sal','comm','deptno']

dim_emp_required_cols = ['empno','ename','job','mgr','hiredate','sal','comm','deptno','emp_hash','last_updated_date']

source_dim_emp = silver_emp.where((f.col('is_active')==True) & (f.col('load_date')==load_date))\
    .withColumn('last_updated_date',f.col('load_date'))\
    .withColumn('emp_hash', f.sha2(f.concat_ws('||',*[f.coalesce(f.col(c).cast('string'), f.lit('NULL')) for c in hash_cols]), 256))\
    .select(*[f.col(c) for c in dim_emp_required_cols])

DeltaTable.forName(spark,'gold.dim_emp').alias('t').merge(
    source=source_dim_emp.alias('s'),
    condition="s.empno=t.empno"
).whenMatchedUpdateAll(condition="s.emp_hash!=t.emp_hash").whenNotMatchedInsertAll().execute().show()


# %%

dim_emp = spark.table('gold.dim_emp')

fact_emp_headcount = dim_emp.groupBy(f.col('deptno')).agg(f.count('*').alias('total_emps'))\
    .withColumn('as_of_date',f.lit(load_date))

DeltaTable.forName(spark, 'gold.fact_emp_headcount').alias('t').merge(
    source=fact_emp_headcount.alias('s'),
    condition="s.as_of_date=t.as_of_date and s.deptno=t.deptno"
).whenMatchedUpdate(
    condition="s.total_emps<>t.total_emps",
    set={
        "total_emps": "s.total_emps"
    }
).whenNotMatchedInsertAll().execute().show()


# %%
dim_emp = spark.table('gold.dim_emp')

fact_emp_salary = dim_emp.groupBy(f.col('deptno')).agg(f.sum(f.col('sal')).alias('total_sal'))\
    .withColumn('as_of_date',f.lit(load_date))


DeltaTable.forName(spark, 'gold.fact_emp_salary').alias('t').merge(
    source=fact_emp_salary.alias('s'),
    condition="s.as_of_date=t.as_of_date and s.deptno=t.deptno"
).whenMatchedUpdate(
    condition="s.total_sal<>t.total_sal",
    set={
        "total_sal": "s.total_sal"
    }
).whenNotMatchedInsertAll().execute().show()



# %%
sql = """
select * from gold.fact_emp_salary

"""

spark.sql(sql).show(truncate=False)


