import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, sum
from datetime import datetime

project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

spark = SparkSession.builder \
        .master('local') \
        .config('spark.driver.extraClassPath',
                os.path.join(project_dir, 'libs/mysql-connector-java-8.0.11.jar')) \
        .appName('covid-etl').getOrCreate()


# Extract
case_df = spark.read.csv(os.path.join(project_dir, 'data/Indonesia_coronavirus_daily_data.csv'),
            header=True, inferSchema=True)

zone_df = spark.read.csv(os.path.join(project_dir, 'data/zone_reference.csv'),
                header=True, inferSchema=True)


# Transform
grouped_case_df = case_df.filter(
        to_date(col('Date'), 'dd/mm/yyyy') >= datetime.strptime('01-01-2021', '%d-%m-%Y')) \
        .groupby('Province').agg(sum('Daily_Case').alias('Total_Case'))

summary_df = grouped_case_df.join(zone_df,
                                (grouped_case_df['Total_Case'] >= zone_df['Min_Total_Case']) &
                                (grouped_case_df['Total_Case'] <= zone_df['Max_Total_Case']),
                                how='inner').select('Province', 'Total_Case', 'Zone')

summary_df.show()


# Load
partition_date = datetime.today().strftime("%Y%m%d")

summary_df.repartition(1).write.csv(os.path.join(project_dir, f"output/summary_by_province_{partition_date}"),
                                mode='overwrite',
                                header=True)
# .repartition(1) means that the output will be 1 file.
# You can change it with integer that >= 1 or even not use the .repartition(n)

db_conn : str = "jdbc:mysql://localhost:3306/data_eng_covid?useSSL=False&serverTimezone=Asia/Jakarta"
table_name : str = "summary_by_province"
properties : dict = {
        'user' : 'root',
        'password' : '',
        'driver' : 'com.mysql.cj.jdbc.Driver'
}

try:
    summary_df.repartition(1).write.mode("overwrite").jdbc(db_conn, table=table_name, mode='overwrite',
                                                           properties=properties)
    print('Data has been loaded to MySQL!')
except Exception as exp:
    print(f'ERROR : Failed to load data to MySQL : {exp}')
    raise
