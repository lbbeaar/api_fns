import requests
import pandas as pd
import json

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("api_fns") \
    .config("spark.jars", "file:///C:/Users/admedvedeva/PycharmProjects/test/postgresql-42.3.1.jar") \
    .getOrCreate()


jdbcDF = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/cinema") \
    .option("dbtable", "inn_tbl") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .load()
inn_arr = [int(row.inn) for row in jdbcDF.select('inn').collect()]


def get_info(inn_mas):
    api_url = 'https://api-fns.ru/api/'
    api_method = 'egr'
    org_info = {}
    for i in inn_mas:
        params = {'req': i, 'key': '074bde7125a80f5daa558eb04bcdde6e5293ac18'}
        resp = requests.get(api_url + api_method, params=params)
        info_arr = json.loads(resp.text)['items'][0]['ЮЛ']
        org_info[info_arr['ИНН']] = info_arr['НаимСокрЮЛ']
    return org_info


if __name__ == '__main__':
    info = get_info(inn_arr)
    df = pd.DataFrame(info.items(), columns=["inn", "name"])
    spark_jdbcDF = spark.createDataFrame(df)
    spark_jdbcDF.write.option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .jdbc("jdbc:postgresql://localhost:5432/cinema", "inn_name",
              properties={"user": "postgres", "password": "postgres"})
