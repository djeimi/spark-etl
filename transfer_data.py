# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

# Создание Spark сессии
spark = SparkSession.builder \
    .appName("DWH ETL") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/postgresql-42.7.4.jar") \
    .config("spark.executor.extraClassPath", "/opt/spark/jars/postgresql-42.7.4.jar") \
    .getOrCreate()

# Параметры подключения к PostgreSQL (источник данных)
jdbc_url_source = "jdbc:postgresql://postgres_container:5432/postgres"
connection_properties_source = {
    "user": "postgres",
    "password": "mysecretpassword",
    "driver": "org.postgresql.Driver"
}

# Параметры подключения к PostgreSQL (DWH)
jdbc_url_dwh = "jdbc:postgresql://postgres_container:5432/postgres"
connection_properties_dwh = {
    "user": "postgres",
    "password": "mysecretpassword",
    "driver": "org.postgresql.Driver"
}

# Чтение данных из источника
try:
    craft_market_wide = spark.read.jdbc(url=jdbc_url_source, table="source1.craft_market_wide", properties=connection_properties_source)
    print("Successfully read data from source")
except Exception as e:
    print("Error reading from source: {}".format(e))
    spark.stop()
    exit(1)

# Таблица измерений заказчиков
dim_customer = craft_market_wide.select("customer_id", "customer_name", "customer_address", "customer_birthday", "customer_email").distinct()
dim_customer = dim_customer.withColumn("load_dttm", current_timestamp())

# Таблица измерений мастеров
dim_craftsman = craft_market_wide.select("craftsman_id", "craftsman_name", "craftsman_address", "craftsman_birthday", "craftsman_email").distinct()
dim_craftsman = dim_craftsman.withColumn("load_dttm", current_timestamp())

# Таблица измерений товаров
dim_product = craft_market_wide.select("product_id", "product_name", "product_description", "product_type", "product_price").distinct()
dim_product = dim_product.withColumn("load_dttm", current_timestamp())

# Таблица фактов заказов
fact_order = craft_market_wide.select("order_id", "product_id", "craftsman_id", "customer_id", "order_created_date", "order_completion_date", "order_status")
fact_order = fact_order.withColumn("load_dttm", current_timestamp())

# Запись данных в DWH
try:
    dim_customer.write.jdbc(url=jdbc_url_dwh, table="dwh.d_customers", mode="append", properties=connection_properties_dwh)
    dim_craftsman.write.jdbc(url=jdbc_url_dwh, table="dwh.d_craftsmans", mode="append", properties=connection_properties_dwh)
    dim_product.write.jdbc(url=jdbc_url_dwh, table="dwh.d_products", mode="append", properties=connection_properties_dwh)
    fact_order.write.jdbc(url=jdbc_url_dwh, table="dwh.f_orders", mode="append", properties=connection_properties_dwh)
    print("Successfully wrote data to DWH")
except Exception as e:
    print("Error writing to DWH: {}".format(e))
    spark.stop()
    exit(1)

# Остановка Spark сессии
spark.stop()