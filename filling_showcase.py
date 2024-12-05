# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, sum as spark_sum, datediff, date_format, expr, lit
from datetime import datetime
from pyspark.sql.types import DateType

# Создание Spark сессии
spark = SparkSession.builder \
    .appName("DWH ETL") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/postgresql-42.7.4.jar") \
    .config("spark.executor.extraClassPath", "/opt/spark/jars/postgresql-42.7.4.jar") \
    .getOrCreate()

# Параметры подключения к PostgreSQL (DWH)
jdbc_url_dwh = "jdbc:postgresql://postgres_container:5432/postgres"
connection_properties_dwh = {
    "user": "postgres",
    "password": "mysecretpassword",
    "driver": "org.postgresql.Driver"
}

# Чтение данных из DWH
try:
    craftsman_data = spark.read.jdbc(url=jdbc_url_dwh, table="dwh.d_craftsmans", properties=connection_properties_dwh)
    order_data = spark.read.jdbc(url=jdbc_url_dwh, table="dwh.f_orders", properties=connection_properties_dwh)
    product_data = spark.read.jdbc(url=jdbc_url_dwh, table="dwh.d_products", properties=connection_properties_dwh)
    customer_data = spark.read.jdbc(url=jdbc_url_dwh, table="dwh.d_customers", properties=connection_properties_dwh)
    print("Successfully read data from DWH")
except Exception as e:
    print("Error reading from DWH: {}".format(e))
    spark.stop()
    exit(1)

# Чтение последней даты загрузки из DWH
try:
    last_load_date_df = spark.read.jdbc(url=jdbc_url_dwh, table="dwh.load_dates_craftsman_report_datamart", properties=connection_properties_dwh)
    if last_load_date_df.count() == 0:
        # Assign the specific date if no data is present
        last_load_date = '2024-12-03'
    else:
        last_load_date = last_load_date_df.select("load_dttm").collect()[-1][0]
    print("Last load date: {}".format(last_load_date))
except Exception as e:
    print("Error reading last load date: {}".format(e))
    spark.stop()
    exit(1)

# Фильтрация данных, измененных с момента последней загрузки
incremental_order_data = order_data.filter(order_data.load_dttm > last_load_date)

# Объединение данных
joined_data = incremental_order_data.join(craftsman_data, incremental_order_data.craftsman_id == craftsman_data.craftsman_id) \
    .join(product_data, incremental_order_data.product_id == product_data.product_id) \
    .join(customer_data, incremental_order_data.customer_id == customer_data.customer_id)

# Вычисление необходимых метрик
craftsman_report = joined_data.groupBy(
    craftsman_data.craftsman_id, craftsman_data.craftsman_name, craftsman_data.craftsman_address,
    craftsman_data.craftsman_birthday, craftsman_data.craftsman_email
).agg(
    count("order_id").alias("count_order"),
    avg("product_price").alias("avg_price_order"),
    spark_sum("product_price").alias("craftsman_money"),
    (spark_sum("product_price") * 0.1).alias("platform_money"),
    avg(datediff(lit(datetime.now().strftime("%Y-%m-%d")), col("customer_birthday")) / 365.25).alias("avg_age_customer"),
    expr("percentile_approx(datediff(order_completion_date, order_created_date), 0.5)").alias("median_time_order_completed"),
    expr("first(product_type)").alias("top_product_category"),
    count(expr("case when order_status = 'created' then 1 end")).alias("count_order_created"),
    count(expr("case when order_status = 'in progress' then 1 end")).alias("count_order_in_progress"),
    count(expr("case when order_status = 'delivery' then 1 end")).alias("count_order_delivery"),
    count(expr("case when order_status = 'done' then 1 end")).alias("count_order_done"),
    count(expr("case when order_status != 'done' then 1 end")).alias("count_order_not_done")
)

# Добавление столбца report_period
craftsman_report = craftsman_report.withColumn("report_period", date_format(lit(datetime.now()), "yyyy-MM"))

# Запись данных в витрину данных
try:
    craftsman_report.write.jdbc(url=jdbc_url_dwh, table="dwh.craftsman_report_datamart", mode="append", properties=connection_properties_dwh)
    print("Successfully wrote data to craftsman_report_datamart")
except Exception as e:
    print("Error writing to craftsman_report_datamart: {}".format(e))
    spark.stop()
    exit(1)

# Обновление даты последней загрузки
try:
    current_load_date_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    current_load_date_df = spark.createDataFrame([(current_load_date_str,)], ["load_dttm"])
    # Cast the load_dttm column to DateType
    current_load_date_df = current_load_date_df.withColumn("load_dttm", col("load_dttm").cast(DateType()))
    current_load_date_df.write.jdbc(url=jdbc_url_dwh, table="dwh.load_dates_craftsman_report_datamart", mode="append", properties=connection_properties_dwh)
    print("Successfully updated load date")
except Exception as e:
    print("Error updating load date: {}".format(e))
    spark.stop()
    exit(1)

# Остановка Spark сессии
spark.stop()
