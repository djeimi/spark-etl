# Spark ETL with PostgreSQL

Этот репозиторий содержит скрипт ETL на Apache Spark, который работает с базой данных PostgreSQL.

## Как запустить

### Предварительные условия

- Убедитесь, что у вас установлен Docker Desktop и DBeaver Community.

### Запуск скриптов

1. Клонируйте репозиторий:
   ```sh
   git clone https://github.com/djeimi/spark-etl.git
   cd spark-etl
2. Запуск контейнера:
   docker run --name postgres_container -e POSTGRES_PASSWORD=mysecretpassword -d postgres
3. Запустить DBeaver и подключиться к PostgreSQL
4. Создание пользовательской сети Docker, чтобы контейнеры могли взаимодействовать друг с другом.
   Подключение существующего контейнера PostgreSQL к созданной сети:
   docker network create spark-network
   docker network connect spark-network postgres_container
5. Запуск контейнера Apache Spark и подключение его к той же сети:
   docker run --name spark-master --network spark-network -p 8080:8080 -p 7077:7077 -e SPARK_MASTER_HOST=spark-master bde2020/spark-master:3.0.1-hadoop3.2
6. Запуск контейнера Spark Worker и подключение его к той же сети (Проверка запуска Spark Worker - откройте веб-интерфейс Spark Master по адресу http://localhost:8080. Там вы должны увидеть информацию о подключенных Worker-ах.):
   docker run --name spark-worker --network spark-network -e SPARK_MASTER=spark://spark-master:7077 bde2020/spark-worker:3.0.1-hadoop3.2
7. Запуск любого скрипта:
   docker cp *путь к локальному файлу* spark-master:/*название файла, которое должно быть у скопированного файла*
   docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --jars /opt/spark/jars/postgresql-42.7.4.jar /*название файла, которое должно быть у скопированного файла*
8. Остановка контейнеров:
   docker stop spark-master spark-worker postgres_container
   docker rm spark-master spark-worker postgres_container
   docker network rm spark-network

