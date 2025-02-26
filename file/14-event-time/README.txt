# event time example in SO: https://stackoverflow.com/questions/63367299/how-can-you-load-a-csv-into-pyflink-as-a-streaming-table-source

Create Confluent Kafka development cluster and load data into Kafak

1) Launch Confluent Kafka Docker Environment

In same directory as docker-compose.yaml file from course materials run the following.


docker-compose up -d


2) Create Kafka source topic named salesitems

docker exec -it broker kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --topic salesitems


3) Create Kafka sink topics named processedsales and processedsales2

docker exec -it broker kafka-topics --create \
    --bootstrap-server localhost:9092 --topic processedsales


4) Download the Kafka Connector Jar using HTTPie HTTP Shell Client

Run the following from the same directory as the eventtime_windows.py file downloaded from course materials


http --download https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka_2.11/1.13.0/flink-sql-connector-kafka_2.11-1.13.0.jar


5) Run sales_producer.py program

python sales_producer.py --bootstrap-server localhost:9092 --topic salesitems


6) In another terminal / cmd prompt (with the Python Virtual Environment activated) run the Flink program eventtime_windows.py

python eventtime_windows.py



7) Verify that the processedsales and processedsales2 topic has data using the kafka-console-consumer

docker exec -it broker kafka-console-consumer --from-beginning \
    --bootstrap-server localhost:9092 \
    --topic processedsales


8) Clean up docker based Kafka environment

Run the following from the same directory as docker-compose.yaml file downloaded from course materials

docker-compose down -v


