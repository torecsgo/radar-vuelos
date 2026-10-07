# radar vuelos

# Athlantic Ocean: 34.727404, 28.909987
# Russia Up North: 72.137679, 47.538162
# > 400 sq° or global = 4 credits
# 36 credits / h (864 credits / day)

# Open Docker container
# docker exec -it kafka bash

# CREATE TOPIC flights WITH (PARTITIONS=3, REPLICATION_FACTOR=1);
# /usr/bin/kafka-topics --create --topic flights --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# PRODUCER Topic flights
# /usr/bin/kafka-console-producer --bootstrap-server localhost:9092 --topic flights

# Consumer Topic flights
# /usr/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic flights --from-beginning

# List Topics
# /usr/bin/kafka-topics --list --bootstrap-server localhost:9092


db/init.sql

dashboard/app.py
dashboard/Dockerfile