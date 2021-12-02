# kafka_test
Test project to fetch and store some data from Kafka

# How to use
Before running you must fill the config file (there should be empty one in src folder)
All the code needed is in src folder
To run the service simply run python main.py service
This will run the kafka reader as a linux service on background
If you wish to run the service in non service mode, use python main.py cli