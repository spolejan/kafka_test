from kafka import KafkaConsumer, TopicPartition, KafkaProducer
from config_loader import load_config
import json
from db_worker import DataWorker


def read_kafka():
    config = load_config('KAFKA')
    consumer = KafkaConsumer(bootstrap_servers=config['server'],
                             security_protocol=config['security_protocol'],
                             ssl_check_hostname=True,
                             group_id=None,
                             auto_offset_reset='earliest',
                             enable_auto_commit=False,
                             sasl_mechanism=config['sasl_mechanism'],
                             sasl_plain_username=config['username'],
                             sasl_plain_password=config['password'],
                             value_deserializer=json_deserializer)
    topic_partition = TopicPartition(config['topic'], 0)
    assigned_topic = [topic_partition]
    consumer.assign(assigned_topic)
    # consumer.poll()
    # consumer.seek_to_end(topic_partition)

    consumer.seek_to_beginning()
    for message in consumer:
        consume_value(message.value)

    consumer.close()


def json_deserializer(v):
    if v is None:
        return None
    else:
        try:
            return json.loads(v.decode('utf-8'))
        except json.decoder.JSONDecodeError:
            print('Unable to decode: %s', v)
            return None


def consume_value(value):
    worker = DataWorker()
    if value is None:
        return
    if value['metadata']['type'] == 'offer':
        worker.save_offer(value['payload'])
        return
    if value['metadata']['type'] == 'category':
        worker.save_category(value['payload'])
        return


if __name__ == '__main__':
    read_kafka()
    # initialize_db()
