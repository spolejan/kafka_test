from kafka_reader import read_kafka
import daemon


if __name__ == '__main__':
    with daemon.DaemonContext(working_directory='./', ):
        read_kafka()
