from kafka_reader import read_kafka
import daemon
import sys


if __name__ == '__main__':
    read_kafka()
    if len(sys.argv) == 2:
        if sys.argv[1] == 'service':
            with daemon.DaemonContext(working_directory='./', ):
                read_kafka()
        elif sys.argv[1] == 'cli':
            read_kafka()
        else:
            print('Uknown command')
            sys.exit(2)
    else:
        print('Usage %s service|cli' % sys.argv[0])
        print('Service will start service in background')
        print('cli will start the code directly')
