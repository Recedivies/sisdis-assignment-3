import logging
import multiprocessing
import random
import sys
import time
from argparse import ArgumentParser
import node

# RUN IN PYTHON 3.8.8

list_nodes = []

logging.basicConfig(format='%(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                    datefmt='%Y-%m-%d:%H:%M:%S',
                    level=logging.DEBUG)
logger = logging.getLogger(__name__)

class NodeProcess(multiprocessing.Process):

    def run(self):
        try:
            super().run()
        except Exception:
            logger.error(f"{self.name} has an error")


def reload_logging_config_node(filename):
    from importlib import reload
    reload(logging)
    logging.basicConfig(format='%(asctime)-4s %(levelname)-6s %(threadName)s:%(lineno)-3d %(message)s',
                        datefmt='%H:%M:%S',
                        filename=f"logs/{filename}",
                        filemode='w',
                        level=logging.DEBUG)

def handle_exception(exc_type, exc_value, exc_traceback):
    logger.error(f"Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))


def manual_event_input(args, starting_port, port_used, number_of_nodes):
    logger.info("Give input to processes...")
    time.sleep(3)
    while True:
        logger.info("Ask for input...")
        time.sleep(1)
        input_value = input("Give input here: ")
        logger.debug(f"input_value: {input_value}")
        logger.info("Process the input...")
        if "k" in input_value:
            logger.debug("Kill node input is working...")
            input_value = int(input_value[1:])
            list_nodes[input_value - 1].kill()
            logger.info(f"Kill node {input_value}...")
        elif "r" in input_value:
            logger.info("Restart node input is working...")
            node_id = int(input_value[1:])
            process = NodeProcess(target=node.main, args=(
                float(args.heartbeat),
                float(args.fault_duration),
                starting_port + node_id - 1,
                node_id, port_used,
                True
            ))
            list_nodes[node_id - 1] = process
            process.start()
            logger.info(f"Node {node_id} has running...")
        elif "e" in input_value:
            logger.info("Stop all nodes...")
            for node_id in range(number_of_nodes):
                process = list_nodes.pop()
                process.kill()
                logger.debug(f"Kill process with ID: {process.name}")
            logger.info("Done stopping all the nodes...")


def main():
    parser = ArgumentParser()
    parser.add_argument("-n", type=str, dest="node",
                        help="The number of nodes", default="4")
    parser.add_argument("-b", type=str, dest="heartbeat",
                        help="The particular duration of the heartbeat", default=1)
    parser.add_argument("-f", type=str, dest="fault_duration",
                        help="The particular duration to assume a leader node to be a fault", default=1.5)
    parser.add_argument("-p", type=str, dest="port",
                        help="Starting port", default=6574)
    args = parser.parse_args()

    sys.excepthook = handle_exception

    logger.info("The main program is running...")
    logger.info("Determining the ports that will be used...")
    starting_port = random.randint(10000, 11000)
    number_of_nodes = int(args.node)
    port_used = [port for port in range(starting_port, starting_port + number_of_nodes)]
    logger.debug(f"port_used: {port_used}")
    logger.info("Done determining the ports that will be used...")

    logger.debug(f"number_of_nodes: {number_of_nodes}")
    logger.debug(f"heartbeat: {float(args.heartbeat)}")

    logger.info("Start running multiple nodes...")
    for node_id in range(number_of_nodes):
        logger.info(f"Run node {node_id+1}...")
        reload_logging_config_node(f"node{node_id + 1}.txt")
        process = NodeProcess(target=node.main, args=(
            float(args.heartbeat),
            float(args.fault_duration),
            starting_port + node_id,
            node_id + 1, port_used
        ))
        process.start()
        list_nodes.append(process)
    logger.info("Done running multiple nodes...")
    logger.debug(f"number of running processes: {len(list_nodes)}")


    logger.info("Execute manual_event_input()...")
    manual_event_input(args, starting_port, port_used, number_of_nodes)



if __name__ == '__main__':
    main()