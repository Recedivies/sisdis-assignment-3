import logging
import threading


def thread_exception_handler(args):
    logging.error(f"Uncaught exception", exc_info=(args.exc_type, args.exc_value, args.exc_traceback))


class Raft:

    def __init__(self, node_id: int, port: int, neighbors_ports: list, lb_fault_duration: int, is_continue: bool,
                 heartbeat_duration: float):
        self.heartbeat_duration = heartbeat_duration
        self.is_continue = is_continue
        self.lb_fault_duration = lb_fault_duration
        self.neighbors_ports = neighbors_ports
        self.port = port
        self.node_id = node_id

    def start(self):
        # TODO
        pass


def reload_logging_windows(filename):
    log = logging.getLogger()
    for handler in log.handlers:
        log.removeHandler(handler)
    logging.basicConfig(format='%(asctime)-4s %(levelname)-6s %(threadName)s:%(lineno)-3d %(message)s',
                        datefmt='%H:%M:%S',
                        filename=filename,
                        filemode='w',
                        level=logging.INFO)

def main(heartbeat_duration=1, lb_fault_duration=1, port=1000,
         node_id=1, neighbors_ports=(1000,), is_continue=False):
    reload_logging_windows(f"logs/node{node_id}.txt")
    threading.excepthook = thread_exception_handler
    try:
        logging.info(f"Node with id {node_id} is running...")
        logging.debug(f"heartbeat_duration: {heartbeat_duration}")
        logging.debug(f"lower_bound_fault_duration: {lb_fault_duration}")
        logging.debug(f"upper_bound_fault_duration = {lb_fault_duration}s + 4s")
        logging.debug(f"port: {port}")
        logging.debug(f"neighbors_ports: {neighbors_ports}")
        logging.debug(f"is_continue: {is_continue}")

        logging.info("Create raft object...")
        raft = Raft(node_id, port, neighbors_ports, lb_fault_duration, is_continue, heartbeat_duration)

        logging.info("Execute raft.start()...")
        raft.start()
    except Exception:
        logging.exception("Caught Error")
        raise
