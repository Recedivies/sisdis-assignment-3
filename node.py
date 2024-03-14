import logging
import threading
import time
from enum import Enum
import random
import socket


class State(Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3


def thread_exception_handler(args):
    logging.error(
        f"Uncaught exception",
        exc_info=(args.exc_type, args.exc_value, args.exc_traceback),
    )


node_ports = {}


class Raft:
    nodes = 0

    def __init__(
        self,
        node_id: int,
        port: int,
        neighbors_ports: list,
        lb_fault_duration: int,
        ub_fault_duration: float,
        heartbeat_duration: float,
    ):
        logging.info("Initialise port and node dictionary...")
        self.heartbeat_duration = heartbeat_duration
        self.lb_fault_duration = lb_fault_duration
        self.ub_fault_duration = ub_fault_duration
        self.neighbors_ports = neighbors_ports
        self.port = port
        self.node_id = node_id
        Raft.nodes = len(neighbors_ports)

        logging.info("Initialise persistent variable...")
        self.current_term = 0
        self.voted_for = None

        logging.info("Initialise volatile state...")
        self.current_role: State = State.FOLLOWER
        self.current_leader = None
        self.votes_received = set()

        logging.info("Initialise flag variable...")
        self.is_stop_timer = False
        self.election_timer = None

        logging.info("Initialise lock variable...")
        self.lock = threading.Lock()

        logging.info("Initialise election timer thread...")
        self.timer_duration = None
        self.candidate_timer_thread = threading.Thread(target=self.start_election_timer)
        self.candidate_timer_thread.name = "start->election_timer"

    def randomize_timer(self) -> float:
        return round(random.uniform(self.lb_fault_duration, self.ub_fault_duration), 1)

    def ceil(self, p: int, q: int) -> int:
        return (p + q - 1) // q

    def send_message(self, message, port):
        tcp_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        message = message.encode("UTF-8")
        addr = ("127.0.0.1", port)

        try:
            tcp_client_socket.connect(addr)
            tcp_client_socket.sendall(message)
            # status_dictionary_node = tcp_client_socket.recv(1024).decode("UTF-8")
        except ConnectionRefusedError as e:
            logging.info(f"Node {node_ports[port]} refused connection...")

    def suspect_leader_fail_or_election_timeout(self):
        self.current_term += 1
        self.current_role = State.CANDIDATE
        self.voted_for = self.node_id
        self.votes_received.add(self.node_id)

        message = ("vote_request", self.node_id, self.current_term)
        thread = threading.Thread(target=self.start_election_timer)
        thread.name = "vote_request->election_timer"
        thread.start()

        for neighbor_port in self.neighbors_ports:
            neighbor_id = node_ports[neighbor_port]
            if neighbor_id == self.node_id:
                continue

            # thread = threading.Thread(
            #     target=self.send_message, args=(f"{message}", neighbor_port)
            # )
            # thread.name = f"sending_thread-node{neighbor_id}"
            # thread.start()
            self.send_message(f"{message}", neighbor_port)

    def receive_vote_request(self, c_id: int, c_term: int):
        self.is_stop_timer = True
        self.election_timer.cancel()
        self.start_election_timer()

        if c_term > self.current_term:
            logging.info(f"Candidate node {c_id} has higher term than my term")
            logging.info(f"Change term from {self.current_term} to {c_term}")
            logging.info(f"I vote for candidate node {c_id}")

            self.current_term = c_term
            self.current_role = State.FOLLOWER
            self.voted_for = None

        if c_term == self.current_term and self.voted_for in {c_id, None}:
            self.voted_for = c_id
            message = ("vote_response", self.node_id, self.current_term, True)
        else:
            message = ("vote_response", self.node_id, self.current_term, False)

        self.send_message(f"{message}", self.neighbors_ports[c_id - 1])

        logging.info(
            f"Connection for vote_procedure from candidate {c_id} has been closed..."
        )

    def receive_vote_response(self, voter_id: int, term: int, granted: bool):
        if (
            self.current_role == State.CANDIDATE
            and term == self.current_term
            and granted
        ):
            self.votes_received.add(voter_id)
            if len(self.votes_received) >= self.ceil(Raft.nodes + 1, 2):
                self.current_role = State.LEADER
                self.current_leader = self.node_id

                self.is_stop_timer = True
                self.election_timer.cancel()

        elif term > self.current_term:
            self.current_term = term
            self.current_role = State.FOLLOWER
            self.voted_for = None

            self.is_stop_timer = True
            self.election_timer.cancel()

    def send_heartbeat(self):
        while True:
            if self.current_role == State.LEADER:
                for follower_port in self.neighbors_ports:
                    follower_id = node_ports[follower_port]
                    if follower_id == self.node_id:
                        continue

                    logging.info(f"Send heartbeat to node {follower_id}...")
                    logging.info(
                        f"Send heartbeat message to node {follower_id} with port {follower_port}"
                    )
                    message = ("log_request", self.node_id, self.current_term)
                    self.send_message(f"{message}", follower_port)

            time.sleep(self.heartbeat_duration)

    def receive_log_request(self, leader_id, term):
        self.is_stop_timer = True
        self.election_timer.cancel()
        thread = threading.Thread(target=self.start_election_timer)
        thread.name = "receive_log->election_timer"
        thread.start()

        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self.current_leader = leader_id
            self.current_role = State.FOLLOWER

        if term == self.current_term and self.current_role == State.CANDIDATE:
            self.current_role = State.FOLLOWER
            self.current_leader = leader_id

        message = ("log_response", self.node_id, self.current_term)
        self.send_message(f"{message}", self.neighbors_ports[leader_id - 1])

    def receive_log_response(self, follower_id, term):
        self.is_stop_timer = True
        self.election_timer.cancel()
        self.start_election_timer()

        if term > self.current_term:
            self.current_term = term
            self.current_role = State.FOLLOWER
            self.voted_for = None

    def listening_procedure(self):
        tcp_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_client_socket.bind(("127.0.0.1", self.port))
        tcp_client_socket.listen(1)

        while True:
            connection, address = tcp_client_socket.accept()
            message_raw = connection.recv(1024).decode("UTF-8")

            eval_message = eval(message_raw)
            if eval_message[0] == "vote_request":
                c_id = eval_message[1]
                c_term = eval_message[2]

                logging.info(f"node{c_id} sends a vote_request")
                logging.info("vote procedure is starting...")
                self.receive_vote_request(c_id, c_term)

            elif eval_message[0] == "vote_response":
                voter_id = eval_message[1]
                term = eval_message[2]
                granted = eval_message[3]

                self.receive_vote_response(voter_id, term, granted)

            elif eval_message[0] == "log_request":
                leader_id = eval_message[1]
                term = eval_message[2]

                logging.info(f"node{leader_id} sends a log_request")
                logging.info("Receive log is starting...")
                self.receive_log_request(leader_id, term)

            elif eval_message[0] == "log_response":
                follower_id = eval_message[1]
                term = eval_message[2]

                self.receive_log_response(follower_id, term)

    def start_election_timer(self):
        self.timer_duration = self.randomize_timer()
        logging.info("Election timer will start...")
        logging.info(f"Election timer duration: {self.timer_duration}s")
        thread = threading.Timer(
            self.timer_duration, function=self.suspect_leader_fail_or_election_timeout
        )
        thread.start()
        self.election_timer = thread

    def start(self):
        # TODO
        logging.info("Start Raft algorithm...")

        logging.info("Execute self.candidate_timer_thread.start()...")
        self.candidate_timer_thread.start()

        logging.info("Listen for any inputs...")

        thread = threading.Thread(target=self.listening_procedure)
        thread.name = "listen_procedure_thread"
        thread.start()

        thread = threading.Thread(target=self.send_heartbeat)
        thread.name = "heartbeat_thread"
        thread.start()


def reload_logging_windows(filename):
    log = logging.getLogger()
    for handler in log.handlers:
        log.removeHandler(handler)
    logging.basicConfig(
        format="%(asctime)-4s %(levelname)-6s %(threadName)s:%(lineno)-3d %(message)s",
        datefmt="%H:%M:%S",
        filename=filename,
        filemode="w",
        level=logging.INFO,
    )


def main(
    heartbeat_duration=1,
    lb_fault_duration=1,
    port=1000,
    node_id=1,
    neighbors_ports=(1000,),
    is_continue=False,
):
    reload_logging_windows(f"logs/node{node_id}.txt")
    threading.excepthook = thread_exception_handler
    try:
        logging.info(f"Node with id {node_id} is running...")
        logging.debug(f"heartbeat_duration: {heartbeat_duration}")
        logging.debug(f"lower_bound_fault_duration: {lb_fault_duration}")
        logging.debug(f"upper_bound_fault_duration = {lb_fault_duration}s + 4s")
        logging.debug(f"port: {port}")
        logging.debug(f"neighbors_ports: {neighbors_ports}")

        logging.info("Create raft object...")
        for i in range(len(neighbors_ports)):
            node_ports[neighbors_ports[i]] = i + 1
        ub_fault_duration = lb_fault_duration + 4.0
        raft = Raft(
            node_id,
            port,
            neighbors_ports,
            lb_fault_duration,
            ub_fault_duration,
            heartbeat_duration,
        )

        logging.info("Execute raft.start()...")
        raft.start()
    except Exception:
        logging.exception("Caught Error")
        raise
