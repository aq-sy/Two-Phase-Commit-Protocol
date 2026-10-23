import time
import socket
from threading import Thread
from constants import STATE, VOTE, ACK, TIMEOUT_LIMIT, SHUTDOWN


class TransactionCoordinator:
    def __init__(self, txn_info) -> None:
        self.servers = []
        self.txn_info = txn_info

        self.votes = False
        self.ack = False
        self.state = None

        self.fail1 = False
        self.fail3 = False

    def add_server(self, s_ip, s_port):
        self.servers.append((s_ip, s_port))

    def start_txn(self):
        self.state = STATE.init
        print("\nStarting transaction", self.txn_info)

    def stop_txn(self):
        while self.state != STATE.stop:
            time.sleep(1)

    def inject_f3(self):
        self.fail3 = True

    def inject_f1(self):
        self.fail1 = True

    def live(self):
        thread = Thread(target=self.start)
        thread.start()

    def start(self):
        while True:
            time.sleep(1)
            if self.state == None:
                continue

            if self.state == STATE.init:
                self.send_broadcast()
            elif self.state == STATE.commit:
                self.send_commit()
            elif self.state == STATE.abort:
                self.shutdown_system()
                break

        self.state = STATE.stop

    def send_broadcast(self):
        time.sleep(2)
        self.votes = True
        self.thread_cnt = 0

        if self.fail1:
            print("TC down!!! (before sending PREPARE)")

        threads = []
        for i in range(len(self.servers)):
            thread = Thread(
                target=self.send_to_a_single_server, args=(self.servers[i], ))
            threads.append(thread)
            thread.start()

        while self.thread_cnt < len(self.servers):
            time.sleep(1)

        if self.votes:
            self.state = STATE.commit
        else:
            if self.fail1:
                print("[+] Transaction Abort (due to all server abort)\n")
            else:
                print("[+] Transaction Abort (due to possible server failure)\n")

            self.state = STATE.abort

    def send_to_a_single_server(self, _server):
        _ip, _port = _server
        try:
            self.send_prepare(_ip, _port)
        except TimeoutError:
            self.votes = False

        self.thread_cnt += 1

    def send_prepare(self, ip, port):
        send_socket = socket.socket()
        send_socket.connect((ip, port))

        send_socket.send(f"{self.txn_info}".encode())
        _port = send_socket.recv(1024).decode().strip()
        _port = int(_port)

        send_socket.close()
        if self.fail1:
            time.sleep(6)
            print(f"[-] '{ip}:{port}' get notify TC up!!!")

        send_socket = socket.socket()
        send_socket.settimeout(5)
        send_socket.connect((ip, _port))
        print(f"[+] TC Sent PREPARE to {ip}:{port}")
        send_socket.send((f"{STATE.prepare};{self.txn_info}").encode())
        msg = send_socket.recv(1024).decode()
        print(f"TC receives '{msg}' from {ip}:{port}")

        if msg == VOTE.no:
            self.votes = False
        send_socket.close()

    def send_commit(self):
        time.sleep(2)
        self.ack = True
        self.thread_cnt = 0

        threads = []
        for i in range(len(self.servers)):
            thread = Thread(
                target=self.commit_to_a_single_server, args=(self.servers[i], ))
            threads.append(thread)
            thread.start()

            if i == 0 and self.fail3:
                time.sleep(2)
                print("TC Down!!! (after sending 1 commit)")
                time.sleep(4)
                print("TC Up!!!")

        while self.thread_cnt < len(self.servers):
            time.sleep(1)

        if self.ack:
            self.state = STATE.abort
            print("Transaction stopped")
        else:
            self.ack = STATE.abort

    def commit_to_a_single_server(self, _server):
        _ip, _port = _server
        try:
            self.commit_prepare(_ip, _port)
        except TimeoutError:
            print("TimeoutError")

        self.thread_cnt += 1

    def commit_prepare(self, ip, port):
        send_socket = socket.socket()
        send_socket.connect((ip, port))

        send_socket.send(f"{self.txn_info}".encode())
        _port = send_socket.recv(1024).decode().strip()
        _port = int(_port)

        send_socket.close()
        send_socket = socket.socket()
        send_socket.settimeout(TIMEOUT_LIMIT)
        send_socket.connect((ip, _port))

        print(f"[+] TC COMMIT to {ip}:{port}")
        send_socket.send((f"{STATE.commit};{self.txn_info}").encode())
        msg = send_socket.recv(1024).decode()
        print(f"TC receives '{msg}' from {ip}:{port}")

        if msg == ACK.no:
            self.ack = False
        send_socket.close()

    def shutdown_system(self):
        time.sleep(1)
        threads = []
        self.thread_cnt = 0

        for i in range(len(self.servers)):
            thread = Thread(
                target=self.shutdown, args=(self.servers[i], ))
            threads.append(thread)
            thread.start()

        while self.thread_cnt < len(self.servers):
            time.sleep(1)

    def shutdown(self, _server):
        _ip, _port = _server
        self.thread_cnt = self.thread_cnt + 1
        send_socket = socket.socket()
        send_socket.connect((_ip, _port))
        send_socket.send(SHUTDOWN.encode())
        send_socket.close()
