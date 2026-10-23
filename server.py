import time
import socket
from constants import VOTE, STATE, ACK, TIMEOUT_LIMIT, SHUTDOWN     


class Server:
    def __init__(self, _server_id, _ip, _port):
        self.server_down_failure = False
        self.server_timeout_failure = False

        self.server_id = _server_id
        self.ip = _ip
        self.port = _port
        self.l_port = _port + 1

        self.vote = VOTE.yes
        self.txn_info = None

    def server_down_inject(self):
        self.server_down_failure = True

    def server_timeout_inject(self):
        self.server_timeout_failure = True

    def live(self):
        self.primary_socket = socket.socket()
        self.primary_socket.bind((self.ip, self.port))
        self.primary_socket.listen(5)

        while True:
            conn, add = self.primary_socket.accept()

            self.txn_info = conn.recv(1024).decode()
            if self.txn_info == SHUTDOWN:
                conn.close()
                break
                
            conn.send((f"{self.l_port}").encode())

            self.listen_socket = socket.socket()
            self.listen_socket.bind((self.ip, self.l_port))
            try:
                self.listen_socket.listen(1)
                self.listen_socket.settimeout(TIMEOUT_LIMIT)
                conn, addr = self.listen_socket.accept()

                _msg = conn.recv(1024).decode()
                msg, self.txn_info = _msg.split(';')

                print(
                    f"[-] Server {self.ip}:{self.port} receives {msg} from TC")

                if self.server_down_failure:
                    time.sleep(2)
                    print(f"[-] Server '{self.ip}:{self.port}' Down!!!")
                    time.sleep(4)

                if self.server_timeout_failure:
                    print(f"[-] Server '{self.ip}:{self.port}' Down!!!")
                    time.sleep(2)
                    print(f"[-] Server '{self.ip}:{self.port}' UP!!!")
                    print(
                        f"[-] Server '{self.ip}:{self.port}' fetched transaction ID(from TC): {self.txn_info}")

                if msg == STATE.prepare:
                    conn.send(self.vote.encode())
                elif msg == STATE.commit:
                    conn.send(ACK.yes.encode())
                    
            except TimeoutError:
                print(f"Server '{self.ip}:{self.port}' abort")
                self.listen_socket.settimeout(None)
                conn, addr = self.listen_socket.accept()
                _msg = conn.recv(1024).decode()
                msg, self.txn_info = _msg.split(';')
                self.vote = VOTE.no

                print(
                    f"[-] Server {self.ip}:{self.port} receives {msg} from TC")

                if self.server_down_failure:
                    time.sleep(2)
                    print(f"[-] Server '{self.ip}:{self.port}' Down!!!")
                    time.sleep(4)
                if msg == STATE.prepare:
                    conn.send(self.vote.encode())
                elif msg == STATE.commit:
                    conn.send(ACK.yes.encode())

            self.listen_socket.close()
