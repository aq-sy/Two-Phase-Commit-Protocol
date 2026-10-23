import socket
from server import Server
from threading import Thread
from contextlib import closing
from transaction_coordinator import TransactionCoordinator


class Transaction:
    def __init__(self, server_nums) -> None:
        self.txn_info = 1
        self.server_nums = server_nums

        self.tc = None
        self.servers = None
        self.server_ports = None

    def get_free_port(self):
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            s.bind(('', 0))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            return s.getsockname()[1]

    def start_tc(self):
        self.tc = TransactionCoordinator(self.txn_info)
        self.txn_info = self.txn_info + 1

    def start_servers(self):
        init_port = 50001
        self.servers = []
        self.server_ports = []
        for i in range(self.server_nums):
            self.server_ports.append(init_port)

            s_id = i + 1
            s_ip = 'localhost'
            s_port = self.get_free_port()

            new_server = Server(s_id, s_ip, s_port)
            self.servers.append(new_server)
            self.tc.add_server(s_ip, s_port)

    def live_server(self, server: Server, inject_failure: list):
        server_down = inject_failure[0]
        server_time_out = inject_failure[1]

        if server_down:
            server.server_down_inject()
        if server_time_out:
            server.server_timeout_inject()
        server.live()

    def start_a_simulation(self, simulation_no):
        self.start_tc()
        self.start_servers()

        threads = []
        for i in range(self.server_nums):
            inject_failure = [
                [False, False],
                [i == 0, False],
                [False, False],
                [False, i == 0],
                [False, False]
            ]

            new_thread = Thread(
                target=self.live_server,
                args=(self.servers[i], inject_failure[simulation_no - 1])
            )
            threads.append(new_thread)
            new_thread.start()

        # calling faliue simulation
        if simulation_no == 1:
            self.tc.inject_f1()
        elif simulation_no == 3:
            self.tc.inject_f3()

        # starting the transaction coordinator
        self.tc.live()
        self.tc.start_txn()
        self.tc.stop_txn()

    def menu(self):
        print("\n-----------Two Phase Commit Demonstration-----------\n")
        print("Enter Option:")
        print("1. TC fails before sending the 'prepare' message")
        print("2. TC does not receive 'yes' from a server (Server DOWN)")
        print("3. TC fails after sending one 'commit' message to the servers")
        print("4. Server fails (time-out) after replying 'yes' to TC")
        print("5. Normal transaction (No Failure)")
        print("6. EXIT")
        print(": ", end='')

    def start(self):
        while True:
            self.menu()
            opt = input().strip()
            if opt == '1' or opt == '2' or opt == '3' or opt == '4' or opt == '5':
                self.start_a_simulation(int(opt))
            elif opt == '6':
                print("Exiting...")
                break
            else:
                print("Error: Invalid option, try again...")


if __name__ == '__main__':
    server_num = int(input("Enter number of servers: "))
    txn = Transaction(server_num)
    txn.start()
