import threading
import time
import random
import string

# 模拟请求文件内容
def generate_requests_file(file_path, num_requests):
    commands = ['P', 'R', 'G']
    with open(file_path, 'w') as file:
        for _ in range(num_requests):
            command = random.choice(commands)
            key = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
            value = ''.join(random.choices(string.ascii_letters + string.digits, k=16)) if command == 'P' else ''
            request = f"{len(key) + len(value) + len(command) + 2} {command} {key} {value}\n"
            file.write(request)

# 启动服务器
def start_server():
    import socket
    import threading
    import time
    from collections import defaultdict

    class TupleSpaceServer:
        def __init__(self, host, port):
            self.host = host
            self.port = port
            self.tuple_space = {}
            self.lock = threading.Lock()
            self.total_clients = 0
            self.total_operations = 0
            self.total_reads = 0
            self.total_gets = 0
            self.total_puts = 0
            self.total_errors = 0

        def start(self):
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.bind((self.host, self.port))
            server_socket.listen(5)
            print(f"Server listening on {self.host}:{self.port}")

            while True:
                client_socket, addr = server_socket.accept()
                print(f"Accepted connection from {addr}")
                client_thread = threading.Thread(target=self.handle_client, args=(client_socket,))
                client_thread.start()
                self.total_clients += 1

        def handle_client(self, client_socket):
            while True:
                try:
                    request = client_socket.recv(1024).decode('utf-8')
                    if not request:
                        break
                    response = self.process_request(request)
                    client_socket.sendall(response.encode('utf-8'))
                except Exception as e:
                    print(f"Error handling client: {e}")
                    break
            client_socket.close()

        def process_request(self, request):
            parts = request.split()
            length = int(parts[0])
            command = parts[1]
            key = parts[2]
            value = ' '.join(parts[3:]) if len(parts) > 3 else None

            with self.lock:
                self.total_operations += 1
                if command == 'P':
                    self.total_puts += 1
                    if key in self.tuple_space:
                        self.total_errors += 1
                        return f"{length} ERR {key} already exists"
                    else:
                        self.tuple_space[key] = value
                        return f"{length} OK ({key}, {value}) added"
                elif command == 'R':
                    self.total_reads += 1
                    if key in self.tuple_space:
                        value = self.tuple_space[key]
                        return f"{length} OK ({key}, {value}) read"
                    else:
                        self.total_errors += 1
                        return f"{length} ERR {key} does not exist"
                elif command == 'G':
                    self.total_gets += 1
                    if key in self.tuple_space:
                        value = self.tuple_space.pop(key)
                        return f"{length} OK ({key}, {value}) removed"
                    else:
                        self.total_errors += 1
                        return f"{length} ERR {key} does not exist"

        def print_status(self):
            while True:
                time.sleep(10)
                with self.lock:
                    print(f"Total clients: {self.total_clients}")
                    print(f"Total operations: {self.total_operations}")
                    print(f"Total reads: {self.total_reads}")
                    print(f"Total gets: {self.total_gets}")
                    print(f"Total puts: {self.total_puts}")
                    print(f"Total errors: {self.total_errors}")
                    print(f"Current tuples: {len(self.tuple_space)}")
                    avg_tuple_size = sum(len(k) + len(v) for k, v in self.tuple_space.items()) / len(self.tuple_space) if self.tuple_space else 0
                    avg_key_size = sum(len(k) for k in self.tuple_space.keys()) / len(self.tuple_space) if self.tuple_space else 0
                    avg_value_size = sum(len(v) for v in self.tuple_space.values()) / len(self.tuple_space) if self.tuple_space else 0
                    print(f"Average tuple size: {avg_tuple_size:.2f}")
                    print(f"Average key size: {avg_key_size:.2f}")
                    print(f"Average value size: {avg_value_size:.2f}")

    if __name__ == "__main__":
        host = 'localhost'
        port = 51234
        server = TupleSpaceServer(host, port)
        status_thread = threading.Thread(target=server.print_status)
        status_thread.start()
        server.start()

# 启动客户端
def start_client(request_file):
    import socket
    import sys

    class TupleSpaceClient:
        def __init__(self, host, port, request_file):
            self.host = host
            self.port = port
            self.request_file = request_file

        def run(self):
            with open(self.request_file, 'r') as file:
                requests = file.readlines()

            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect((self.host, self.port))

            for request in requests:
                request = request.strip()
                if not request:
                    continue
                client_socket.sendall(request.encode('utf-8'))
                response = client_socket.recv(1024).decode('utf-8')
                print(f"{request}: {response}")

            client_socket.close()

    if __name__ == "__main__":
        host = 'localhost'
        port = 51234
        client = TupleSpaceClient(host, port, request_file)
        client.run()

# 主测试逻辑
if __name__ == "__main__":
    # 生成请求文件
    request_file = "requests.txt"
    num_requests = 100
    generate_requests_file(request_file, num_requests)

    # 启动服务器
    server_thread = threading.Thread(target=start_server)
    server_thread.daemon = True
    server_thread.start()
    time.sleep(1)  # 等待服务器启动

    # 启动多个客户端
    num_clients = 5
    client_threads = []
    for _ in range(num_clients):
        client_thread = threading.Thread(target=start_client, args=(request_file,))
        client_thread.start()
        client_threads.append(client_thread)

    # 等待客户端完成
    for client_thread in client_threads:
        client_thread.join()

    print("测试完成")