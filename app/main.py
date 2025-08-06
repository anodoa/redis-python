import socket  


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    with socket.create_server(("localhost", 6379), reuse_port=True) as server_socket:

        connection, _ = server_socket.accept() # wait for client
        #print(f"accepted connection from {address}")
        
        data = connection.recv(1024)
        
        for data in connection:
            connection.sendall(b"+PONG\r\n")


if __name__ == "__main__":
    main()
