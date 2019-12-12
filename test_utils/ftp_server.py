import os
import threading
import time

from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer


class TestFTPServer(threading.Thread):
    def __init__(self, port=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.running = True
        self.started = False
        self.port = port or 2121

    def run(self):
        # Instantiate a dummy authorizer for managing 'virtual' users
        authorizer = DummyAuthorizer()

        # Define a new user having full r/w permissions and a read-only
        # anonymous user
        authorizer.add_user('user', '12345', '.', perm='elradfmwMT')
        authorizer.add_anonymous(os.getcwd())

        # Instantiate FTP handler class
        handler = FTPHandler
        handler.authorizer = authorizer

        # Define a customized banner (string returned when client connects)
        handler.banner = "pyftpdlib based ftpd ready."

        # Specify a masquerade address and the range of ports to use for
        # passive connections.  Decomment in case you're behind a NAT.
        # handler.masquerade_address = '151.25.42.11'
        # handler.passive_ports = range(60000, 65535)

        # Instantiate FTP server class and listen on 0.0.0.0:2121
        address = ('', self.port)
        server = FTPServer(address, handler)

        # set a limit for connections
        server.max_cons = 256
        server.max_cons_per_ip = 5

        while self.running:
            try:
                server.serve_forever(timeout=0.1, blocking=False, handle_exit=False)
            finally:
                self.started = True

    def start(self) -> None:
        super().start()
        while not self.started:
            time.sleep(0.1)

    def stop(self):
        self.running = False
        self.join(timeout=20)
