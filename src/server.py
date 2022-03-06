import socket
import argparse
from threading import Thread, Event, Lock, Timer
import json


class Client(Thread):
    def __init__(self, conn, index, cb_close, cb_msg):
        Thread.__init__(self)
        self._conn = conn
        self._index = index
        self._signal = Event()
        self._cb_close = cb_close
        self._cb_msg = cb_msg

    @property
    def index(self):
        return self._index

    def stop(self):
        self._signal.set()

    def send_msg(self, msg):
        self._conn.sendall(msg.encode('utf-8'))
        return

    def run(self):
        self._conn.settimeout(0.1)
        while not self._signal.is_set():
            try:
                data = self._conn.recv(1024)
                if data == b'':
                    print(f"client#{self._index} disconnected")
                    self._cb_close(self)
                    break
                self._cb_msg(self, data.decode())
            except socket.timeout:
                continue
            except:
                print(f"client#{self._index} disconnected yep")
                self._cb_close(self)
                break
        self._conn.close()


class ConnectHandler:
    def __init__(self):
        self._threads = {}
        self._topics = {}
        self._index = 0
        self._mutex = Lock()

    def remove_clients(self):
        print("remove clients")
        for _, thread in self._threads.items():
            if thread.is_alive():
                thread.stop()
                thread.join()

    def _erase_client(self, index):
        try:
            thread = self._threads[index]
            if thread.is_alive():
                thread.stop()
                thread.join()
            del self._threads[index]
            print("removed client#{}, total connections: {}".format(index, len(self._threads)))
        except:
            print("client#{} already removed!".format(index))

    def remove_client(self, client):
        removes = []
        for topic, indexes in self._topics.items():
            if client.index in indexes:
                with self._mutex:
                    self._topics[topic].remove(client.index)
                    print(f"removed client#{client.index} from topic {topic}")
                    if not self._topics[topic]:
                        removes.append(topic)
        for topic in removes:
            del self._topics[topic]
            print(f"removed topic {topic}")
        Timer(0.01, self._erase_client, args=[client.index,]).start()

    def _send_msg(self, client, status, msg):
        res = {
            "status": status,
            "msg": msg
        }
        client.send_msg(json.dumps(res))

    def _cmd_pub(self, client, cmd):
        if "topic" not in cmd.keys():
            self._send_msg(client, "fail, ""missing topic")
            return
        if "msg" not in cmd.keys():
            self._send_msg(client, "fail", "missing message")
            return
        topic = cmd['topic']
        msg = {
            "action": "pub",
            "topic": topic,
            "msg": cmd['msg']
        }
        if topic not in self._topics:
            msg['clients'] = 0
            self._send_msg(client, "ok", msg)
        else:
            res = {
                "topic": topic,
                "msg": cmd['msg']
            }
            with self._mutex:
                for index in self._topics[topic]:
                    try:
                        self._send_msg(self._threads[index], "msg", res)
                    except:
                        pass
            msg['clients'] = len(self._topics[topic])
            self._send_msg(client, "ok", msg)
            print('published message "{}" for {} clients on topic {}'.format(
                cmd['msg'], msg['clients'], topic
            ))


    def _cmd_sub(self, client, cmd):
        if "topic" not in cmd.keys():
            self._send_msg(client, "fail", "missing topic")
            return
        topic = cmd['topic']
        msg = {
            "action": "sub",
            "topic": topic
        }
        if topic not in self._topics:
            with self._mutex:
                self._topics[topic] = [client.index]
                print(f"new topic {topic} with recipient client#{client.index}")
            self._send_msg(client, "ok", msg)
        else:
            if client.index in self._topics[topic]:
                msg['msg'] = "already subscribed"
                self._send_msg(client, "fail", msg)
            else:
                with self._mutex:
                    self._topics[topic].append(client.index)
                    print(f"add recipient client#{client.index} for topic {topic}")
                self._send_msg(client, "ok", msg)

    def _cmd_unsub(self, client, cmd):
        if "topic" not in cmd.keys():
            self._send_msg(client, "fail", "missing topic")
            return
        topic = cmd['topic']
        msg = {
            "action": "unsub",
            "topic": topic
        }
        if topic not in self._topics:
            msg['msg'] = "topic not found"
            self._send_msg(client, "fail", msg)
        else:
            if client.index in self._topics[topic]:
                with self._mutex:
                    self._topics[topic].remove(client.index)
                    print(f"removed client#{client.index} topic {topic}")
                    if not self._topics[topic]:
                        del self._topics[topic]
                        print(f"removed topic {topic}")
                self._send_msg(client, "ok", msg)
            else:
                msg['msg'] = "topic not subscribed"
                self._send_msg(client, "fail", msg)

    def _process_cmd(self, client, cmd):
        if "cmd" not in cmd.keys():
            self._send_msg(client, "fail", "missing command")
            return
        if cmd['cmd'] == "pub":
            self._cmd_pub(client, cmd)
        elif cmd['cmd'] == "sub":
            self._cmd_sub(client, cmd)
        elif cmd['cmd'] == "unsub":
            self._cmd_unsub(client, cmd)
        else:
            self._send_msg(client, "fail", "undefined command")

    def add_msg(self, client, msg):
        js = None
        try:
            js = json.loads(msg)
        except:
            self._send_msg(client, "fail", "parsing error")
            return
        self._process_cmd(client, js)

    def add_connection(self, conn):
        self._index += 1
        client = Client(conn, self._index, cb_close=self.remove_client, cb_msg=self.add_msg)
        client.start()
        self._threads[self._index] = client
        print("added client#{}, total connections: {}".format(self._index, len(self._threads)))


class MQTTServer:
    def __init__(self, host, port):
        self._host = host
        self._port = port
        self._handler = ConnectHandler()

    def start(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self._host, self._port))
        sock.listen()
        print(f"start listening connections in {self._host}:{self._port}")
        try:
            while True:
                conn, addr = sock.accept()
                print("new connection, address {}:{}".format(addr[0], addr[1]))
                self._handler.add_connection(conn)
        except KeyboardInterrupt:
            print("keyboard interrupt")
        finally:
            self._handler.remove_clients()
            print("closing server")
            sock.close()


def parse_args():
    argp = argparse.ArgumentParser()
    argp.add_argument('--host', type=str, default='127.0.0.1')
    argp.add_argument('--port', type=int, default=12346)
    args = argp.parse_args()
    return args.host, args.port

def main():
    host, port = parse_args()
    server = MQTTServer(host, port)
    server.start()

if __name__ == "__main__":
    main()