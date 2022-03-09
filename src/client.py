import argparse
from re import S
from click import getchar
import sys
import signal
import os
from threading import Thread, Lock
import socket
import json
from math import ceil

INPUT_PREFIX = "> "
ERASE_LINE = "\x1b[2K\r"
CURSOR_UP = "\x1b[1A"
CHR_ENTER = '\x0d'
CHR_BACKSPACE = '\x7f'
CHR_TAB = '\x09'
CHR_UP = '\x1b[A'
CHR_DOWN = '\x1b[B'
CHR_RIGHT = '\x1b[C'
CHR_LEFT = '\x1b[D'
COL_BLUE = "\x1b[34;1m"
COL_GREEN = "\x1b[32m"
COL_WHITE = "\x1b[37;1m"
COL_NONE = "\x1b[0m"


class MQTTClient:
    def __init__(self, host, port):
        self._host = host
        self._port = port
        self._mutex = Lock()
        self._sock = None
        self._running = False
        self._input_str = ''
        self._past_commands = []
        self._topics = []
        self._commands = {
            "exit": {
                "info": "close program",
                "func": self._cmd_exit,
            },
            "help": {
                "info": "show all commands",
                "func": self._cmd_help
            },
            "pub": {
                "info": "publish message",
                "func": self._cmd_pub,
                "hint": ["topic message", "message"]
            },
            "sub": {
                "info": "subscribe topic",
                "func": self._cmd_sub,
                "hint": ["topic"]
            },
            "unsub": {
                "info": "unsubscribe topic",
                "func": self._cmd_unsub,
                "hint": ["topic"]
            },
            "topics": {
                "info": "show subscribed topics",
                "func": self._cmd_topics
            }
        }

    def _check_chan_msg(self, arg, check_msg=False):
        if arg == None or arg == '':
            if check_msg:
                print("missing topic and message!")
            else:
                print("missing topic!")
            return False
        parts = arg.split(' ', 1)
        if not check_msg:
            if len(parts) == 2 and parts[1] != '':
                print("too many arguments!")
                return False
            return True
        if len(parts) == 1 or parts[1] == '':
            print("missing message!")
            return False
        return True

    def _cmd_help(self):
        print("\nAll Commands\n")
        for name, data in self._commands.items():
            spaces = ' ' * (9 - len(name))
            print("{}{}: {}".format(name, spaces, data['info']))
        print()

    def _cmd_pub(self, arg):
        if not self._check_chan_msg(arg, True):
            return
        parts = arg.split(' ', 1)
        msg = {
            "cmd": "pub",
            "topic": parts[0],
            "msg": parts[1]
        }
        self._sock.sendall(json.dumps(msg).encode('utf-8'))

    def _cmd_sub(self, arg):
        if not self._check_chan_msg(arg):
            return
        topic = arg.split(' ', 1)[0]
        if topic not in self._topics:
            msg = {
                "cmd": "sub",
                "topic": topic,
            }
            self._sock.sendall(json.dumps(msg).encode('utf-8'))
        else:
            print(f"{COL_GREEN}{topic}{COL_NONE} already in subscribed topics!")

    def _cmd_unsub(self, arg):
        if not self._check_chan_msg(arg):
            return
        topic = arg.split(' ', 1)[0]
        if topic in self._topics:
            msg = {
                "cmd": "unsub",
                "topic": topic,
            }
            self._sock.sendall(json.dumps(msg).encode('utf-8'))
        else:
            print(f"{COL_GREEN}{topic}{COL_NONE} not in subscribed topics!")

    def _cmd_topics(self):
        if len(self._topics) == 0:
            return
        print(COL_GREEN, end='')
        for name in self._topics:
            print(name)
        print(COL_NONE, end='')

    def _cmd_exit(self):
        os.kill(os.getpid(), signal.SIGINT)

    def _input(self, prefix):
        past_len = len(self._past_commands)
        cmd_ptr = past_len
        index = 0
        cmd = ''
        print(f"{prefix}", end='')
        sys.stdout.flush()
        while True:
            chr = getchar(echo=False)
            if chr == CHR_ENTER:
                break
            elif chr == CHR_BACKSPACE:
                if len(cmd) > 0:
                    if index == len(cmd):
                        cmd = cmd[:-1]
                        index -= 1
                    elif index > 0:
                        cmd = cmd[:index-1] + cmd[index:]
                        index -= 1
            elif chr == CHR_TAB:
                cmd = self._auto_complete(cmd)
                index = len(cmd)
            elif chr == CHR_UP:
                if past_len and cmd_ptr > 0:
                    cmd_ptr -= 1
                    cmd = self._past_commands[cmd_ptr]
                    index = len(cmd)
            elif chr == CHR_DOWN:
                if past_len and cmd_ptr < past_len-1:
                    cmd_ptr += 1
                    cmd = self._past_commands[cmd_ptr]
                    index = len(cmd)
            elif chr == CHR_RIGHT:
                if index < len(cmd):
                    index += 1
            elif chr == CHR_LEFT:
                if index > 0:
                    index -= 1
            else:
                if index == len(cmd):
                    cmd += str(chr)
                elif index == 0:
                    cmd = str(chr) + cmd
                else:
                    cmd = cmd[:index] + str(chr) + cmd[index:]
                index += 1
            cursor = ''
            hint_cmd, add_idx = self._get_hint(cmd)
            if index < len(hint_cmd):
                cursor = f'\x1b[{len(hint_cmd) - index - add_idx}D'
            with self._mutex:
                self._input_str = f"{prefix}{hint_cmd}{cursor}"
                print(f"{ERASE_LINE}{self._input_str}", end='')
                sys.stdout.flush()
        self._past_commands.append(cmd)
        with self._mutex:
            self._input_str = prefix
            print(f"{ERASE_LINE}{INPUT_PREFIX}{cmd}")
        return cmd

    def _get_hint(self, cmd):
        parts = cmd.split(' ', 2)
        if len(parts) < 2:
            return cmd, 0
        for command, data in self._commands.items():
            if "hint" in data.keys() and command == parts[0]:
                for index, hint in enumerate(data["hint"]):
                    if len(parts) == 2+index and parts[1+index] == '':
                        return cmd + COL_BLUE + hint + COL_NONE, 11
        return cmd, 0

    def _auto_complete(self, cmd):
        parts = cmd.split(' ', 2)
        hint = parts[-1]
        cands = []
        if len(parts) == 1:
            cands = [key for key in iter(self._commands) if key.startswith(hint)]
        if len(parts) == 2:
            cands = [key for key in self._topics if key.startswith(hint)]
        if len(cands) == 1:
            cmd = cands[0] if len(parts) == 1 else parts[0] + ' ' + cands[0]
        elif len(cands) > 1:
            key = cands[0]
            index = len(hint)
            cmd = hint if len(parts) == 1 else parts[0] + ' ' + hint
            for i,c in enumerate(key[index:]):
                for cand in cands:
                    if len(cand)-1 < index+i or cand[index+i] is not c:
                        return cmd
                cmd += str(c)
        return cmd

    def _check_cmd(self, cmd):
        parts = cmd.split(' ', 1)
        for name, data in self._commands.items():
            if parts[0] == name:
                if "hint" not in data.keys():
                    if len(parts) == 2 and parts[1] != '':
                        print("command takes no arguments!")
                    else:
                        data['func']()
                else:
                    data['func'](parts[1] if len(parts) == 2 else None)
                return
        print("undefined command!")

    def _process_msg(self, msg):
        js = None
        try:
            js =json.loads(msg)
            status = js['status']
            if status == "ok":
                res = js['msg']
                action = res['action']
                topic = res['topic']
                if action == "pub":
                    return 'published "{}" on topic {}{}{} for {} recipients'.format(
                        res['msg'], COL_GREEN, topic, COL_NONE, res['clients'])
                elif action == "sub":
                    self._topics.append(topic)
                    return 'subscribed topic {}{}{}'.format(
                        COL_GREEN, topic, COL_NONE)
                elif action == "unsub":
                    self._topics.remove(topic)
                    return 'unsubscribed topic {}{}{}'.format(
                        COL_GREEN, topic, COL_NONE)
            elif status == "fail":
                res = js['msg']
                if type(res) is str:
                    return f'got server error: {res}'
                action = res['action']
                topic = res['topic']
                err = res['msg']
                if action == "sub":
                    self._topics.append(topic)
                    return 'failed to subscribe topic {}{}{}: {}'.format(
                        COL_GREEN, topic, COL_NONE, err)
                elif action == "unsub":
                    self._topics.remove(topic)
                    return 'failed to unsubscribed topic {}{}{}: {}'.format(
                        COL_GREEN, topic, COL_NONE, err)
            elif status == "msg":
                res = js['msg']
                topic = res['topic']
                msg = res['msg']
                return '{}{}{}: {}{}'.format(
                        COL_GREEN, topic, COL_WHITE, msg, COL_NONE)
        except:
            pass
        return f'unhandled message: {msg}'

    def _wait_messages(self):
        self._sock.settimeout(0.1)
        while self._running:
            try:
                data_recv = self._sock.recv(1024)
                if data_recv == b'':
                    with self._mutex:
                        print(f'{ERASE_LINE}MQTT broker disconnected')
                        print(f'\r{self._input_str}', end='')
                        sys.stdout.flush()
                    os.kill(os.getpid(), signal.SIGINT)
                    break
                res = self._process_msg(data_recv.decode())
                with self._mutex:
                    print(f'{ERASE_LINE}{res}')
                    print(f'\r{self._input_str}', end='')
                    sys.stdout.flush()
            except socket.timeout:
                pass
            except:
                raise

    def _try_connect(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        try:
            sock.connect((self._host, self._port))
            print(f'connected MQTT broker {self._host}:{self._port}')
        except ConnectionRefusedError:
            print(f'no MQTT broker connection available at {self._host}:{self._port}')
            return None
        return sock

    def start(self):
        self._sock = self._try_connect()
        if self._sock == None:
            return
        print('\nType "help" for commands or "exit" to quit.\n')
        self._running = True
        read_thread = Thread(target=self._wait_messages)
        read_thread.start()
        try:
            while True:
                cmd = self._input(INPUT_PREFIX)
                self._check_cmd(cmd)
        except KeyboardInterrupt:
            print("exiting")
        finally:
            print("closing connection")
            self._running = False
            read_thread.join()
            self._sock.close()

def parse_args():
    argp = argparse.ArgumentParser()
    argp.add_argument('--host', type=str, default='127.0.0.1')
    argp.add_argument('--port', type=int, default=12346)
    args = argp.parse_args()
    return args.host, args.port

def main():
    host, port = parse_args()
    client = MQTTClient(host, port)
    client.start()

if __name__ == "__main__":
    main()