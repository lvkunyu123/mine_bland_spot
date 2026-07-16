#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import socket
import json
import threading
from typing import Dict, Tuple, Optional, Callable

class TCPSender:
    def __init__(self, pc_ip: str, pc_port: int, listen_port: int = 8889, timeout: float = 3.0):
        self.pc_ip = pc_ip
        self.pc_port = pc_port
        self.listen_port = listen_port
        self.timeout = timeout
        self.command_callback: Optional[Callable] = None
        self.listener_running = False

    def send(self, data: Dict) -> Tuple[bool, str]:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(self.timeout)
            sock.connect((self.pc_ip, self.pc_port))
            json_str = json.dumps(data, ensure_ascii=False)
            sock.sendall(json_str.encode('utf-8'))
            response = sock.recv(1024)
            sock.close()
            if response:
                resp = json.loads(response.decode())
                if resp.get('status') == 'ok':
                    return True, resp.get('type', 'unknown')
                else:
                    return False, resp.get('reason', 'unknown_error')
            else:
                return False, "empty_response"
        except Exception as e:
            return False, str(e)

    def set_command_callback(self, callback: Callable):
        self.command_callback = callback

    def start_listener(self):
        if self.listener_running:
            return
        self.listener_running = True
        threading.Thread(target=self._listen_loop, daemon=True).start()
        print(f"[监听] 指令监听端口 {self.listen_port} 已启动")

    def stop_listener(self):
        self.listener_running = False

    def _listen_loop(self):
        try:
            server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_sock.bind(('0.0.0.0', self.listen_port))
            server_sock.listen(5)
            server_sock.settimeout(1.0)
            while self.listener_running:
                try:
                    conn, addr = server_sock.accept()
                    threading.Thread(target=self._handle_command_connection,
                                     args=(conn, addr), daemon=True).start()
                except socket.timeout:
                    continue
                except Exception as e:
                    if self.listener_running:
                        print(f"[监听] 异常: {e}")
        except Exception as e:
            print(f"[监听] 启动失败: {e}")

    def test_connection(self) -> bool:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(self.timeout)
            sock.connect((self.pc_ip, self.pc_port))
            sock.close()
            return True
        except Exception:
            return False

    def _handle_command_connection(self, conn, addr):
        try:
            data = conn.recv(4096)
            if data:
                try:
                    cmd = json.loads(data.decode())
                    print(f"[指令] 从 {addr[0]} 收到: {cmd}")
                    if self.command_callback:
                        self.command_callback(cmd)
                except json.JSONDecodeError as e:
                    print(f"[指令] JSON解析失败: {e}")
        except Exception as e:
            print(f"[指令] 处理异常: {e}")
        finally:
            conn.close()