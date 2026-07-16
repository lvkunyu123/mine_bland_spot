#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
网络控制器模块 – 自适应动态组包 + 改进LRU分级缓存 + 模糊控制链路切换
功能：
    1. 数据生成：随机产生多条报文数据
    2. 网络检测：通过模糊控制模块判断当前网络环境
    3. 链路切换：根据网络质量自动切换WiFi/蜂窝链路
    4. 分级缓存：两级LRU缓存，高优数据不淘汰
    5. 重传机制：根据网络质量动态计算重传次数，高优强制3次
    6. 补发机制：网络恢复后自动补发缓存数据
流程：
    随机产生数据 → 网络检测 → 网络优良直接传输
    → 某条链路优良则采取该链路 → 网络差则写入L1缓存
    → 网络恢复时补发 → 发送失败写入L2缓存 → 再次补发
    → 发送成功则从缓存删除
"""

import time
import threading
import random
from typing import Dict, Optional, Tuple, List

from data_generator import DataGenerator
from lru_cache import TwoLevelCache
from tcp_sender import TCPSender
from fuzzy_link_switch_core import FuzzyLinkDecisionEngine
from adaptive_packet import AdaptivePacketController


class NetworkController:
    def __init__(self, vehicle_id: str, pc_ip: str, pc_port: int,
                 listen_port: int = 8889,
                 l1_cache_mb: float = 50.0, l2_cache_mb: float = 30.0):
        self.vehicle_id = vehicle_id
        self.data_gen = DataGenerator(vehicle_id)

        self.two_level_cache = TwoLevelCache(l1_cache_mb, l2_cache_mb)

        self.sender = TCPSender(pc_ip, pc_port, listen_port)
        self.fuzzy = FuzzyLinkDecisionEngine()
        self.packet_ctrl = AdaptivePacketController()

        self.current_link = "CELLULAR"
        self.current_metrics = {"rsrp": -80, "rssi": -60, "loss": 5.0}
        self.scene = "网络质量良好"

        self.stats = {
            "sent_ok": 0,
            "sent_fail": 0,
            "cached_l1": 0,
            "cached_l2": 0,
            "evicted": 0,
            "resend_success": 0,
            "resend_fail": 0,
            "high_priority_sent": 0,
            "link_switches": 0
        }

        self._network_available = True
        self._last_network_state = True

        self.sender.set_command_callback(self._handle_command)
        self.sender.start_listener()

        self._resend_thread_running = False
        self._resend_thread = None

        self._resend_lock = threading.Lock()

    def _handle_command(self, command: Dict):
        cmd_type = command.get('cmd')
        if cmd_type == 'RESEND_CMD':
            seq = command.get('seq')
            if seq is not None:
                print(f"[指令] 收到PC端补发指令 seq={seq}")
                threading.Thread(target=self._resend_by_seq, args=(seq,), daemon=True).start()
        elif cmd_type == 'lock_high_priority':
            print(f"[指令] 收到PC端锁存预警指令（锁存由本地网络状态自动决定）")
        elif cmd_type == 'NET_STATUS':
            print(f"[指令] 收到网络状态指令: {command}")
        else:
            print(f"[指令] 未知指令类型: {cmd_type}")

    def update_network(self, rsrp: float, rssi: float, loss: float) -> Dict:
        self.current_metrics = {"rsrp": rsrp, "rssi": rssi, "loss": loss}
        decision = self.fuzzy.decide(rsrp, rssi, loss, self.current_link)
        cmd = decision['command']
        old_link = self.current_link

        if cmd == "SWITCH_TO_WIFI":
            self.current_link = "WIFI"
            if old_link != "WIFI":
                self.stats["link_switches"] += 1
                print(f"[链路切换] 从 {old_link} 切换到 WIFI")
        elif cmd == "SWITCH_TO_CELLULAR":
            self.current_link = "CELLULAR"
            if old_link != "CELLULAR":
                self.stats["link_switches"] += 1
                print(f"[链路切换] 从 {old_link} 切换到 CELLULAR")

        was_available = self._network_available
        state = decision.get('state', '')
        lq = decision.get('lq', 50)

        if state == "失效" or lq < 15:
            self._network_available = False
            self.scene = "无信号"
        else:
            self._network_available = True
            if lq >= 75:
                self.scene = "网络质量良好"
            elif self.current_link == "WIFI":
                self.scene = "WiFi链路可用"
            elif self.current_link == "CELLULAR":
                self.scene = "蜂窝链路可用"
            else:
                self.scene = "网络质量良好"

        packet_params = self.packet_ctrl.update(lq)
        pkt_state = packet_params['state']

        if self._network_available and not was_available:
            print(f"[网络恢复] 检测到网络恢复，开始补发缓存数据 (状态:{pkt_state})")
            threading.Thread(target=self._auto_resend_all, daemon=True).start()
        elif not self._network_available and was_available:
            print(f"[网络中断] 检测到网络中断，停止发送，数据写入缓存 (状态:{pkt_state})")
        else:
            print(f"[动态组包] 状态:{pkt_state} 包大小:{packet_params['packet_size']}B "
                  f"间隔:{packet_params['interval_ms']}ms 重传:{packet_params['retries_regular']}次")

        self._last_network_state = self._network_available
        return decision

    def is_network_available(self) -> bool:
        return self._network_available

    def _calc_max_retries(self, priority: int) -> int:
        return self.packet_ctrl.calc_retries(
            self.packet_ctrl.current_q_score, priority)

    def get_packet_params(self) -> Dict:
        return self.packet_ctrl.get_params()

    def _send_data(self, data: Dict) -> Tuple[bool, str]:
        seq = data.get('seq')
        priority = data.get('priority', 0)

        if not self._network_available:
            return False, "network_unavailable"

        max_retries = self._calc_max_retries(priority)

        for attempt in range(max_retries):
            if not self._network_available:
                print(f"[发送中断] seq={seq} 发送过程中网络变差，停止重试")
                return False, "network_lost_during_send"

            success, ack_type = self.sender.send(data)
            if success:
                self.stats["sent_ok"] += 1
                if priority == 1:
                    self.stats["high_priority_sent"] += 1
                print(f"[发送成功] seq={seq} 优先级={'高' if priority==1 else '常规'} "
                      f"尝试{attempt+1}/{max_retries}次 ACK={ack_type}")
                return True, "success"
            else:
                print(f"[发送失败] seq={seq} 尝试{attempt+1}/{max_retries}次 原因: {ack_type}")
                if attempt < max_retries - 1:
                    wait_time = 0.5 * (attempt + 1)
                    time.sleep(wait_time)

        self.stats["sent_fail"] += 1
        return False, f"failed_after_{max_retries}_retries"

    def _resend_by_seq(self, seq: int):
        data = self.two_level_cache.get(seq)
        if data is None:
            print(f"[补发] seq={seq} 不在缓存中")
            return

        if not self._network_available:
            print(f"[补发] seq={seq} 网络不可用，保留在缓存")
            return

        with self._resend_lock:
            print(f"[补发] 开始补发 seq={seq}")
            success, reason = self._send_data(data)

            if success:
                self.two_level_cache.delete(seq)
                self.stats["resend_success"] += 1
                print(f"[补发成功] seq={seq} 已从缓存删除")
            else:
                self.stats["resend_fail"] += 1
                priority = data.get('priority', 0)
                if self.two_level_cache.l1.read(seq) is not None:
                    self.two_level_cache.move_to_l2(seq)
                    print(f"[补发失败] seq={seq} 移入L2缓存等待再次补发")
                else:
                    print(f"[补发失败] seq={seq} 已在L2缓存，保留等待下次补发")

    def _auto_resend_all(self):
        if not self._network_available:
            return

        if not self._resend_lock.acquire(blocking=False):
            print("[自动补发] 已有补发线程运行中，跳过本次")
            return

        try:
            print("[自动补发] 开始自动补发所有缓存数据")

            l1_data = self.two_level_cache.get_l1_all()
            if l1_data:
                print(f"[自动补发] L1缓存共 {len(l1_data)} 条待补发")
                for data in l1_data:
                    if not self._network_available:
                        print("[自动补发] 网络再次变差，停止补发")
                        break
                    seq = data.get('seq')
                    success, reason = self._send_data(data)
                    if success:
                        self.two_level_cache.delete(seq)
                        self.stats["resend_success"] += 1
                    else:
                        self.stats["resend_fail"] += 1
                        self.two_level_cache.move_to_l2(seq)

            l2_data = self.two_level_cache.get_l2_all()
            if l2_data:
                print(f"[自动补发] L2缓存共 {len(l2_data)} 条待补发")
                for data in l2_data:
                    if not self._network_available:
                        print("[自动补发] 网络再次变差，停止补发")
                        break
                    seq = data.get('seq')
                    success, reason = self._send_data(data)
                    if success:
                        self.two_level_cache.delete(seq)
                        self.stats["resend_success"] += 1
                    else:
                        self.stats["resend_fail"] += 1
                        print(f"[L2补发失败] seq={seq} 继续保留在L2缓存")

            print(f"[自动补发] 补发完成，L1剩余{self.two_level_cache.l1_size()}条，"
                  f"L2剩余{self.two_level_cache.l2_size()}条")
        finally:
            self._resend_lock.release()

    def process_new_data_batch(self, data_list: List[Dict]):
        for data in data_list:
            self.process_new_data(data)

    def process_new_data(self, data: Dict):
        seq = data.get('seq')
        priority = data.get('priority', 0)

        if self._network_available:
            success, reason = self._send_data(data)
            if success:
                return
            else:
                print(f"[缓存] seq={seq} 发送失败({reason})，写入L1缓存")
                ok, msg = self.two_level_cache.add_to_l1(data)
                if ok:
                    self.stats["cached_l1"] += 1
                else:
                    print(f"[缓存失败] seq={seq} L1写入失败: {msg}")
        else:
            print(f"[缓存] seq={seq} 网络不可用，写入L1缓存")
            ok, msg = self.two_level_cache.add_to_l1(data)
            if ok:
                self.stats["cached_l1"] += 1
            else:
                print(f"[缓存失败] seq={seq} L1写入失败: {msg}")

    def run_cycle(self, rsrp: float, rssi: float, loss: float,
                  min_batch: int = 1, max_batch: int = 5) -> Dict:
        decision = self.update_network(rsrp, rssi, loss)

        lq = decision.get('lq', 0)
        state = decision.get('state', '')
        cmd = decision.get('command', '')
        print(f"[网络状态] RSRP={rsrp}dBm RSSI={rssi}dBm Loss={loss}% "
              f"LQ={lq:.2f} 状态={state} 指令={cmd} 当前链路={self.current_link}")

        batch_data = self.data_gen.generate_random_batch(min_batch, max_batch, scene=self.scene)
        print(f"[数据生成] 本次生成 {len(batch_data)} 条数据，场景: {self.scene}")
        for d in batch_data:
            prio_str = "高优" if d['priority'] == 1 else "常规"
            print(f"  - seq={d['seq']} 优先级={prio_str}")

        if self._network_available:
            packets = self.packet_ctrl.batch_data_to_packets(batch_data)
            print(f"[动态组包] 共 {len(packets)} 个包，包大小目标: {self.packet_ctrl.current_packet_size}B")
            for i, pkt in enumerate(packets):
                print(f"  包{i+1}: {len(pkt)}条数据")
                for d in pkt:
                    self.process_new_data(d)
                    interval = self.packet_ctrl.current_interval_ms / 1000.0
                    if interval > 0:
                        time.sleep(interval)
        else:
            self.process_new_data_batch(batch_data)

        return {
            "decision": decision,
            "generated_count": len(batch_data),
            "scene": self.scene,
            "network_available": self._network_available,
            "current_link": self.current_link,
            "packet_params": self.packet_ctrl.get_params()
        }

    def start_background_resend(self, interval: float = 5.0):
        if self._resend_thread_running:
            return

        self._resend_thread_running = True

        def _bg_resend_loop():
            print(f"[后台补发] 后台补发线程已启动，间隔 {interval} 秒")
            while self._resend_thread_running:
                try:
                    time.sleep(interval)
                    if self._network_available:
                        l1_size = self.two_level_cache.l1_size()
                        l2_size = self.two_level_cache.l2_size()
                        if l1_size > 0 or l2_size > 0:
                            print(f"[后台补发] 检测到缓存数据(L1:{l1_size}, L2:{l2_size})，尝试补发")
                            self._auto_resend_all()
                except Exception as e:
                    print(f"[后台补发] 异常: {e}")

        self._resend_thread = threading.Thread(target=_bg_resend_loop, daemon=True)
        self._resend_thread.start()

    def stop_background_resend(self):
        self._resend_thread_running = False

    def get_stats(self) -> Dict:
        return {
            **self.stats,
            "l1_cache_size": self.two_level_cache.l1_size(),
            "l2_cache_size": self.two_level_cache.l2_size(),
            "total_cache_size": self.two_level_cache.total_size(),
            "l1_high_priority": self.two_level_cache.l1.get_high_priority_count(),
            "l2_high_priority": self.two_level_cache.l2.get_high_priority_count(),
            "current_link": self.current_link,
            "scene": self.scene,
            "network_available": self._network_available
        }

    def test_connection(self) -> bool:
        return self.sender.test_connection()
