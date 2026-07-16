#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
自适应动态组包 + 改进LRU分级缓存 + 模糊控制链路切换
主程序 - 常驻运行仿真
流程：
    随机产生数据 → 网络检测 → 网络优良直接传输
    → 某条链路优良则采取该链路 → 网络差则写入L1缓存
    → 网络恢复时补发 → 发送失败写入L2缓存 → 再次补发
    → 发送成功则从缓存删除
"""
import time
import random
import json
import signal
import sys
from network_controller import NetworkController


def print_separator(char: str = "=", length: int = 70):
    print(char * length)


class MainSimulator:
    def __init__(self, pc_ip: str = "192.168.1.3", pc_port: int = 8080,
                 listen_port: int = 8889, vehicle_id: str = "TRUCK-001"):
        self.pc_ip = pc_ip
        self.pc_port = pc_port
        self.listen_port = listen_port
        self.vehicle_id = vehicle_id

        self.controller = NetworkController(
            vehicle_id=vehicle_id,
            pc_ip=pc_ip,
            pc_port=pc_port,
            listen_port=listen_port
        )

        self.running = False
        self.cycle_count = 0
        self.offline_block_start = -1
        self.offline_block_length = 0

        signal.signal(signal.SIGINT, self._signal_handler)

    def _signal_handler(self, sig, frame):
        print("\n\n收到中断信号，正在停止...")
        self.running = False

    def _generate_network_params(self, cycle: int):
        """
        模拟不同场景下的网络参数
        场景包括：网络质量良好、WiFi链路可用、蜂窝链路可用、无信号
        以及深坑、开阔区、弱网边坡等矿场场景
        """
        scene_type = random.random()

        if scene_type < 0.3:
            scene = "网络质量良好"
            rsrp = random.uniform(-85, -55)
            rssi = random.uniform(-55, -35)
            loss = random.uniform(0.2, 2.0)
        elif scene_type < 0.5:
            scene = "WiFi链路可用"
            rsrp = random.uniform(-120, -90)
            rssi = random.uniform(-60, -40)
            loss = random.uniform(1.0, 5.0)
        elif scene_type < 0.7:
            scene = "蜂窝链路可用"
            rsrp = random.uniform(-95, -70)
            rssi = random.uniform(-85, -70)
            loss = random.uniform(1.0, 5.0)
        elif scene_type < 0.8:
            scene = "深坑"
            rsrp = random.uniform(-130, -100)
            rssi = random.uniform(-95, -75)
            loss = random.uniform(5.0, 20.0)
        elif scene_type < 0.9:
            scene = "开阔区"
            rsrp = random.uniform(-90, -60)
            rssi = random.uniform(-65, -40)
            loss = random.uniform(0.5, 3.0)
        else:
            scene = "弱网边坡"
            rsrp = random.uniform(-115, -85)
            rssi = random.uniform(-80, -60)
            loss = random.uniform(3.0, 12.0)

        return rsrp, rssi, loss, scene

    def _start_offline_block(self, cycle: int, duration: int = 8):
        """开始一个离线（无信号）时间段"""
        self.offline_block_start = cycle
        self.offline_block_length = duration
        print(f"\n[场景模拟] 进入无信号区域，持续约 {duration} 个周期")

    def _is_in_offline_block(self, cycle: int) -> bool:
        if self.offline_block_start < 0:
            return False
        if cycle - self.offline_block_start < self.offline_block_length:
            return True
        self.offline_block_start = -1
        self.offline_block_length = 0
        return False

    def run_once(self, cycle_interval: float = 1.0,
                 min_batch: int = 1, max_batch: int = 5):
        """运行一个周期"""
        self.cycle_count += 1
        cycle = self.cycle_count

        print_separator("-", 70)
        print(f"[周期 {cycle}] 开始")

        if self._is_in_offline_block(cycle):
            rsrp = -140.0
            rssi = -100.0
            loss = 30.0
            scene = "无信号"
        else:
            if random.random() < 0.08 and self.offline_block_start < 0:
                self._start_offline_block(cycle, random.randint(5, 12))
                rsrp = -140.0
                rssi = -100.0
                loss = 30.0
                scene = "无信号"
            else:
                rsrp, rssi, loss, scene = self._generate_network_params(cycle)

        result = self.controller.run_cycle(rsrp, rssi, loss, min_batch, max_batch)

        if cycle % 10 == 0:
            self._print_stats()

        time.sleep(cycle_interval)
        return result

    def _print_stats(self):
        stats = self.controller.get_stats()
        print_separator("=", 70)
        print(f"[统计报告 - 周期 {self.cycle_count}]")
        print(f"  发送成功: {stats['sent_ok']}  (高优: {stats['high_priority_sent']})")
        print(f"  发送失败: {stats['sent_fail']}")
        print(f"  补发成功: {stats['resend_success']}")
        print(f"  补发失败: {stats['resend_fail']}")
        print(f"  L1缓存: {stats['l1_cache_size']} 条 (高优: {stats['l1_high_priority']})")
        print(f"  L2缓存: {stats['l2_cache_size']} 条 (高优: {stats['l2_high_priority']})")
        print(f"  累计入L1缓存: {stats['cached_l1']}")
        print(f"  链路切换次数: {stats['link_switches']}")
        print(f"  当前链路: {stats['current_link']}")
        print(f"  当前场景: {stats['scene']}")
        print(f"  网络可用: {'是' if stats['network_available'] else '否'}")
        print_separator("=", 70)

    def run_forever(self, cycle_interval: float = 1.0,
                    min_batch: int = 1, max_batch: int = 5,
                    background_resend_interval: float = 5.0):
        """常驻运行模式"""
        print_separator("=", 70)
        print("  自适应动态组包 + 改进LRU分级缓存 + 模糊控制链路切换")
        print("  主程序 - 常驻运行模式")
        print_separator("=", 70)
        print(f"  车辆ID: {self.vehicle_id}")
        print(f"  目标PC: {self.pc_ip}:{self.pc_port}")
        print(f"  监听端口: {self.listen_port}")
        print(f"  数据生成: 每周期 {min_batch}-{max_batch} 条")
        print(f"  周期间隔: {cycle_interval} 秒")
        print(f"  后台补发间隔: {background_resend_interval} 秒")
        print_separator("=", 70)
        print("  按 Ctrl+C 停止程序")
        print_separator("=", 70)

        self.controller.start_background_resend(background_resend_interval)
        self.running = True

        try:
            while self.running:
                self.run_once(cycle_interval, min_batch, max_batch)
        except KeyboardInterrupt:
            pass
        finally:
            self._shutdown()

    def _shutdown(self):
        print("\n")
        print_separator("=", 70)
        print("  程序停止中...")
        self.controller.stop_background_resend()
        self._print_stats()
        print("  程序已停止")
        print_separator("=", 70)

    def run_test(self, total_cycles: int = 50, cycle_interval: float = 0.5,
                 min_batch: int = 1, max_batch: int = 5):
        """测试模式 - 运行指定周期数"""
        print_separator("=", 70)
        print("  自适应动态组包 + 改进LRU分级缓存 + 模糊控制链路切换")
        print("  测试模式")
        print_separator("=", 70)
        print(f"  车辆ID: {self.vehicle_id}")
        print(f"  目标PC: {self.pc_ip}:{self.pc_port}")
        print(f"  测试周期: {total_cycles}")
        print(f"  数据生成: 每周期 {min_batch}-{max_batch} 条")
        print_separator("=", 70)

        self.running = True
        try:
            for i in range(total_cycles):
                if not self.running:
                    break
                self.run_once(cycle_interval, min_batch, max_batch)
        except KeyboardInterrupt:
            pass
        finally:
            self._shutdown()


def main():
    PC_IP = "192.168.1.3"
    PC_PORT = 8080
    LISTEN_PORT = 8889
    VEHICLE_ID = "TRUCK-001"

    if len(sys.argv) > 1:
        mode = sys.argv[1]
    else:
        mode = "forever"

    simulator = MainSimulator(
        pc_ip=PC_IP,
        pc_port=PC_PORT,
        listen_port=LISTEN_PORT,
        vehicle_id=VEHICLE_ID
    )

    if mode == "test":
        cycles = int(sys.argv[2]) if len(sys.argv) > 2 else 50
        simulator.run_test(total_cycles=cycles, cycle_interval=0.5,
                           min_batch=1, max_batch=5)
    else:
        simulator.run_forever(cycle_interval=1.0, min_batch=1, max_batch=5,
                              background_resend_interval=5.0)


if __name__ == "__main__":
    main()
