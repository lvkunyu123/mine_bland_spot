#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
自适应动态组包模块

功能：
    1. 动态组包大小（PacketSize）：根据网络质量Q_score，通过sigmoid平滑映射
    2. 动态发送间隔（Interval）：与网络质量负相关
    3. 动态重传次数（RetryCount）：与网络质量负相关，高优数据强制3次

计算公式（来自报告）：
    PacketSize = 最小包 + (最大包 - 最小包) × sigmoid(Q_score)
    Interval = Interval_max - (Interval_max - Interval_min) × Q_score
    RetryCount = Retry_max - (Retry_max - Retry_min) × Q_score

其中：
    Q_score = LQ / 100.0  （从模糊控制引擎的LQ值转换，范围[0,1]）
    sigmoid(x) = 1 / (1 + exp(-k*(x - x0)))
    k=8, x0=0.5
"""

import math
from typing import Dict, Tuple


class AdaptivePacketController:
    """自适应动态组包控制器"""

    def __init__(self):
        # 组包大小参数
        self.packet_min_bytes = 256      # 最小包大小（字节）
        self.packet_max_bytes = 4096     # 最大包大小（字节）
        self.sigmoid_k = 8               # sigmoid斜率
        self.sigmoid_x0 = 0.5            # sigmoid中心点

        # 发送间隔参数
        self.interval_min_ms = 50        # 最小发送间隔（毫秒）
        self.interval_max_ms = 2000      # 最大发送间隔（毫秒）

        # 重传次数参数
        self.retry_min = 1               # 最少重传次数
        self.retry_max = 3               # 最多重传次数
        self.high_priority_retries = 3   # 高优数据固定重传次数

        # 离线阈值（Q_score低于此值不组包，直接缓存）
        self.offline_threshold = 0.15

        # 当前状态
        self.current_q_score = 0.5
        self.current_packet_size = self.packet_min_bytes
        self.current_interval_ms = self.interval_min_ms
        self.current_retries = 2

    @staticmethod
    def sigmoid(x: float, k: float = 8, x0: float = 0.5) -> float:
        """sigmoid函数：f(x) = 1 / (1 + exp(-k*(x-x0)))

        特性：
            - f(0.5) = 0.5
            - f(0) ≈ 0.018
            - f(1) ≈ 0.982
            - 在x0附近变化最敏感，两端趋于平缓
        """
        try:
            return 1.0 / (1.0 + math.exp(-k * (x - x0)))
        except OverflowError:
            return 0.0 if x < x0 else 1.0

    def lq_to_q_score(self, lq: float) -> float:
        """将模糊引擎的LQ值[0,100]转换为Q_score[0,1]"""
        return max(0.0, min(1.0, lq / 100.0))

    def calc_packet_size(self, q_score: float) -> int:
        """计算动态组包大小

        PacketSize = 最小包 + (最大包 - 最小包) × sigmoid(Q_score)

        Args:
            q_score: 网络质量评分[0,1]

        Returns:
            组包大小（字节）
        """
        if q_score < self.offline_threshold:
            return self.packet_min_bytes

        s = self.sigmoid(q_score, self.sigmoid_k, self.sigmoid_x0)
        size = self.packet_min_bytes + (self.packet_max_bytes - self.packet_min_bytes) * s
        return int(round(size))

    def calc_interval(self, q_score: float) -> float:
        """计算动态发送间隔

        Interval = Interval_max - (Interval_max - Interval_min) × Q_score

        Args:
            q_score: 网络质量评分[0,1]

        Returns:
            发送间隔（秒）
        """
        if q_score < self.offline_threshold:
            return self.interval_max_ms / 1000.0

        interval_ms = self.interval_max_ms - (self.interval_max_ms - self.interval_min_ms) * q_score
        return interval_ms / 1000.0

    def calc_retries(self, q_score: float, priority: int = 0) -> int:
        """计算动态重传次数

        高优数据固定重传high_priority_retries次
        常规数据：RetryCount = Retry_max - (Retry_max - Retry_min) × Q_score

        Args:
            q_score: 网络质量评分[0,1]
            priority: 优先级（1=高优，0=常规）

        Returns:
            最大重传次数
        """
        if priority == 1:
            return self.high_priority_retries

        if q_score < self.offline_threshold:
            return self.retry_max

        retries = self.retry_max - (self.retry_max - self.retry_min) * q_score
        retries = int(round(retries))
        return max(self.retry_min, min(self.retry_max, retries))

    def update(self, lq: float) -> Dict:
        """更新网络质量，重新计算所有动态参数

        Args:
            lq: 模糊控制引擎输出的LQ值[0,100]

        Returns:
            包含所有参数的字典
        """
        q_score = self.lq_to_q_score(lq)
        self.current_q_score = q_score

        is_offline = q_score < self.offline_threshold

        self.current_packet_size = self.calc_packet_size(q_score)
        self.current_interval_ms = int(self.calc_interval(q_score) * 1000)
        self.current_retries = self.calc_retries(q_score, priority=0)

        return {
            'q_score': q_score,
            'lq': lq,
            'packet_size': self.current_packet_size,
            'interval_ms': self.current_interval_ms,
            'interval_sec': self.current_interval_ms / 1000.0,
            'retries_regular': self.current_retries,
            'retries_high': self.high_priority_retries,
            'is_offline': is_offline,
            'state': self._get_state(q_score)
        }

    def _get_state(self, q_score: float) -> str:
        """根据Q_score获取工作状态

        状态对应报告中的四状态：
            - STATE_ONLINE_DUAL (双链路可用): Q_score > 0.65
            - STATE_ONLINE_SINGLE (单链路可用): 0.45 ≤ Q_score ≤ 0.65
            - STATE_WEAK_NET (弱网环境): 0.15 ≤ Q_score ≤ 0.45
            - STATE_OFFLINE (完全离线): Q_score < 0.15
        """
        if q_score < 0.15:
            return "STATE_OFFLINE"
        elif q_score <= 0.45:
            return "STATE_WEAK_NET"
        elif q_score <= 0.65:
            return "STATE_ONLINE_SINGLE"
        else:
            return "STATE_ONLINE_DUAL"

    def get_params(self) -> Dict:
        """获取当前参数"""
        return {
            'q_score': self.current_q_score,
            'packet_size': self.current_packet_size,
            'interval_ms': self.current_interval_ms,
            'interval_sec': self.current_interval_ms / 1000.0,
            'retries_regular': self.current_retries,
            'retries_high': self.high_priority_retries,
            'state': self._get_state(self.current_q_score)
        }

    def batch_data_to_packets(self, data_list: list, q_score: float = None) -> list:
        """将数据列表按动态组包大小分成数据包

        Args:
            data_list: 待发送数据列表
            q_score: 网络质量评分，不传则使用当前值

        Returns:
            分组后的数据包列表，每个包是多条数据的列表
        """
        if q_score is None:
            q_score = self.current_q_score

        if q_score < self.offline_threshold:
            return [[d] for d in data_list]

        packet_size_bytes = self.calc_packet_size(q_score)

        packets = []
        current_packet = []
        current_size = 0

        import json
        for data in data_list:
            data_bytes = len(json.dumps(data, ensure_ascii=False).encode('utf-8'))
            if current_size + data_bytes > packet_size_bytes and current_packet:
                packets.append(current_packet)
                current_packet = [data]
                current_size = data_bytes
            else:
                current_packet.append(data)
                current_size += data_bytes

        if current_packet:
            packets.append(current_packet)

        return packets
