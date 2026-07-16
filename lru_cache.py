#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
改进的LRU分级缓存模块
- 第一缓存层（L1）：主缓存，存储待发送数据，带LRU淘汰
- 第二缓存层（L2）：补发失败后的数据，等待再次补发
- 高优先级数据绝对不淘汰
- 低优先级数据按综合得分score淘汰（得分越高越先淘汰）
- score计算：结合访问频率、数据年龄、优先级权重
"""
import time
import json
import threading
from typing import Dict, List, Optional, Tuple
from collections import OrderedDict


class ImprovedLRUNode:
    def __init__(self, data: Dict):
        self.data = data
        self.seq = data.get('seq')
        self.priority = data.get('priority', 0)
        self.access_count = 0
        self.last_access_time = time.time()
        self.create_time = time.time()
        self.retry_count = 0
        self.size = 0


class ImprovedLRUCache:
    def __init__(self, capacity_mb: float = 50.0, name: str = "L1"):
        self.capacity = int(capacity_mb * 1024 * 1024)
        self.cache: OrderedDict[int, ImprovedLRUNode] = OrderedDict()
        self.used = 0
        self.lock = threading.Lock()
        self.name = name

    def _calc_size(self, data: Dict) -> int:
        try:
            return len(json.dumps(data, ensure_ascii=False).encode('utf-8'))
        except:
            return 1024

    def _calc_score(self, node: ImprovedLRUNode, now: float) -> float:
        if node.priority == 1:
            return float('inf')

        age = now - node.create_time
        idle_time = now - node.last_access_time

        age_max = 3600.0
        age_norm = min(age / age_max, 1.0)
        idle_max = 1800.0
        idle_norm = min(idle_time / idle_max, 1.0)

        access_factor = 1.0 / (1.0 + node.access_count)

        score = (0.4 * age_norm + 0.4 * idle_norm + 0.2 * access_factor)
        return score

    def write(self, data: Dict) -> Tuple[bool, Optional[str]]:
        with self.lock:
            seq = data.get('seq')
            if seq is None:
                return False, "no_seq"

            size = self._calc_size(data)

            if seq in self.cache:
                old_node = self.cache[seq]
                old_size = old_node.size
                self.used += (size - old_size)
                old_node.data = data
                old_node.size = size
                old_node.last_access_time = time.time()
                old_node.access_count += 1
                self.cache.move_to_end(seq)
                return True, "updated"

            if self.used + size > self.capacity:
                freed = self._evict(size)
                if self.used + size > self.capacity:
                    return False, f"空间不足，释放{freed}字节后仍不够"

            node = ImprovedLRUNode(data)
            node.size = size
            self.cache[seq] = node
            self.used += size
            return True, None

    def _evict(self, needed: int) -> int:
        candidates = []
        now = time.time()
        for seq, node in self.cache.items():
            if node.priority == 0:
                score = self._calc_score(node, now)
                candidates.append((score, seq, node))
        candidates.sort(key=lambda x: x[0], reverse=True)

        freed = 0
        evicted_count = 0
        for score, seq, node in candidates:
            if self.used - freed <= self.capacity - needed:
                break
            sz = node.size
            del self.cache[seq]
            freed += sz
            evicted_count += 1

        self.used -= freed
        print(f"[{self.name}缓存淘汰: 淘汰{evicted_count}条低优数据，释放{freed}字节")
        return freed

    def read(self, seq: int) -> Optional[Dict]:
        with self.lock:
            if seq in self.cache:
                node = self.cache[seq]
                node.access_count += 1
                node.last_access_time = time.time()
                self.cache.move_to_end(seq)
                return node.data
            return None

    def delete(self, seq: int) -> bool:
        with self.lock:
            if seq in self.cache:
                node = self.cache[seq]
                self.used -= node.size
                del self.cache[seq]
                return True
            return False

    def get_all(self) -> List[Dict]:
        with self.lock:
            return [node.data for node in self.cache.values()]

    def get_all_sorted(self) -> List[Dict]:
        with self.lock:
            nodes = list(self.cache.values())
            nodes.sort(key=lambda n: (-n.priority, n.create_time))
            return [node.data for node in nodes]

    def is_empty(self) -> bool:
        with self.lock:
            return len(self.cache) == 0

    def size(self) -> int:
        with self.lock:
            return len(self.cache)

    def get_high_priority_count(self) -> int:
        with self.lock:
            return sum(1 for n in self.cache.values() if n.priority == 1)

    def clear(self):
        with self.lock:
            self.cache.clear()
            self.used = 0

    def increment_retry(self, seq: int):
        with self.lock:
            if seq in self.cache:
                self.cache[seq].retry_count += 1

    def get_retry_count(self, seq: int) -> int:
        with self.lock:
            if seq in self.cache:
                return self.cache[seq].retry_count
            return 0


class TwoLevelCache:
    def __init__(self, l1_capacity_mb: float = 50.0, l2_capacity_mb: float = 30.0):
        self.l1 = ImprovedLRUCache(l1_capacity_mb, "L1")
        self.l2 = ImprovedLRUCache(l2_capacity_mb, "L2")
        self.lock = threading.Lock()

    def add_to_l1(self, data: Dict) -> Tuple[bool, Optional[str]]:
        return self.l1.write(data)

    def add_to_l2(self, data: Dict) -> Tuple[bool, Optional[str]]:
        return self.l2.write(data)

    def move_to_l2(self, seq: int) -> bool:
        data = self.l1.read(seq)
        if data is None:
            return False
        self.l1.delete(seq)
        ok, _ = self.l2.write(data)
        if ok:
            print(f"[缓存迁移] seq={seq} 从L1移入L2")
        return ok

    def move_to_l1(self, seq: int) -> bool:
        data = self.l2.read(seq)
        if data is None:
            return False
        self.l2.delete(seq)
        ok, _ = self.l1.write(data)
        if ok:
            print(f"[缓存迁移] seq={seq} 从L2移入L1")
        return ok

    def get(self, seq: int) -> Optional[Dict]:
        data = self.l1.read(seq)
        if data is not None:
            return data
        data = self.l2.read(seq)
        if data is not None:
            return data
        return None

    def delete(self, seq: int) -> bool:
        if self.l1.delete(seq):
            return True
        if self.l2.delete(seq):
            return True
        return False

    def get_l1_all(self) -> List[Dict]:
        return self.l1.get_all_sorted()

    def get_l2_all(self) -> List[Dict]:
        return self.l2.get_all_sorted()

    def total_size(self) -> int:
        return self.l1.size() + self.l2.size()

    def l1_size(self) -> int:
        return self.l1.size()

    def l2_size(self) -> int:
        return self.l2.size()

    def clear_all(self):
        self.l1.clear()
        self.l2.clear()
