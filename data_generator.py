#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import time
import random
from typing import Dict, List, Optional

SCENE_TYPES = ["网络质量良好", "WiFi链路可用", "蜂窝链路可用", "无信号", "深坑", "开阔区", "弱网边坡"]

class DataGenerator:
    def __init__(self, vehicle_id: str = "A", start_seq: int = 100):
        self.vehicle_id = vehicle_id
        self.seq_counter = start_seq

    def generate_one(self, scene: str = "网络质量良好", priority: Optional[int] = None) -> Dict:
        self.seq_counter += 1
        if priority is None:
            priority = 1 if random.random() < 0.15 else 0

        if priority == 1:
            alarm_type = random.choice(["发动机过热", "超载告警", "液压系统故障",
                                        "制动系统异常", "胎压异常"])
            data_content = {"alarm_type": alarm_type}
            if alarm_type == "发动机过热":
                data_content["temperature"] = random.randint(95, 140)
            elif alarm_type == "超载告警":
                data_content["load_weight"] = round(random.uniform(80, 120), 1)
            elif alarm_type == "液压系统故障":
                data_content["pressure"] = round(random.uniform(5, 15), 1)
            elif alarm_type == "制动系统异常":
                data_content["brake_temp"] = random.randint(200, 350)
            else:
                data_content["tire_pressure"] = round(random.uniform(0.5, 2.5), 1)
        else:
            data_content = {
                "gps": {"lat": round(random.uniform(39, 41), 6),
                        "lng": round(random.uniform(115, 117), 6)},
                "fuel_consumption": round(random.uniform(20, 35), 2),
                "speed": round(random.uniform(0, 60), 1),
                "engine_rpm": random.randint(800, 2500),
                "battery_voltage": round(random.uniform(23.5, 28.0), 1)
            }

        return {
            "vehicle_id": self.vehicle_id,
            "timestamp": time.time(),
            "seq": self.seq_counter,
            "priority": priority,
            "scene": scene,
            "data": data_content
        }

    def generate_batch(self, count: int, scene: str = "网络质量良好") -> List[Dict]:
        return [self.generate_one(scene) for _ in range(count)]

    def generate_random_batch(self, min_count: int = 1, max_count: int = 10,
                              scene: str = "网络质量良好") -> List[Dict]:
        count = random.randint(min_count, max_count)
        return self.generate_batch(count, scene)