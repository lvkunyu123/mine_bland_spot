#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
模糊控制预判式链路切换算法 —— 核心模块

供其他模块直接调用，用法:
    from fuzzy_link_switch_core import FuzzyLinkDecisionEngine

    engine = FuzzyLinkDecisionEngine()
    result = engine.decide(rsrp=-95, rssi=-65, loss_rate=2.5)
    # result 为 dict, 包含 command / lq / state / reason 等字段
"""

import numpy as np
from collections import deque
from enum import Enum
from dataclasses import dataclass
from typing import Optional, List


# ==================== 基础隶属函数 ====================

def trimf(x, a, b, c):
    """三角形隶属函数"""
    if x <= a or x >= c:
        return 0.0
    elif a < x <= b:
        return (x - a) / (b - a) if b != a else 1.0
    else:
        return (c - x) / (c - b) if c != b else 1.0


def trapmf_l(x, a, b):
    """左梯形"""
    if x <= a:
        return 1.0
    elif x >= b:
        return 0.0
    else:
        return (b - x) / (b - a)


def trapmf_r(x, a, b):
    """右梯形"""
    if x >= b:
        return 1.0
    elif x <= a:
        return 0.0
    else:
        return (x - a) / (b - a)


# ==================== 输入输出隶属函数 ====================

def rsrp_mu(rsrp):
    """RSRP [-140,-44] dBm"""
    return {
        "VS": trapmf_r(rsrp, -85, -80),
        "S":  trimf(rsrp, -100, -90, -80),
        "M":  trimf(rsrp, -110, -100, -90),
        "W":  trimf(rsrp, -120, -110, -100),
        "VW": trapmf_l(rsrp, -120, -115),
    }


def rssi_mu(rssi):
    """RSSI [-100,-30] dBm"""
    return {
        "VS": trapmf_r(rssi, -40, -30),
        "S":  trimf(rssi, -55, -45, -35),
        "M":  trimf(rssi, -65, -55, -45),
        "W":  trimf(rssi, -75, -65, -55),
        "VW": trapmf_l(rssi, -75, -70),
    }


def loss_mu(loss):
    """丢包率 [0,30] %"""
    return {
        "NL": trapmf_l(loss, 0, 1),
        "LL": trimf(loss, 0, 1.5, 3),
        "ML": trimf(loss, 1, 4.5, 8),
        "HL": trimf(loss, 5, 10, 15),
        "PL": trapmf_r(loss, 10, 15),
    }


def output_mu(lq):
    """LQ输出 [0,100]"""
    return {
        "VG": trapmf_r(lq, 70, 85),
        "G":  trimf(lq, 40, 65, 90),
        "M":  trimf(lq, 20, 45, 70),
        "B":  trimf(lq, 5, 22, 40),
        "VB": trapmf_l(lq, 10, 20),
    }


# ==================== 规则库 (125条) ====================

SCORE = {"VS": 4, "S": 3, "M": 2, "W": 1, "VW": 0,
         "NL": 4, "LL": 3, "ML": 2, "HL": 1, "PL": 0}


def _is_bad(lv, vtype):
    if vtype in ("RSRP", "RSSI"):
        return lv in ("W", "VW")
    return lv in ("HL", "PL")


def _build_rules():
    rules = []
    for rv in ["VS", "S", "M", "W", "VW"]:
        for wi in ["VS", "S", "M", "W", "VW"]:
            for lo in ["NL", "LL", "ML", "HL", "PL"]:
                bc = sum([_is_bad(rv, "RSRP"), _is_bad(wi, "RSSI"), _is_bad(lo, "Loss")])
                if bc == 3:
                    out = "VB"
                elif bc == 2:
                    out = "B"
                elif bc == 1:
                    out = "M"
                else:
                    sc = 0.4 * (SCORE[rv] + SCORE[wi]) / 2 + 0.6 * SCORE[lo]
                    out = "VG" if sc >= 3.6 else "G" if sc >= 2.8 else "M" if sc >= 2.0 else "B"
                rules.append((rv, wi, lo, out))
    return rules


_RULES = _build_rules()


# ==================== Mamdani推理 ====================

def _mamdani_inference(rsrp, rssi, loss):
    """模糊推理，返回 (lq, detail_dict)"""
    rsrp = np.clip(rsrp, -140, -44)
    rssi = np.clip(rssi, -100, -30)
    loss = np.clip(loss, 0, 30)

    rsrp_f = rsrp_mu(rsrp)
    rssi_f = rssi_mu(rssi)
    loss_f = loss_mu(loss)

    agg = {"VG": 0.0, "G": 0.0, "M": 0.0, "B": 0.0, "VB": 0.0}
    activated = []
    for rv, wi, lo, out in _RULES:
        act = min(rsrp_f[rv], rssi_f[wi], loss_f[lo])
        if act > 0:
            activated.append((rv, wi, lo, out, act))
            if act > agg[out]:
                agg[out] = act

    universe = np.arange(0, 100.01, 0.1)
    num = den = 0.0
    for w in universe:
        mu_total = 0.0
        for lv, mav in agg.items():
            mc = min(mav, output_mu(w)[lv])
            if mc > mu_total:
                mu_total = mc
        num += w * mu_total
        den += mu_total

    lq = num / den if den > 1e-10 else 50.0
    lq = np.clip(lq, 0, 100)

    return lq, {
        'rsrp_f': rsrp_f, 'rssi_f': rssi_f, 'loss_f': loss_f,
        'activated': activated, 'aggregated': agg, 'lq': lq,
    }


# ==================== 趋势检测 ====================

class _TrendDetector:
    def __init__(self, size=10):
        self.window = deque(maxlen=size)

    def update(self, v):
        self.window.append(v)

    def slope(self):
        if len(self.window) < 3:
            return 0.0
        n = len(self.window)
        x, y = np.arange(n), np.array(self.window)
        xm, ym = x.mean(), y.mean()
        den = np.sum((x - xm) ** 2)
        if abs(den) < 1e-10:
            return 0.0
        return float(np.sum((x - xm) * (y - ym)) / den)

    def data(self):
        return list(self.window)


# ==================== 切换决策 ====================

class _SwitchAction(Enum):
    HOLD = "维持当前链路"
    PREPARE_SWITCH = "预切换准备"
    SWITCH_TO_WIFI = "切换到WiFi链路"
    SWITCH_TO_CELLULAR = "切换到蜂窝链路"
    CACHE_DATA = "双链路失效，写入缓存"


@dataclass
class _SwitchCommand:
    action: _SwitchAction
    lq: float
    slope: float
    reason: str
    current_link: str
    in_holdoff: bool
    blacklisted: bool


class _LinkSwitchDecision:
    def __init__(self):
        self.current_link = "CELLULAR"
        self.in_holdoff = False
        self.last_switch_time = 0.0
        self.holdoff_duration = 2.0
        self.blacklist = {}
        self.blacklist_duration = 6.0
        self.sample_idx = 0

    def _get_state(self, lq):
        if lq >= 75:
            return "优秀"
        elif lq >= 55:
            return "良好"
        elif lq >= 35:
            return "预警"
        elif lq >= 15:
            return "危险"
        else:
            return "失效"

    def _check_holdoff(self, timestamp):
        if self.in_holdoff:
            if timestamp - self.last_switch_time >= self.holdoff_duration:
                self.in_holdoff = False
                return False
            return True
        return False

    def _start_holdoff(self, timestamp):
        self.in_holdoff = True
        self.last_switch_time = timestamp

    def _is_blacklisted(self, link):
        if link in self.blacklist:
            if self.sample_idx * 0.1 < self.blacklist[link]:
                return True
            del self.blacklist[link]
        return False

    def decide(self, lq, slope, timestamp=0.0):
        self.sample_idx += 1
        state = self._get_state(lq)

        if self._check_holdoff(timestamp):
            return _SwitchCommand(
                _SwitchAction.HOLD, lq, slope,
                f"保护期内，维持当前链路", self.current_link, True, False
            ), state

        if state == "失效":
            return _SwitchCommand(
                _SwitchAction.CACHE_DATA, lq, slope,
                f"链路失效: LQ={lq:.2f} < 15", self.current_link, False, False
            ), state

        if lq < 35:
            target = "WIFI" if self.current_link == "CELLULAR" else "CELLULAR"
            if self._is_blacklisted(target):
                return _SwitchCommand(
                    _SwitchAction.HOLD, lq, slope,
                    f"{target}在黑名单中", self.current_link, False, True
                ), state
            self.current_link = target
            self._start_holdoff(timestamp)
            action = _SwitchAction.SWITCH_TO_WIFI if target == "WIFI" else _SwitchAction.SWITCH_TO_CELLULAR
            return _SwitchCommand(
                action, lq, slope,
                f"紧急切换: LQ={lq:.2f} < 35", self.current_link, False, False
            ), state

        if lq < 55 and slope < 0:
            target = "WIFI" if self.current_link == "CELLULAR" else "CELLULAR"
            if self._is_blacklisted(target):
                return _SwitchCommand(
                    _SwitchAction.HOLD, lq, slope,
                    f"{target}在黑名单中", self.current_link, False, True
                ), state
            self.current_link = target
            self._start_holdoff(timestamp)
            action = _SwitchAction.SWITCH_TO_WIFI if target == "WIFI" else _SwitchAction.SWITCH_TO_CELLULAR
            return _SwitchCommand(
                action, lq, slope,
                f"预判切换: LQ={lq:.2f} 且 slope={slope:.4f}<0", self.current_link, False, False
            ), state

        if lq < 55:
            return _SwitchCommand(
                _SwitchAction.PREPARE_SWITCH, lq, slope,
                f"预切换准备: LQ={lq:.2f}预警", self.current_link, False, False
            ), state

        return _SwitchCommand(
            _SwitchAction.HOLD, lq, slope,
            f"维持: LQ={lq:.2f} 状态={state}", self.current_link, False, False
        ), state


# ==================== 对外API类 ====================

class FuzzyLinkDecisionEngine:
    """
    模糊控制预判式链路切换决策引擎

    调用方式:
        engine = FuzzyLinkDecisionEngine()
        result = engine.decide(rsrp=-95, rssi=-65, loss_rate=2.5)

    result 字典字段:
        command      : str   "HOLD" / "PREPARE_SWITCH" / "SWITCH_TO_WIFI" /
                            "SWITCH_TO_CELLULAR" / "CACHE_DATA"
        command_desc : str   指令中文描述
        lq           : float 链路质量评分 [0,100]
        slope        : float 趋势斜率
        state        : str   优秀/良好/预警/危险/失效
        target_link  : str   目标链路 WIFI/CELLULAR
        current_link : str   当前链路
        in_holdoff   : bool  是否在保护期
        blacklisted  : bool  是否在黑名单
        reason       : str   决策原因
    """

    def __init__(self, holdoff_sec: float = 2.0, blacklist_sec: float = 6.0):
        self._decision = _LinkSwitchDecision()
        self._decision.holdoff_duration = holdoff_sec
        self._decision.blacklist_duration = blacklist_sec
        self._trend = _TrendDetector(10)
        self._sample_count = 0

    def decide(self, rsrp: float, rssi: float, loss_rate: float,
               current_link: str = "CELLULAR") -> dict:
        """
        执行一次模糊推理与切换决策

        参数:
            rsrp:         蜂窝信号(dBm) [-140,-44]
            rssi:         WiFi信号(dBm) [-100,-30]
            loss_rate:    丢包率(%) [0,30]
            current_link: 当前链路 "CELLULAR"/"WIFI"

        返回: dict 包含 command, lq, state, reason 等
        """
        lq, _ = _mamdani_inference(rsrp, rssi, loss_rate)
        self._trend.update(lq)
        slope = self._trend.slope()
        cmd, state = self._decision.decide(lq, slope, self._sample_count * 0.1)
        self._sample_count += 1

        if cmd.action in (_SwitchAction.SWITCH_TO_WIFI,):
            target = "WIFI"
        elif cmd.action in (_SwitchAction.SWITCH_TO_CELLULAR,):
            target = "CELLULAR"
        else:
            target = current_link

        result = {
            'lq': round(lq, 2),
            'slope': round(slope, 4) if slope is not None else None,
            'command': cmd.action.name,
            'command_desc': cmd.action.value,
            'target_link': target,
            'current_link': cmd.current_link,
            'state': state,
            'in_holdoff': cmd.in_holdoff,
            'blacklisted': cmd.blacklisted,
            'reason': cmd.reason,
        }
        self._last_result = result
        return result

    def get_last_lq(self) -> Optional[float]:
        """获取最近一次LQ值"""
        return self._last_result['lq'] if self._last_result else None

    def get_last_state(self) -> Optional[str]:
        """获取当前链路状态"""
        return self._last_result['state'] if self._last_result else None

    def reset(self):
        """重置引擎状态"""
        self._trend = _TrendDetector(10)
        self._decision = _LinkSwitchDecision()
        self._sample_count = 0
