"""
PC调度后台全系列算法
运行环境：笔记本电脑 PyCharm / Python 3.8+
功能模块：
  1. 基础数据处理 - 实时接收、归档入库、序列号缺项检测
  2. ARMA时序预测 - 预测上报间隔，提前发现断档风险
  3. 分级错峰补发 - 按优先级分时调度补发任务（测试模式取消时间限制）
  4. 滑动窗口脏数据剔除 - 去重、丢弃过期与乱序数据
修复：
  - 使用线程锁保护所有数据库操作，解决 Recursive use of cursors not allowed
  - 每次数据库操作创建独立cursor，避免多线程cursor冲突
  - 参数类型严格转换，解决 Error binding parameter 0
  - ARMA训练放到独立线程，避免阻塞数据处理
  - 优化异常处理和连接管理
"""
import socket
import json
import sqlite3
import time
import threading
import warnings
from collections import defaultdict, deque
import heapq
import numpy as np

warnings.filterwarnings('ignore')

# ==================== 全局配置 ====================
LISTEN_HOST = '0.0.0.0'
LISTEN_PORT = 8080
DB_PATH = 'mine_data.db'

ARMA_WINDOW = 100
ARMA_UPDATE_INTERVAL = 20
ARMA_ANOMALY_THRESHOLD = 1.5
ARMA_CONSECUTIVE_LIMIT = 3

PEAK_START = 8
PEAK_END = 20
HIGH_PRIORITY_LIMIT = 10
LOW_PRIORITY_LIMIT = 50
SCHEDULE_INTERVAL = 2

SLIDING_WINDOW_SIZE = 1000
UNICAST_PORT = 8889


class DispatchServer:
    def __init__(self):
        # 使用独立连接 + 线程锁，彻底解决多线程SQLite冲突
        self.conn = sqlite3.connect(DB_PATH, check_same_thread=False, isolation_level=None)
        self.db_lock = threading.Lock()
        self._init_db()

        # 基础数据处理
        self.last_seq = defaultdict(lambda: -1)
        self.received_seqs = defaultdict(set)
        self.missing_seqs = defaultdict(set)

        # 数据处理锁（保护内存数据结构）
        self.data_lock = threading.Lock()

        # ARMA 预测
        self.interval_history = defaultdict(lambda: deque(maxlen=ARMA_WINDOW))
        self.arma_models = {}
        self.receive_count = defaultdict(int)
        self.consecutive_anomalies = defaultdict(int)
        self.arma_lock = threading.Lock()

        # 补发队列
        self.high_priority_queue = []
        self.low_priority_queue = []
        self.pending_resend = set()
        self.queue_lock = threading.Lock()

        # 滑动窗口
        self.sliding_windows = defaultdict(lambda: [0, SLIDING_WINDOW_SIZE - 1])

        # 车辆IP记录
        self.vehicle_ips = {}
        self.ip_lock = threading.Lock()

        # 时间戳内存缓存
        self._last_ts = {}

        # 启动补发调度线程（无时间限制）
        threading.Thread(target=self._schedule_loop, daemon=True).start()

    # ==================== 数据库操作（线程安全） ====================
    def _execute_sql(self, sql, params=None, fetch=False):
        """线程安全的SQL执行，每次创建独立cursor"""
        with self.db_lock:
            cursor = self.conn.cursor()
            try:
                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)
                if fetch:
                    result = cursor.fetchall()
                    cursor.close()
                    return result
                cursor.close()
                return None
            except Exception as e:
                cursor.close()
                raise e

    def _init_db(self):
        with self.db_lock:
            cursor = self.conn.cursor()
            cursor.execute('''CREATE TABLE IF NOT EXISTS reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                vehicle_id TEXT,
                timestamp REAL,
                seq INTEGER,
                priority INTEGER,
                scene TEXT,
                data TEXT,
                received_at REAL
            )''')
            cursor.execute('''CREATE INDEX IF NOT EXISTS idx_vehicle_seq 
                                    ON reports(vehicle_id, seq)''')
            # 尝试创建唯一索引，若已存在脏数据则先清理
            cursor.execute('''SELECT COUNT(*) FROM (
                SELECT vehicle_id, seq FROM reports
                GROUP BY vehicle_id, seq HAVING COUNT(*) > 1
            )''')
            dup_count = cursor.fetchone()[0]
            if dup_count > 0:
                print(f"[数据库] 检测到 {dup_count} 组重复(vehicle_id, seq)，将在初始化时清理")
                self._cleanup_duplicates_locked(cursor)
            try:
                cursor.execute('''CREATE UNIQUE INDEX IF NOT EXISTS 
                                          idx_unique_vehicle_seq ON reports(vehicle_id, seq)''')
                print("[数据库] 唯一索引 idx_unique_vehicle_seq 创建/已存在")
            except sqlite3.Error as e:
                print(f"[数据库] 创建唯一索引失败（可能仍有脏数据）: {e}")
            cursor.close()

    def _cleanup_duplicates_locked(self, cursor):
        """在持有db_lock的前提下清理重复数据，保留最早入库的记录"""
        cursor.execute('''DELETE FROM reports WHERE id NOT IN (
            SELECT MIN(id) FROM reports GROUP BY vehicle_id, seq
        )''')
        removed = cursor.rowcount
        print(f"[数据库清理] 已删除 {removed} 条重复记录，保留每组最早的一条")

    def _is_seq_exists(self, vid, seq):
        """查询数据库中是否已存在该(vehicle_id, seq)记录"""
        result = self._execute_sql(
            'SELECT 1 FROM reports WHERE vehicle_id = ? AND seq = ? LIMIT 1',
            (vid, seq), fetch=True
        )
        return bool(result)

    # ==================== 1. 基础数据处理 ====================
    def process_incoming_packet(self, packet):
        vid = str(packet.get('vehicle_id', ''))
        seq = int(packet.get('seq', 0))
        priority = int(packet.get('priority', 0))
        scene = str(packet.get('scene', ''))
        timestamp = float(packet.get('timestamp', time.time()))

        # ---------- 严格去重：入库前查询数据库 ----------
        if self._is_seq_exists(vid, seq):
            print(f"[去重丢弃] {vid} seq={seq} 数据库中已存在")
            return False

        if not self._validate_seq(vid, seq):
            print(f"[去重丢弃] {vid} seq={seq} 不在窗口内或重复")
            return False

        # 入库（使用 INSERT OR IGNORE 兜底，配合唯一索引）
        data_json = json.dumps(packet.get('data', {}), ensure_ascii=False)
        try:
            self._execute_sql(
                'INSERT OR IGNORE INTO reports (vehicle_id, timestamp, seq, priority, scene, data, received_at) VALUES (?,?,?,?,?,?,?)',
                (vid, timestamp, seq, priority, scene, data_json, time.time())
            )
        except Exception as e:
            print(f"[入库失败] {vid} seq={seq}: {e}")
            return False

        # 再次确认是否真正入库（唯一索引可能IGNORE）
        if not self._is_seq_exists(vid, seq):
            print(f"[去重丢弃] {vid} seq={seq} 唯一索引拦截，未实际入库")
            return False

        with self.data_lock:
            self.received_seqs[vid].add(seq)
            self._advance_window(vid)

            # ---------- 序列号缺项检测 ----------
            last = self.last_seq[vid]
            if last == -1:
                self.last_seq[vid] = seq
            else:
                if seq > last + 1:
                    for missing in range(last + 1, seq):
                        print(f"[缺号检测] {vid} 缺失 seq={missing}，加入补发队列")
                        self.missing_seqs[vid].add(missing)
                        self._add_resend_task(vid, missing, priority)
                self.last_seq[vid] = max(last, seq)
            self.missing_seqs[vid].discard(seq)

            # ---------- ARMA 数据供给 ----------
            last_ts = self._last_ts.get(vid, 0)
            if last_ts != 0 and timestamp > last_ts:
                interval = timestamp - last_ts
                if interval > 0:
                    self.interval_history[vid].append(interval)
                    self.receive_count[vid] += 1
                    need_train = (len(self.interval_history[vid]) >= 20 and
                                  self.receive_count[vid] % ARMA_UPDATE_INTERVAL == 0)
            else:
                need_train = False
            self._last_ts[vid] = timestamp

        # ARMA训练放到独立线程，避免阻塞
        if need_train:
            threading.Thread(target=self._train_arma_safe, args=(vid,), daemon=True).start()

        # 检查异常
        if self.check_anomaly(vid):
            self._send_lock_command(vid)

        return True

    def _add_resend_task(self, vid, seq, priority):
        with self.queue_lock:
            if (vid, seq) in self.pending_resend:
                return
            self.pending_resend.add((vid, seq))
            task = (time.time(), vid, seq, priority)
            if priority == 1:
                heapq.heappush(self.high_priority_queue, task)
                print(f"[缺号入队] {vid} seq={seq} 加入高优队列")
            else:
                heapq.heappush(self.low_priority_queue, task)
                print(f"[缺号入队] {vid} seq={seq} 加入低优队列")

    # ==================== 2. ARMA 时序预测 ====================
    def _train_arma_safe(self, vid):
        """ARMA训练的安全包装，避免阻塞主流程"""
        try:
            self._train_arma(vid)
        except Exception as e:
            print(f"[ARMA] {vid} 训练异常: {e}")

    def _train_arma(self, vid):
        with self.arma_lock:
            intervals = list(self.interval_history[vid])
        if len(intervals) < 20:
            return
        try:
            from statsmodels.tsa.arima.model import ARIMA
            best_aic = np.inf
            best_order = (1, 0, 1)
            best_model = None
            for p in range(1, 4):
                for q in range(1, 4):
                    try:
                        model = ARIMA(intervals, order=(p, 0, q))
                        fitted = model.fit()
                        if fitted.aic < best_aic:
                            best_aic = fitted.aic
                            best_order = (p, 0, q)
                            best_model = fitted
                    except:
                        continue
            if best_model is None:
                # 降级：用最简单的(1,0,1)
                try:
                    model = ARIMA(intervals, order=(1, 0, 1))
                    best_model = model.fit()
                    best_aic = best_model.aic
                except:
                    return
            with self.arma_lock:
                self.arma_models[vid] = best_model
            print(f"[ARMA] {vid} 模型更新，阶数={best_order}, AIC={best_aic:.2f}")
        except Exception as e:
            print(f"[ARMA] {vid} 训练失败: {e}")

    def check_anomaly(self, vid):
        with self.arma_lock:
            if vid not in self.arma_models:
                return False
            intervals = list(self.interval_history[vid])
            model = self.arma_models[vid]
        if len(intervals) < 5:
            return False
        latest_interval = intervals[-1]
        try:
            forecast = model.forecast(steps=1)
            pred = forecast[0]
            conf = model.get_forecast(steps=1).conf_int()
            std_est = (conf[0][1] - pred) / 1.96
            threshold = pred + ARMA_ANOMALY_THRESHOLD * std_est
            if latest_interval > threshold and latest_interval > pred:
                self.consecutive_anomalies[vid] += 1
            else:
                self.consecutive_anomalies[vid] = 0
            if self.consecutive_anomalies[vid] >= ARMA_CONSECUTIVE_LIMIT:
                self.consecutive_anomalies[vid] = 0
                for _ in range(ARMA_CONSECUTIVE_LIMIT):
                    try:
                        self.interval_history[vid].pop()
                    except IndexError:
                        break
                return True
        except:
            pass
        return False

    def _send_lock_command(self, vid):
        target_ip = self._get_vehicle_ip(vid)
        if not target_ip:
            return
        command = {"cmd": "lock_high_priority", "timestamp": time.time()}
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            sock.connect((target_ip, UNICAST_PORT))
            sock.sendall(json.dumps(command).encode())
            sock.close()
            print(f"[预警] 已向 {vid} 发送锁定高优数据指令")
        except Exception as e:
            print(f"[预警] 发送锁定指令失败: {e}")

    def _get_vehicle_ip(self, vid):
        with self.ip_lock:
            return self.vehicle_ips.get(vid, None)

    # ==================== 3. 分级错峰补发调度（无时间限制） ====================
    def _schedule_loop(self):
        print("[调度] 补发调度线程已启动（测试模式：无时间限制）")
        while True:
            try:
                self._process_queue(self.high_priority_queue, HIGH_PRIORITY_LIMIT)
                self._process_queue(self.low_priority_queue, LOW_PRIORITY_LIMIT)
            except Exception as e:
                print(f"[调度] 异常: {e}")
            time.sleep(SCHEDULE_INTERVAL)

    def _process_queue(self, queue, limit):
        processed = 0
        while queue and processed < limit:
            with self.queue_lock:
                if not queue:
                    break
                create_time, vid, seq, priority = heapq.heappop(queue)

            with self.data_lock:
                if seq not in self.missing_seqs.get(vid, set()):
                    with self.queue_lock:
                        self.pending_resend.discard((vid, seq))
                    continue

            target_ip = self._get_vehicle_ip(vid)
            if target_ip:
                resend_cmd = {"cmd": "RESEND_CMD", "seq": seq}
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(2)
                    sock.connect((target_ip, UNICAST_PORT))
                    sock.sendall(json.dumps(resend_cmd).encode())
                    sock.close()
                    print(f"[补发指令] 已向 {vid} 发送补发 seq={seq}")
                except Exception as e:
                    print(f"[补发指令] 发送失败 {vid} seq={seq}: {e}")
            else:
                print(f"[补发指令] 未找到 {vid} 的IP，无法补发 seq={seq}")

            with self.queue_lock:
                self.pending_resend.discard((vid, seq))
            processed += 1

    # ==================== 4. 滑动窗口脏数据剔除 ====================
    def _validate_seq(self, vid, seq):
        """滑动窗口去重：丢弃过期和重复的数据包"""
        with self.data_lock:
            win = self.sliding_windows[vid]
            start, end = win[0], win[1]

            # 小于窗口左边界：过期数据
            if seq < start:
                return False

            # 已在窗口内且已接收：重复数据
            if seq <= end and seq in self.received_seqs[vid]:
                return False

            # 扩展窗口右边界
            if seq > end:
                win[1] = seq
                self.sliding_windows[vid] = win

            return True

    def _advance_window(self, vid):
        """调用方需已持有 data_lock"""
        win = self.sliding_windows[vid]
        start, end = win
        while start in self.received_seqs[vid]:
            self.received_seqs[vid].discard(start)
            start += 1
        win[0] = start
        win[1] = max(end, start + SLIDING_WINDOW_SIZE - 1)
        self.sliding_windows[vid] = win

    # ==================== 网络监听入口 ====================
    def start(self):
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind((LISTEN_HOST, LISTEN_PORT))
        server_sock.listen(5)
        print(f"调度后台启动，监听 {LISTEN_HOST}:{LISTEN_PORT} ...")
        while True:
            try:
                conn, addr = server_sock.accept()
                threading.Thread(target=self._handle_client, args=(conn, addr), daemon=True).start()
            except Exception as e:
                print(f"[监听] accept异常: {e}")

    def _handle_client(self, conn, addr):
        try:
            while True:
                data = conn.recv(4096)
                if data:
                    try:
                        packet = json.loads(data.decode())
                        vid = packet.get('vehicle_id')
                        if vid:
                            vid = str(vid)
                            with self.ip_lock:
                                self.vehicle_ips[vid] = addr[0]
                            ok = self.process_incoming_packet(packet)
                            if ok:
                                ack = json.dumps({"status": "ok", "type": "inserted"})
                            else:
                                ack = json.dumps({"status": "ok", "type": "duplicate"})
                            conn.sendall(ack.encode())
                    except json.JSONDecodeError:
                        print(f"[报文] JSON解析失败，来自 {addr[0]}")
                        try:
                            conn.sendall(json.dumps({"status": "error", "type": "bad_json"}).encode())
                        except:
                            pass
                    except Exception as e:
                        print(f"[报文] 处理异常: {e}")
                        try:
                            conn.sendall(json.dumps({"status": "error", "type": "server_error"}).encode())
                        except:
                            pass
                else:
                    break
        except (ConnectionResetError, ConnectionAbortedError, OSError) as e:
            pass  # 客户端断开，静默处理
        except Exception as e:
            print(f"[连接] 异常: {e}")
        finally:
            try:
                conn.close()
            except:
                pass


if __name__ == '__main__':
    server = DispatchServer()
    server.start()
