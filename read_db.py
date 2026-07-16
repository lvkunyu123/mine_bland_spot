#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
读取 mine_data.db 中的 reports 表，输出 JSON Lines 格式（每行一条完整 JSON）
用法：
    python read_db.py [输出文件名] [--stats] [--dedup]
参数：
    --stats    仅打印统计信息，不输出数据
    --dedup    输出时按(vehicle_id, seq)去重（保留第一条）
若不指定文件名，则打印到屏幕。
"""

import sys
import sqlite3
import json
import argparse

DB_PATH = "mine_data.db"  # 默认数据库文件，可修改


def get_stats(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM reports")
    total = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM (SELECT DISTINCT vehicle_id, seq FROM reports)")
    distinct = cursor.fetchone()[0]
    cursor.execute('''
        SELECT COUNT(*) FROM (
            SELECT vehicle_id, seq FROM reports
            GROUP BY vehicle_id, seq HAVING COUNT(*) > 1
        )
    ''')
    dup_groups = cursor.fetchone()[0]
    return total, distinct, dup_groups


def main():
    parser = argparse.ArgumentParser(description='读取 mine_data.db')
    parser.add_argument('output_file', nargs='?', help='输出文件名（可选）')
    parser.add_argument('--stats', action='store_true', help='仅打印统计信息')
    parser.add_argument('--dedup', action='store_true', help='输出时按vehicle_id+seq去重')
    args = parser.parse_args()

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
    except sqlite3.Error as e:
        print(f"无法打开数据库文件 '{DB_PATH}': {e}", file=sys.stderr)
        return

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='reports'")
    if not cursor.fetchone():
        print("数据库中不存在 'reports' 表", file=sys.stderr)
        conn.close()
        return

    total, distinct, dup_groups = get_stats(conn)
    print(f"共 {total} 条记录，唯一(vehicle_id, seq) {distinct} 组，重复组数 {dup_groups}",
          file=sys.stderr)

    if args.stats:
        if dup_groups > 0:
            print(f"存在 {total - distinct} 条重复记录，建议运行 python clean_duplicate.py 清理",
                  file=sys.stderr)
        conn.close()
        return

    # 获取列名
    cursor.execute("PRAGMA table_info(reports)")
    columns = [col[1] for col in cursor.fetchall()]

    # 读取全部记录（按 seq 排序，id升序保证最早入库在前）
    cursor.execute("SELECT * FROM reports ORDER BY seq ASC, id ASC")
    rows = cursor.fetchall()

    out = open(args.output_file, 'w', encoding='utf-8') if args.output_file else sys.stdout
    seen = set()
    output_count = 0
    skipped = 0

    try:
        for row in rows:
            record = {}
            for col_name, value in zip(columns, row):
                if col_name == "data":
                    try:
                        record[col_name] = json.loads(value)
                    except (json.JSONDecodeError, TypeError):
                        record[col_name] = value
                else:
                    record[col_name] = value

            key = (record.get('vehicle_id'), record.get('seq'))
            if args.dedup:
                if key in seen:
                    skipped += 1
                    continue
                seen.add(key)

            out.write(json.dumps(record, ensure_ascii=False) + '\n')
            output_count += 1
    except Exception as e:
        print(f"输出错误: {e}", file=sys.stderr)
    finally:
        if args.output_file:
            out.close()

    conn.close()
    if args.dedup:
        print(f"去重后输出 {output_count} 条，跳过 {skipped} 条重复",
              file=sys.stderr)
    else:
        print(f"完成，共输出 {output_count} 条记录", file=sys.stderr)


if __name__ == "__main__":
    main()
