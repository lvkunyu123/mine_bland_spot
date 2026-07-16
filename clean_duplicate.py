#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库脏数据清理工具
功能：清理 mine_data.db 中 reports 表的重复记录，保留每组(vehicle_id, seq)最早入库的一条
用法：
    python clean_duplicate.py              # 清理并打印统计
    python clean_duplicate.py --dry-run    # 仅预览，不实际删除
"""
import sqlite3
import sys
import argparse

DB_PATH = "mine_data.db"


def get_stats(conn):
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM reports')
    total = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM (SELECT DISTINCT vehicle_id, seq FROM reports)')
    distinct = cursor.fetchone()[0]
    cursor.execute('''
        SELECT COUNT(*) FROM (
            SELECT vehicle_id, seq FROM reports
            GROUP BY vehicle_id, seq HAVING COUNT(*) > 1
        )
    ''')
    dup_groups = cursor.fetchone()[0]
    return total, distinct, dup_groups


def preview_duplicates(conn, limit=20):
    cursor = conn.cursor()
    cursor.execute('''
        SELECT vehicle_id, seq, COUNT(*) as cnt, MIN(id), MAX(id)
        FROM reports
        GROUP BY vehicle_id, seq
        HAVING cnt > 1
        ORDER BY cnt DESC
        LIMIT ?
    ''', (limit,))
    return cursor.fetchall()


def clean_duplicates(conn):
    cursor = conn.cursor()
    # 保留每组(vehicle_id, seq)中id最小（最早入库）的记录
    cursor.execute('''
        DELETE FROM reports WHERE id NOT IN (
            SELECT MIN(id) FROM reports GROUP BY vehicle_id, seq
        )
    ''')
    removed = cursor.rowcount
    conn.commit()
    return removed


def create_unique_index(conn):
    cursor = conn.cursor()
    try:
        cursor.execute('''
            CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_vehicle_seq
            ON reports(vehicle_id, seq)
        ''')
        conn.commit()
        return True, "唯一索引创建成功"
    except sqlite3.Error as e:
        conn.rollback()
        return False, str(e)


def main():
    parser = argparse.ArgumentParser(description='清理数据库重复记录')
    parser.add_argument('--dry-run', action='store_true', help='仅预览，不删除')
    args = parser.parse_args()

    print("=" * 60)
    print("  数据库脏数据清理工具")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)

    total_before, distinct_before, dup_groups_before = get_stats(conn)
    print(f"\n清理前:")
    print(f"  总记录数: {total_before}")
    print(f"  唯一记录: {distinct_before}")
    print(f"  重复组数: {dup_groups_before}")
    print(f"  预计可删除: {total_before - distinct_before}")

    if dup_groups_before > 0:
        print(f"\n重复数据示例（前10条）:")
        for row in preview_duplicates(conn, 10):
            print(f"  vehicle={row[0]}, seq={row[1]}, 重复{row[2]}次, id范围[{row[3]}-{row[4]}]")

    if args.dry_run:
        print("\n[DRY-RUN] 未实际删除数据")
        conn.close()
        return

    if total_before == distinct_before:
        print("\n无需清理，数据库中没有重复数据")
    else:
        removed = clean_duplicates(conn)
        total_after, distinct_after, dup_groups_after = get_stats(conn)
        print(f"\n已删除 {removed} 条重复记录")
        print(f"清理后:")
        print(f"  总记录数: {total_after}")
        print(f"  唯一记录: {distinct_after}")
        print(f"  重复组数: {dup_groups_after}")

    # 创建唯一索引，防止未来重复
    ok, msg = create_unique_index(conn)
    print(f"\n唯一索引: {'成功' if ok else '失败'} - {msg}")

    conn.close()
    print("\n清理完成")


if __name__ == "__main__":
    main()
