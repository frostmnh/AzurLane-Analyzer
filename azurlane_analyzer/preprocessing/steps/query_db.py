import sqlite3
import os # 確保導入 os 模塊 (雖然主要用 pathlib，但了解 __file__ 來源有幫助)
from pathlib import Path

# 1. 獲取當前腳本文件所在的絕對目錄路徑
#    Path(__file__)-> 獲取當前腳本的相對或絕對路徑 (取決於如何運行)
#    .resolve()    -> 將路徑轉換為絕對路徑，並解析任何符號鏈接 (更可靠)
#    .parent       -> 獲取包含該文件的父目錄
script_dir = Path(__file__).resolve().parent
print(f"腳本運行目錄: {script_dir}") # 打印出來方便確認

# 2. 定義 SQLite 數據庫文件的路徑
#    我們將數據庫文件直接放在與腳本相同的目錄下
DB_FILE = script_dir / 'azur_lane_data.db'
print(f"預期數據庫文件路徑: {DB_FILE}") # 打印出來方便確認


def query_db(db_path):
    """連接數據庫並執行一些查詢"""
    if not db_path.is_file():
        print(f"錯誤: 數據庫文件未找到 {db_path}")
        return

    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        print(f"成功連接到數據庫: {db_path}\n")

        # 示例查詢 1: 總行數
        cursor.execute("SELECT count(*) FROM equipment")
        total_rows = cursor.fetchone()[0] # fetchone() 返回一個元組，[0] 取第一個元素
        print(f"equipment 表總行數: {total_rows}")

        # 示例查詢 2: 有名稱的行數
        cursor.execute("SELECT count(*) FROM equipment WHERE name IS NOT NULL")
        named_rows = cursor.fetchone()[0]
        print(f"equipment 表中有名稱的行數: {named_rows}")

        # 示例查詢 3: 查看 ID 50000 的數據
        print("\n查詢 ID 50000 的數據:")
        cursor.execute("SELECT id, name FROM equipment WHERE id = ?", (50000,)) # 使用參數化查詢
        row = cursor.fetchone()
        if row:
            print(f"  ID: {row[0]}, Name: {row[1]}")
        else:
            print("  未找到 ID 50000 的數據")

        # 示例查詢 4: 查看前 5 行數據
        print("\n查看前 5 行 (ID 和 Name):")
        cursor.execute("SELECT id, name FROM equipment LIMIT 5")
        rows = cursor.fetchall() # fetchall() 返回所有結果行的列表
        for r in rows:
            print(f"  ID: {r[0]}, Name: {r[1]}")

    except sqlite3.Error as e:
        print(f"查詢數據庫時出錯: {e}")
    finally:
        if conn:
            conn.close()
            # print("\n數據庫連接已關閉。") # 可以取消註釋

if __name__ == '__main__':
    query_db(DB_FILE)