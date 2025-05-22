# 位於: AzurLane-Analyzer/azurlane_analyzer/preprocessing/steps/process_weapon_name.py

import json
import sqlite3
import sys
from pathlib import Path # 仍然需要 Path 來處理路徑

# --- 配置 ---
# 定義此腳本負責處理的 JSON 文件名 (在 sharecfgdata 目錄下)
# !!! 請根據您的數據源確認這是否是包含裝備基礎 ID 和名稱的正確文件名 !!!
TARGET_JSON_FILENAME = 'weapon_name.json'

# --- 核心處理函數 (邏輯基本不變) ---
def process_id_name_json(cursor, json_file_path):
    """
    (臨時調整) 處理 weapon_name.json。
    目前僅確保 ID 存在 (如果其 ID 與 equipment.id 對應)。
    暫時不更新 name 欄位，等待進一步確認此檔案的用途。
    """
    print(f"  -> 開始處理基礎信息文件: {json_file_path.name} (功能臨時調整)")
    items_processed = 0
    items_inserted_or_ignored = 0 # 計算 INSERT OR IGNORE 的次數
    # items_name_updated = 0 # 暫時不更新名稱

    try:
        with open(json_file_path, 'r', encoding='utf-utf-8') as f: #修正encoding='utf-8'
            data = json.load(f)

        if not isinstance(data, dict):
            print(f"  錯誤: {json_file_path.name} 的頂層結構不是預期的字典。", file=sys.stderr)
            raise ValueError(f"文件 {json_file_path.name} 格式錯誤：頂層不是字典。")

        for item_id_str, item_info in data.items():
            items_processed += 1
            if not isinstance(item_info, dict):
                print(f"  警告: ID {item_id_str} 對應的值不是字典，跳過。", file=sys.stderr)
                continue

            try:
                item_id = int(item_info.get('id', item_id_str))
                # name = item_info.get('name') # 暫時不獲取或使用 name

                # 步驟 1: (如果 weapon_name.json 的 ID 對應 equipment.id)
                # 確保 ID 在 equipment 表中存在。如果此 ID 來源不同，則此操作可能需要調整或移除。
                # 假設其 ID 是裝備 ID
                cursor.execute("INSERT OR IGNORE INTO equipment (id) VALUES (?)", (item_id,))
                if cursor.rowcount > 0 : # 如果真的插入了新行
                    items_inserted_or_ignored +=1
                elif cursor.rowcount == 0 : #如果是0 代表沒插入
                    # 代表資料庫已經有了
                    pass


                # 步驟 2: 暫時不更新名稱
                # if name is not None:
                #     cursor.execute("UPDATE equipment SET name = ? WHERE id = ?", (name, item_id))
                #     items_name_updated += 1
                pass #明確表示不做任何事情

            except ValueError:
                print(f"  警告: 無法將 ID '{item_info.get('id', item_id_str)}' 轉換為整數，跳過。", file=sys.stderr)
            except Exception as e:
                print(f"  警告: 處理 ID {item_id_str} 時發生未知錯誤: {e}", file=sys.stderr)

        print(f"  -> 完成處理 {json_file_path.name}。共處理 {items_processed} 項。")
        print(f"     嘗試插入或忽略了 {items_inserted_or_ignored} 個 ID 到 equipment 表。 (名稱未更新)")


    except FileNotFoundError:
        print(f"  錯誤: 目標 JSON 文件未找到: {json_file_path}", file=sys.stderr)
        raise
    except json.JSONDecodeError as e:
        print(f"  錯誤: 解析 JSON 文件 {json_file_path} 失敗: {e}", file=sys.stderr)
        raise
    except Exception as e:
        print(f"  處理 {json_file_path.name} 時發生意外錯誤: {e}", file=sys.stderr)
        raise

# --- 主執行入口 ---
if __name__ == '__main__':
    # 這個腳本現在應該由 azurlane_analyzer/preprocessing/main.py 通過 subprocess 調用

    print(f"\n--- [子腳本執行開始]: {Path(__file__).name} ---")

    # 1. 檢查並獲取命令行參數
    if len(sys.argv) != 3:
        print("錯誤: 此腳本需要兩個命令行參數：JSON數據目錄路徑 和 數據庫文件路徑。", file=sys.stderr)
        print(f"用法: python {sys.argv[0]} <json_data_dir> <db_file_path>", file=sys.stderr)
        sys.exit(1) # 以錯誤碼退出

    json_data_dir_arg = sys.argv[1]
    db_file_arg = sys.argv[2]

    # 將接收到的字符串參數轉換為 Path 對象
    json_data_dir = Path(json_data_dir_arg).resolve() # 使用 resolve 確保是絕對路徑
    db_file = Path(db_file_arg).resolve()

    print(f"  接收到 JSON 目錄: {json_data_dir}")
    print(f"  接收到 DB 文件: {db_file}")

    # 2. 構建需要處理的具體 JSON 文件的完整路徑
    target_json_file = json_data_dir / TARGET_JSON_FILENAME
    print(f"  目標處理文件: {target_json_file}")

    # 3. 檢查目標 JSON 文件是否存在
    if not target_json_file.is_file():
        print(f"錯誤: 目標 JSON 文件 '{TARGET_JSON_FILENAME}' 在指定目錄中未找到: {target_json_file}", file=sys.stderr)
        sys.exit(1)

    # 4. 連接數據庫並執行處理邏輯
    conn = None
    try:
        print(f"  連接到數據庫: {db_file} ...")
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        print("  數據庫連接成功。")

        # 調用核心處理函數
        process_id_name_json(cursor, target_json_file)

        # 提交事務
        conn.commit()
        print("  數據庫更改已提交。")

    except sqlite3.Error as e:
        print(f"!!! 數據庫操作過程中發生錯誤: {e} !!!", file=sys.stderr)
        if conn:
            conn.rollback() # 出錯時回滾
        sys.exit(1) # 以錯誤碼退出
    except Exception as e:
        # 捕獲由 process_id_name_json 拋出的其他異常
        print(f"!!! 處理過程中發生意外錯誤: {e} !!!", file=sys.stderr)
        if conn:
            conn.rollback()
        sys.exit(1)
    finally:
        if conn:
            conn.close()
            print("  數據庫連接已關閉。")

    print(f"--- [子腳本執行結束]: {Path(__file__).name} ---")