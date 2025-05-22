# 位於: AzurLane-Analyzer/azurlane_analyzer/preprocessing/steps/process_weapon_property.py

import json
import sqlite3
import sys
from pathlib import Path
import time  # 用於計時

# --- 配置 ---
# 此腳本負責處理的 JSON 文件名
TARGET_JSON_FILENAME = 'weapon_property.json'

# --- 核心處理函數 ---
def update_equipment_with_weapon_properties(cursor, weapon_properties, equipment_to_update):
    """
    根據 weapon_id 將 weapon_properties 中的數據更新到 equipment 表中。

    Args:
        cursor: SQLite 資料庫游標。
        weapon_properties (dict): 從 weapon_property.json 載入的字典 {prop_id_str: prop_data_dict}。
        equipment_to_update (list): 從 equipment 表查詢到的 [(equip_id, weapon_id)] 列表。
    """
    print(f"  -> 開始更新 {len(equipment_to_update)} 筆裝備資料的武器屬性...")
    start_time = time.time()
    updated_count = 0
    skipped_count = 0

    # 準備 SQL UPDATE 語句 (只準備一次)
    # 注意欄位順序必須與後面的 data_tuple 完全一致
    sql_update_query = """
        UPDATE equipment
        SET
            weapon_property_id = ?,  -- 1
            wp_type = ?,             -- 2
            wp_bullet_ids = ?,       -- 3
            wp_barrage_ids = ?,      -- 4
            wp_range = ?,            -- 5
            wp_angle = ?,            -- 6
            wp_min_range = ?,        -- 7
            wp_auto_aftercast = ?,   -- 8
            wp_recover_time = ?,     -- 9
            wp_precast_param = ?,    -- 10
            wp_damage = ?,           -- 11
            wp_oxy_type = ?,         -- 12
            wp_expose = ?,           -- 13
            wp_fire_fx = ?,          -- 14
            wp_fire_sfx = ?,         -- 15
            wp_fire_fx_loop_type = ?,-- 16
            weapon_property_json = ? -- 17
        WHERE id = ?                 -- 18 (用於 WHERE 條件)
    """

    for equip_id, weapon_id_int in equipment_to_update:
        if weapon_id_int is None:
            skipped_count += 1
            continue # 理論上不應發生，因為 SELECT 查詢已過濾

        # weapon_property.json 的鍵通常是字串
        weapon_id_str = str(weapon_id_int)

        # 在載入的 weapon_properties 中查找對應的資料
        prop_data = weapon_properties.get(weapon_id_str)

        if prop_data is None:
            # print(f"  警告: 裝備 ID {equip_id} 的 weapon_id '{weapon_id_str}' 在 {TARGET_JSON_FILENAME} 中未找到，跳過。", file=sys.stderr)
            skipped_count += 1
            continue

        try:
            # --- 提取需要放入獨立欄位的數據 ---
            wp_id = prop_data.get('id') # weapon_property 自身的 id
            wp_type_val = prop_data.get('type')
            # 對列表類型，使用 .get(key, []) 確保即使鍵不存在也返回空列表，避免 json.dumps 出錯
            wp_bullet_ids_val = json.dumps(prop_data.get('bullet_ID', []))
            wp_barrage_ids_val = json.dumps(prop_data.get('barrage_ID', []))
            wp_range_val = prop_data.get('range')
            wp_angle_val = prop_data.get('angle')
            wp_min_range_val = prop_data.get('min_range')
            wp_auto_aftercast_val = prop_data.get('auto_aftercast')
            wp_recover_time_val = prop_data.get('recover_time')
            wp_precast_param_val = json.dumps(prop_data.get('precast_param', []))
            wp_damage_val = prop_data.get('damage')
            wp_oxy_type_val = json.dumps(prop_data.get('oxy_type', []))
            wp_expose_val = prop_data.get('expose')
            wp_fire_fx_val = prop_data.get('fire_fx')
            wp_fire_sfx_val = prop_data.get('fire_sfx')
            wp_fire_fx_loop_type_val = prop_data.get('fire_fx_loop_type')

            # --- 準備完整的 JSON 字串 ---
            weapon_property_json_str = json.dumps(prop_data)

            # --- 準備更新用的數據元組 (順序必須與 SQL 語句完全對應) ---
            data_tuple = (
                wp_id,                   # 1
                wp_type_val,             # 2
                wp_bullet_ids_val,       # 3
                wp_barrage_ids_val,      # 4
                wp_range_val,            # 5
                wp_angle_val,            # 6
                wp_min_range_val,        # 7
                wp_auto_aftercast_val,   # 8
                wp_recover_time_val,     # 9
                wp_precast_param_val,    # 10
                wp_damage_val,           # 11
                wp_oxy_type_val,         # 12
                wp_expose_val,           # 13
                wp_fire_fx_val,          # 14
                wp_fire_sfx_val,         # 15
                wp_fire_fx_loop_type_val,# 16
                weapon_property_json_str,# 17
                equip_id                 # 18 (用於 WHERE id = ?)
            )

            # --- 執行更新 ---
            cursor.execute(sql_update_query, data_tuple)
            updated_count += 1

            # 進度提示 (可選，避免輸出過多)
            if updated_count % 500 == 0:
                 elapsed = time.time() - start_time
                 print(f"    已更新 {updated_count} / {len(equipment_to_update)} 筆記錄... ({elapsed:.2f} 秒)")


        except Exception as e:
            print(f"  錯誤: 更新裝備 ID {equip_id} (weapon_id={weapon_id_str}) 時發生錯誤: {e}", file=sys.stderr)
            # 決定是否要因為單筆錯誤而中止，或者繼續處理下一筆
            # continue # 選擇繼續處理下一筆

    end_time = time.time()
    total_time = end_time - start_time
    print(f"  -> 完成武器屬性更新。成功更新 {updated_count} 筆，跳過 {skipped_count} 筆。耗時: {total_time:.2f} 秒。")


# --- 主執行入口 ---
if __name__ == '__main__':
    print(f"\n--- [子腳本執行開始]: {Path(__file__).name} ---")

    # 1. 檢查並獲取命令行參數
    if len(sys.argv) != 3:
        print("錯誤: 此腳本需要兩個命令行參數：JSON數據目錄路徑 和 數據庫文件路徑。", file=sys.stderr)
        print(f"用法: python {sys.argv[0]} <json_data_dir> <db_file_path>", file=sys.stderr)
        sys.exit(1)

    json_data_dir = Path(sys.argv[1]).resolve()
    db_file = Path(sys.argv[2]).resolve()

    print(f"  接收到 JSON 目錄: {json_data_dir}")
    print(f"  接收到 DB 文件: {db_file}")

    # 2. 構建 weapon_property.json 的完整路徑
    target_json_file = json_data_dir / TARGET_JSON_FILENAME
    print(f"  目標處理文件: {target_json_file}")

    # 3. 載入 weapon_property.json
    weapon_properties_data = {}
    if not target_json_file.is_file():
        print(f"錯誤: 目標 JSON 文件 '{TARGET_JSON_FILENAME}' 在指定目錄中未找到: {target_json_file}", file=sys.stderr)
        sys.exit(1)
    try:
        print(f"  正在載入 {target_json_file.name}...")
        with open(target_json_file, 'r', encoding='utf-8') as f:
            weapon_properties_data = json.load(f)
        print(f"  成功載入 {len(weapon_properties_data)} 筆武器屬性資料。")
    except json.JSONDecodeError as e:
        print(f"錯誤: 解析 JSON 文件 {target_json_file} 失敗: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"載入 {target_json_file.name} 時發生意外錯誤: {e}", file=sys.stderr)
        sys.exit(1)

    # 4. 連接數據庫並執行處理邏輯
    conn = None
    try:
        print(f"  連接到數據庫: {db_file} ...")
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        print("  數據庫連接成功。")

        # 查詢需要更新的裝備 ID 和 weapon_id
        print("  正在從 equipment 表查詢需要更新的記錄...")
        cursor.execute("SELECT id, weapon_id FROM equipment WHERE weapon_id IS NOT NULL")
        equipment_to_update = cursor.fetchall()
        print(f"  查詢到 {len(equipment_to_update)} 筆裝備記錄有關聯的 weapon_id。")

        if not equipment_to_update:
            print("  沒有找到需要更新武器屬性的裝備記錄 (可能是 process_equip_stats.py 未執行或未填充 weapon_id)。")
        else:
            # 調用核心更新函數
            update_equipment_with_weapon_properties(cursor, weapon_properties_data, equipment_to_update)

        # 提交事務
        print("  提交資料庫更改...")
        conn.commit()
        print("  資料庫更改已提交。")

    except sqlite3.Error as e:
        print(f"!!! 數據庫操作過程中發生錯誤: {e} !!!", file=sys.stderr)
        if conn:
            conn.rollback()
        sys.exit(1)
    except Exception as e:
        print(f"!!! 處理過程中發生意外錯誤: {e} !!!", file=sys.stderr)
        if conn:
            conn.rollback()
        sys.exit(1)
    finally:
        if conn:
            conn.close()
            print("  數據庫連接已關閉。")

    print(f"--- [子腳本執行結束]: {Path(__file__).name} ---")