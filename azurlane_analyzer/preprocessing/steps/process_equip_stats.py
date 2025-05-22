# 位於: AzurLane-Analyzer/azurlane_analyzer/preprocessing/steps/process_equip_stats.py

import json
import sqlite3
import sys
from pathlib import Path
import time

# --- 配置 ---
TARGET_JSON_FILENAME = 'equip_data_statistics.json' # 處理的目標JSON檔案

# --- 輔助函數：處理 `base` 繼承 (保持不變) ---
def get_merged_equip_data(equip_id_str, all_equip_data):
    """
    遞迴獲取並合併裝備資料，處理 'base' 繼承。
    Args:
        equip_id_str (str): 要獲取資料的裝備 ID (字串形式)。
        all_equip_data (dict): 包含所有裝備原始資料的字典。
    Returns:
        dict: 合併了所有基礎屬性後的最終裝備資料字典。
              如果 equip_id_str 不存在，返回 None。
    """
    if equip_id_str not in all_equip_data:
        return None

    current_data = all_equip_data[equip_id_str].copy()
    base_id_str = current_data.get('base')

    if base_id_str:
        base_id_str = str(base_id_str)
        base_data = get_merged_equip_data(base_id_str, all_equip_data)
        if base_data:
            merged_data = base_data.copy()
            merged_data.update(current_data)
            return merged_data
        else:
            print(f"  嚴重警告: ID '{equip_id_str}' 的基礎 ID '{base_id_str}' 未找到！將只使用 '{equip_id_str}' 的資料。", file=sys.stderr)
            return current_data
    else:
        return current_data

# --- 核心處理函數 ---
def process_equipment_stats(cursor, json_file_path):
    """
    處理 equip_data_statistics.json，提取屬性並使用 Upsert (插入或更新) 到 equipment 表。
    """
    print(f"  -> 開始處理裝備統計檔案: {json_file_path.name}")
    start_time_load = time.time()
    raw_equip_data = {}
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            raw_equip_data = json.load(f) # 載入JSON數據
        load_time = time.time() - start_time_load
        print(f"  成功載入 {json_file_path.name} ({len(raw_equip_data)} 個頂層條目)，耗時: {load_time:.2f} 秒。")
    except FileNotFoundError:
        print(f"  錯誤: 檔案未找到 {json_file_path}", file=sys.stderr)
        raise
    except json.JSONDecodeError as e:
        print(f"  錯誤: 解析 JSON 檔案 {json_file_path} 失敗: {e}", file=sys.stderr)
        raise
    except Exception as e:
        print(f"  載入 {json_file_path.name} 時發生意外錯誤: {e}", file=sys.stderr)
        raise

    # --- 預處理所有裝備，處理繼承關係 ---
    print(f"  正在處理 'base' 繼承關係...")
    start_time_merge = time.time()
    final_equip_data = {}
    all_ids_to_process = []
    # 假設JSON結構直接是 ID:data 的字典
    if isinstance(raw_equip_data, dict) and all(isinstance(k, str) for k in raw_equip_data.keys()):
        all_ids_to_process = list(raw_equip_data.keys())
        actual_data_dict = raw_equip_data
    # (可選) 如果您的 JSON 頂層有 'all' 鍵包含所有 ID 列表，可以取消註釋並調整以下邏輯
    # elif 'all' in raw_equip_data and isinstance(raw_equip_data['all'], list):
    #     all_ids_to_process = [str(eid) for eid in raw_equip_data['all']]
    #     actual_data_dict = {str(k): v for k, v in raw_equip_data.items() if k != 'all'}
    else:
        print(f"  錯誤: {json_file_path.name} 的頂層結構無法識別。期望是直接的 ID->資料的字典。", file=sys.stderr)
        raise ValueError(f"無法處理 {json_file_path.name} 的結構")

    for equip_id_str in all_ids_to_process:
        merged_data = get_merged_equip_data(equip_id_str, actual_data_dict) # 獲取合併繼承後的數據
        if merged_data:
            final_equip_data[equip_id_str] = merged_data
    merge_time = time.time() - start_time_merge
    print(f"  完成 'base' 繼承處理，得到 {len(final_equip_data)} 筆最終裝備資料，耗時: {merge_time:.2f} 秒。")

    # --- 遍歷處理後的資料並 Upsert 到數據庫 ---
    print(f"  開始將裝備統計數據插入或更新到資料庫 (基於 attribute_x 解析屬性)...")
    start_time_db = time.time()
    processed_count = 0
    skipped_errors_count = 0

    # SQL Upsert 語句保持不變，因為欄位定義是一樣的
    sql_upsert_query = """
        INSERT INTO equipment (
            id, name, equipment_type, rarity, faction, weapon_id,               -- 基礎6個
            sub_type, base_damage_initial, volley_count, stat_bonus,            -- 舊擴展4個 (總共10個)
            stat_hp, stat_firepower, stat_torpedo, stat_aviation, stat_reload,  -- 屬性第1組5個 (總共15個)
            stat_antiair,stat_hit, stat_evasion, stat_speed, stat_luck,         -- 屬性第2組5個 (總共20個)
            stat_antisub,                                                       -- 屬性第3組1個 (總共21個)

            -- *** 新增的欄位 ***
            stat_oxy_max, stat_raid_distance                                    -- 新增2個 (總共23個)
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?                             -- 對應新欄位的 VALUES 佔位符
        )
        ON CONFLICT(id) DO UPDATE SET
            name = COALESCE(excluded.name, equipment.name),
            equipment_type = excluded.equipment_type,
            rarity = excluded.rarity,
            faction = excluded.faction,
            weapon_id = excluded.weapon_id,
            sub_type = excluded.sub_type,
            base_damage_initial = excluded.base_damage_initial,
            volley_count = excluded.volley_count,
            stat_bonus = excluded.stat_bonus,
            stat_hp = excluded.stat_hp,
            stat_firepower = excluded.stat_firepower,
            stat_torpedo = excluded.stat_torpedo,
            stat_aviation = excluded.stat_aviation,
            stat_reload = excluded.stat_reload,
            stat_antiair = excluded.stat_antiair,
            stat_hit = excluded.stat_hit,
            stat_evasion = excluded.stat_evasion,
            stat_speed = excluded.stat_speed,
            stat_luck = excluded.stat_luck,
            stat_antisub = excluded.stat_antisub,
            stat_oxy_max = excluded.stat_oxy_max,
            stat_raid_distance = excluded.stat_raid_distance
        ;
    """
    # 創建一個從 JSON attribute 名稱到數據庫 stat_* 欄位名的映射
    # 您需要根據 wiki 或數據實際情況擴充這個映射
    # 鍵是 JSON 中 attribute_x 的值，值是 s_* 變量名 (用於 locals() 賦值) 或直接的數據庫欄位名
    attribute_to_stat_map = {
        "health": "s_hp",          # 耐久
        "durability": "s_hp",      # 耐久 durability
        "cannon": "s_fp",          # 炮擊 (Firepower)
        "torpedo": "s_trp",        # 雷擊 (Torpedo)
        "air": "s_avi",            # 航空 (Aviation)
        "reload": "s_reload",      # 裝填
        "antiaircraft": "s_aa",    # 防空 (Anti-Air)
        "hit": "s_hit",            # 命中 (Hit/Accuracy)
        "dodge": "s_eva",          # 機動/閃避 (Evasion)
        "speed": "s_spd",          # 航速 (Speed)
        "luck": "s_luck",          # 幸運
        "antisub": "s_asw",        # 反潛
        "oxy_max": "s_oxy_max",    # 氧氣最大 
        "raid_distance": "s_raid_distance",   # 突襲距離

        # --- 請根據您的數據源添加更多映射 ---
        # 例如，如果 JSON 中有 "antiair" 代表防空，則也加入 "antiair": "s_aa",
        # 如果有 "range" 代表射程 (這通常不是 value_x 屬性，但舉例)
        # 如果有 "accuracy" 代表命中
        # 如果有 "aviation_correction" 代表空襲引導之類的
    }


    for equip_id_str, data in final_equip_data.items():
        try:
            equip_id_int = int(data.get('id', equip_id_str))
            name_val = data.get('name')
            type_val_json = data.get('type') # 裝備大類 (來自 JSON)
            type_val_db = str(type_val_json) if type_val_json is not None else None # 存入DB的equipment_type

            rarity_val = str(data.get('rarity')) if data.get('rarity') is not None else None
            faction_val = str(data.get('nationality')) if data.get('nationality') is not None else None
            weapon_id_list = data.get('weapon_id', [])
            main_weapon_id_val = weapon_id_list[0] if weapon_id_list else None

            damage_str = data.get('damage')
            base_dmg_val = None
            volley_ct_val = None
            if isinstance(damage_str, str) and 'x' in damage_str:
                parts = damage_str.split('x')
                try:
                    base_dmg_val = int(parts[0].strip())
                    volley_ct_val = int(parts[1].strip())
                except (ValueError, IndexError):
                    print(f"  警告: ID {equip_id_int} damage 解析錯誤: '{damage_str}'", file=sys.stderr)
            elif isinstance(damage_str, (int, float)):
                 base_dmg_val = damage_str
                 volley_ct_val = 1

            label_list = data.get('label', [])
            sub_type_val = None
            # 之前的 sub_type 示例，您可以保持或完善:
            if "MG" in label_list: sub_type_val = "主炮"
            elif "TP" in label_list: sub_type_val = "魚雷"
            # ... (其他 sub_type 判斷)

            # --- 初始化所有 s_* 屬性變量為 None ---
            # 這樣可以確保每次循環都從乾淨的狀態開始
            # 改為初始化一個字典來存儲這些屬性值
            current_stats = {
                "s_hp": None, "s_fp": None, "s_trp": None, "s_avi": None, "s_reload": None, "s_aa": None,
                "s_hit": None, "s_eva": None, "s_spd": None, "s_luck": None, "s_asw": None,
                "s_oxy_max": None, "s_raid_distance": None # 使用與 map 值一致的鍵名
            }
            # 初始化新欄位的變量
            # (如果添加了更多 stat_* 欄位，這裡也要對應初始化)

            # --- 獲取 attribute_x 和 value_x ---
            # 字典推導式，方便獲取所有存在的 attribute_x 和 value_x
            attributes = {
                f"attribute_{i}": data.get(f"attribute_{i}") for i in range(1, 4) # 通常是 1, 2, 3
            }
            values = {
                f"value_{i}": data.get(f"value_{i}") for i in range(1, 4)
            }

            # 創建一個字典來存儲原始 value_x，用於 stat_bonus 欄位
            raw_stat_bonus_dict = {}
            for i in range(1, 4):
                val_key = f"value_{i}"
                if values.get(val_key) is not None:
                    raw_stat_bonus_dict[val_key] = values[val_key]
            stat_bonus_json = json.dumps(raw_stat_bonus_dict) if raw_stat_bonus_dict else None


            # --- 根據 attribute_x 和 value_x 填充 current_stats 字典 ---
            for i in range(1, 4):
                attr_key = f"attribute_{i}"
                val_key = f"value_{i}"
                json_attr_name = attributes.get(attr_key)
                attr_value = values.get(val_key)

                print(f"  DEBUG LOOP: ID {equip_id_int}, attr_idx: {i}, json_attr: '{json_attr_name}', val: '{attr_value}'")

                if json_attr_name and attr_value is not None:
                    stat_key_in_dict = attribute_to_stat_map.get(json_attr_name.lower()) # 得到 "s_fp", "s_reload" 等
                    print(f"    Mapped_stat_key: '{stat_key_in_dict}'")
                    if stat_key_in_dict and stat_key_in_dict in current_stats: # 確保鍵存在於我們的字典中
                        try:
                            if isinstance(attr_value, str):
                                if '.' in attr_value: attr_value_numeric = float(attr_value)
                                else: attr_value_numeric = int(attr_value)
                            else:
                                attr_value_numeric = float(attr_value)

                            current_stats[stat_key_in_dict] = attr_value_numeric # 直接給字典賦值
                            print(f"    賦值成功(循環內): {stat_key_in_dict} = {current_stats[stat_key_in_dict]}")
                        except ValueError:
                            print(f"    警告: ID {equip_id_int}, 屬性 {json_attr_name} 值 '{attr_value}' 轉換失敗。", file=sys.stderr)
                    elif not stat_key_in_dict:
                        print(f"    信息: ID {equip_id_int}, 未處理屬性 '{json_attr_name}'。檢查 attribute_to_stat_map。", file=sys.stderr)
                    elif stat_key_in_dict not in current_stats:
                         print(f"   嚴重警告: ID {equip_id_int}, 鍵 '{stat_key_in_dict}' 不在 current_stats 字典中！請檢查初始化。", file=sys.stderr)

                elif json_attr_name and attr_value is None and data.get(val_key) == 0:
                    stat_key_in_dict = attribute_to_stat_map.get(json_attr_name.lower())
                    if stat_key_in_dict and stat_key_in_dict in current_stats:
                        current_stats[stat_key_in_dict] = 0.0
                        print(f"    賦值成功(循環內, value is 0): {stat_key_in_dict} = {current_stats[stat_key_in_dict]}")
            # --- attribute_x 循環結束 ---

            # --- 後備邏輯 (如果有的話，也應該更新 current_stats 字典) ---
            if attributes.get("attribute_1") is None and values.get("value_1") is not None and current_stats["s_hp"] is None:
                try:
                    current_stats["s_hp"] = float(values["value_1"])
                    print(f"  DEBUG FALLBACK: ID {equip_id_int}, 後備邏輯將 value_1 ({values['value_1']}) 賦值給 s_hp。")
                except (ValueError, TypeError):
                    pass
            # --- 後備邏輯結束 ---

            # --- 直接從 JSON data 中讀取新欄位的值 (也更新 current_stats 字典) ---
            oxy_max_json_val = data.get('oxy_max')
            if oxy_max_json_val is not None:
                try: current_stats["s_oxy_max"] = float(oxy_max_json_val)
                except ValueError: print(f"  警告: ID {equip_id_int}, oxy_max 值 '{oxy_max_json_val}' 轉換失敗。", file=sys.stderr)

            raid_distance_json_val = data.get('raid_distance')
            if raid_distance_json_val is not None:
                try: current_stats["s_raid_distance"] = float(raid_distance_json_val)
                except ValueError: print(f"  警告: ID {equip_id_int}, raid_distance 值 '{raid_distance_json_val}' 轉換失敗。", file=sys.stderr)


            # --- 正確的 DEBUG FINAL STATS 打印位置 ---
            print(f"  DEBUG FINAL STATS for ID {equip_id_int} (準備寫入DB):")
            # 從 current_stats 字典中獲取值來打印
            print(f"    s_hp: {current_stats['s_hp']}, s_fp: {current_stats['s_fp']}, s_trp: {current_stats['s_trp']}, s_avi: {current_stats['s_avi']}, s_reload: {current_stats['s_reload']}, s_aa: {current_stats['s_aa']}")
            print(f"    s_hit: {current_stats['s_hit']}, s_eva: {current_stats['s_eva']}, s_spd: {current_stats['s_spd']}, s_luck: {current_stats['s_luck']}, s_asw: {current_stats['s_asw']}")
            print(f"    s_oxy_max: {current_stats['s_oxy_max']}, s_raid_distance: {current_stats['s_raid_distance']}")


            # --- 準備 data_tuple (從 current_stats 字典中按順序取值) ---
            data_tuple = (
                equip_id_int, name_val, type_val_db, rarity_val, faction_val, main_weapon_id_val,
                sub_type_val, base_dmg_val, volley_ct_val, stat_bonus_json,
                current_stats['s_hp'], current_stats['s_fp'], current_stats['s_trp'], current_stats['s_avi'], current_stats['s_reload'], current_stats['s_aa'],
                current_stats['s_hit'], current_stats['s_eva'], current_stats['s_spd'], current_stats['s_luck'], current_stats['s_asw'],
                current_stats['s_oxy_max'], current_stats['s_raid_distance'] # 確保鍵名與 current_stats 初始化時一致
            )

            cursor.execute(sql_upsert_query, data_tuple)
            processed_count += 1

        except KeyError as e:
            print(f"  警告: ID {equip_id_str} 缺少鍵: {e}", file=sys.stderr)
            skipped_errors_count += 1
        except ValueError as e:
            print(f"  警告: ID {equip_id_str} 資料轉換錯誤: {e}", file=sys.stderr)
            skipped_errors_count += 1
        except Exception as e:
            print(f"  錯誤: ID {equip_id_str} 意外錯誤: {e}", file=sys.stderr)
            # 為了更詳細的錯誤追蹤，可以打印 traceback
            # import traceback
            # traceback.print_exc()
            skipped_errors_count += 1

    db_time = time.time() - start_time_db
    print(f"  完成資料庫操作。成功處理 {processed_count} 筆，跳過 {skipped_errors_count} 筆。耗時: {db_time:.2f} 秒。")

# --- 主執行入口 (保持不變) ---
if __name__ == '__main__':
    print(f"\n--- [子腳本執行開始]: {Path(__file__).name} ---")
    if len(sys.argv) != 3:
        print("錯誤: 需要兩個命令行參數：JSON數據目錄路徑 和 數據庫文件路徑。", file=sys.stderr)
        sys.exit(1)

    json_data_dir = Path(sys.argv[1]).resolve()
    db_file = Path(sys.argv[2]).resolve()
    print(f"  接收到 JSON 目錄: {json_data_dir}")
    print(f"  接收到 DB 文件: {db_file}")

    target_json_file = json_data_dir / TARGET_JSON_FILENAME
    print(f"  目標處理文件: {target_json_file}")

    if not target_json_file.is_file():
        print(f"錯誤: 目標 JSON 檔案 '{TARGET_JSON_FILENAME}' 未找到: {target_json_file}", file=sys.stderr)
        sys.exit(1)

    conn = None
    try:
        print(f"  連接到數據庫: {db_file} ...")
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        print("  數據庫連接成功。")

        process_equipment_stats(cursor, target_json_file) # 調用核心處理函數

        print("  提交資料庫更改...")
        conn.commit() # 提交事務
        print("  資料庫更改已提交。")

    except sqlite3.Error as e:
        print(f"!!! 數據庫操作過程中發生錯誤: {e} !!!", file=sys.stderr)
        if conn: conn.rollback()
        sys.exit(1)
    except Exception as e:
        print(f"!!! 處理過程中發生意外錯誤: {e} !!!", file=sys.stderr)
        if conn: conn.rollback()
        sys.exit(1)
    finally:
        if conn:
            conn.close()
            print("  數據庫連接已關閉。")
    print(f"--- [子腳本執行結束]: {Path(__file__).name} ---")