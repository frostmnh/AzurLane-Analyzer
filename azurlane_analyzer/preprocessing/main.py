# 位於: AzurLane-Analyzer/azurlane_analyzer/preprocessing/main.py

import sqlite3
import subprocess
import sys
from pathlib import Path

# --- 路徑計算 (基於此文件的新位置) ---
try:
    # 1. 當前腳本所在目錄 (.../azurlane_analyzer/preprocessing/)
    SCRIPT_DIR = Path(__file__).resolve().parent
    # 2. 包根目錄 (.../azurlane_analyzer/)
    PACKAGE_ROOT = SCRIPT_DIR.parent
    # 3. 項目根目錄 (.../AzurLane-Analyzer/)
    PROJECT_ROOT = PACKAGE_ROOT.parent
except NameError:
    # Fallback if __file__ is not defined (e.g., interactive)
    # This fallback might be less reliable with the new structure
    PROJECT_ROOT = Path.cwd() # Assumes running from project root
    SCRIPT_DIR = PROJECT_ROOT / 'azurlane_analyzer' / 'preprocessing'
    print("警告: 無法通過 __file__ 自動檢測路徑。", file=sys.stderr)
    print(f"假設項目根目錄為: {PROJECT_ROOT}", file=sys.stderr)
    print(f"假設腳本目錄為: {SCRIPT_DIR}", file=sys.stderr)

# 4. 數據輸出目錄
OUTPUT_DIR = PROJECT_ROOT / 'DataOutput'
# 5. 數據庫文件的絕對路徑
DB_FILE = OUTPUT_DIR / 'azur_lane_data.db'
# 6. 原始 JSON 數據目錄的絕對路徑
JSON_DATA_DIR = PROJECT_ROOT / 'AzurLaneData' / 'sharecfgdata'
# 7. 預處理步驟子腳本目錄
STEPS_DIR = SCRIPT_DIR / 'steps'

# --- 子腳本路徑定義 ---
#  process_equip_templates.py ?
PROCESS_WEAPON_NAME_SCRIPT = STEPS_DIR / 'process_weapon_name.py'
PROCESS_STATS_SCRIPT = STEPS_DIR / 'process_equip_stats.py'
PROCESS_WEAPON_PROP_SCRIPT = STEPS_DIR / 'process_weapon_property.py'
PROCESS_SHIPS_SCRIPT = STEPS_DIR / 'process_ships.py'
PROCESS_SKILLS_SCRIPT = STEPS_DIR / 'process_skills.py'
# ... 其他子腳本 ...


# --- 數據庫結構定義 (*** 更新此函數 ***) ---
def create_all_tables(db_path):
    """連接數據庫並確保所有表都已根據最新結構創建。"""
    print(f"初始化/檢查數據庫結構於: {db_path}")
    try:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # --- 裝備表 (equipment) - 添加屬性欄位 ---
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS equipment (
                -- 核心識別信息
                id INTEGER PRIMARY KEY,
                name TEXT,
                equipment_type TEXT,         -- 主類型 (例如: 艦炮, 魚雷, 飛機, 設備)
                rarity TEXT,
                tier TEXT,
                faction TEXT,
                weapon_type TEXT,            -- 武器類型 (例如: 主炮, 副炮, 驅逐炮) - 可能與 equipment_type 或 sub_type 關聯
                sub_type TEXT,               -- 子類型 (例如: 高爆彈主炮, 通常彈魚雷)

                -- === 新增的詳細屬性加成欄位 (來自 value_x 的解析) ===
                stat_hp REAL,                -- 耐久 (Health Points)
                stat_firepower REAL,         -- 炮擊 (Firepower)
                stat_torpedo REAL,           -- 雷擊 (Torpedo)
                stat_aviation REAL,          -- 航空 (Aviation)
                stat_reload REAL,            -- 裝填 (Reload)
                stat_antiair REAL,           -- 防空 (Anti-Air)
                stat_hit REAL,               -- 命中 (Hit/Accuracy)
                stat_evasion REAL,           -- 機動/閃避 (Evasion)
                stat_speed REAL,             -- 航速 (Speed)
                stat_luck REAL,              -- 幸運 (Luck)
                stat_antisub REAL,           -- 反潛 (Anti-Submarine Warfare)
                stat_oxy_max REAL,           -- 氧氣最大值
                stat_raid_distance REAL,     -- 突襲距離
                -- (可以根據需要添加更多特定屬性)

                -- 時間與攻擊週期相關
                storehouse_cd_initial REAL,  -- 倉庫初始CD (遊戲內顯示的面板射速)
                storehouse_cd_max REAL,      -- 倉庫滿強CD
                attack_foreswing REAL,       -- 攻擊前搖
                attack_duration REAL,        -- 攻擊持續時間
                attack_backswing REAL,       -- 攻擊後搖
                has_preload INTEGER,         -- 是否預裝填 (0 或 1)
                triggers_global_cooldown INTEGER, -- 是否觸發全局冷卻 (0 或 1)
                volley_barrel_delay REAL,    -- 多聯裝炮管間的開火延遲

                -- 傷害與彈藥相關
                base_damage_initial REAL,    -- 初始基礎傷害 (單發/單次)
                base_damage_max REAL,        -- 滿強基礎傷害
                damage_coefficient_initial REAL, -- 初始傷害補正係數
                damage_coefficient_max REAL,   -- 滿強傷害補正係數
                damage_stat_type TEXT,       -- 傷害依賴的屬性類型 (例如: firepower, torpedo)
                stat_efficiency REAL,        -- 屬性效率 (例如: 炮擊效率 120%)
                volley_count INTEGER,        -- 彈幕/攻擊數量 (例如: 3聯裝炮是3)
                payload TEXT,                -- 彈藥/掛載配置 (JSON字串)
                compatible_ammo TEXT,        -- 兼容彈藥類型 (JSON字串)
                override_ammo_properties TEXT,-- 覆蓋彈藥屬性 (JSON字串)

                -- 速度與範圍
                base_velocity REAL,          -- 彈藥基礎飛行速度
                base_speed REAL,             -- (飛機)基礎航速
                targeting_range_max REAL,    -- 最大索敵/攻擊範圍
                targeting_range_min REAL,    -- 最小索敵/攻擊範圍
                targeting_angle REAL,        -- 攻擊角度/扇區

                -- 其他加成與效果
                stat_bonus TEXT,             -- 原始 value_x 數據的 JSON 存儲 (備份/參考)
                inherent_modifiers TEXT,     -- 內在修正 (JSON字串)
                unique_group_id TEXT,        -- 唯一分組ID (用於互斥裝備等)

                -- 裝備限制
                forbidden_ship_types TEXT,   -- 禁用艦種類型 (JSON字串)

                -- 強化數據細節
                enhancement_data TEXT,       -- 強化數據 (JSON字串, 包含各等級屬性)

                -- 關聯 ID
                weapon_id INTEGER,           -- 關聯的 weapon_property.json 中的 ID

                -- === 來自 weapon_property.json 的欄位 ===
                weapon_property_id INTEGER,
                wp_type INTEGER,
                wp_bullet_ids TEXT,
                wp_barrage_ids TEXT,
                wp_range REAL,
                wp_angle REAL,
                wp_min_range REAL,
                wp_auto_aftercast REAL,
                wp_recover_time REAL,
                wp_precast_param TEXT,
                wp_damage REAL,
                wp_oxy_type TEXT,
                wp_expose INTEGER,
                wp_fire_fx TEXT,
                wp_fire_sfx TEXT,
                wp_fire_fx_loop_type INTEGER,
                weapon_property_json TEXT    -- weapon_property 完整 JSON (備份/參考)
            )
        ''')
        print("  - 表 'equipment' 結構檢查/創建完成 (已包含詳細屬性欄位 stat_*)。")

        # --- 艦船表 (ships) ---
        # (結構不變)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ships (
                id INTEGER PRIMARY KEY, name TEXT, ship_type TEXT, rarity TEXT, faction TEXT,
                base_reload_stat INTEGER, base_fp INTEGER, base_trp INTEGER, base_avi INTEGER,
                base_aa INTEGER, base_hp INTEGER, slots TEXT, aircraft_slots TEXT
            )
        ''')
        print("  - 表 'ships' 結構檢查/創建完成。")

        # --- 技能表 (skills) ---
        # (結構不變)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS skills (
                id INTEGER PRIMARY KEY, name TEXT, description TEXT, trigger_info TEXT, effects TEXT
            )
        ''')
        print("  - 表 'skills' 結構檢查/創建完成。")

        conn.commit()
        print("數據庫結構已準備就緒。")

    except sqlite3.Error as e:
        print(f"!!! 數據庫操作錯誤: {e} !!!", file=sys.stderr)
        sys.exit(1)
    finally:
        if conn:
            conn.close()


# --- 子腳本執行 (函數邏輯不變, 使用新的子腳本路徑) ---
def run_script(script_path: Path, json_dir: Path, db_file: Path):
    """運行指定的 Python 子腳本，並將 JSON 目錄和 DB 文件路徑作為參數傳遞。"""
    if not script_path.is_file():
        print(f"!!! 錯誤: 子腳本未找到: {script_path} !!!", file=sys.stderr)
        print("請確保所有 process_*.py 文件都存在於 'steps' 目錄下。", file=sys.stderr)
        return False

    script_name = script_path.name
    print(f"\n--- === [ 開始執行: {script_name} ] === ---")
    print(f"  傳遞參數: JSON 目錄='{json_dir}', DB 文件='{db_file}'")
    try:
        # 將 JSON 目錄和 DB 文件路徑作為命令行參數傳遞給子腳本
        cmd_args = [
            sys.executable, '-u', str(script_path),
            str(json_dir),
            str(db_file)
        ]
        print(f"  執行命令: {' '.join(cmd_args)}") # 打印實際執行的命令

        result = subprocess.run(
            cmd_args,
            check=True,
            capture_output=True,
            text=True,
            encoding='utf-8',
            # 子腳本的工作目錄可以設置為項目根目錄或腳本所在目錄，
            # 但由於我們傳遞了絕對路徑，這影響不大。設置為項目根目錄可能更直觀。
            cwd=PROJECT_ROOT
        )
        print(f"--- [ {script_name} 標準輸出 ] ---")
        # 處理子腳本的標準輸出
        stdout_content = result.stdout.strip() if result.stdout else "" # 移除首尾空白
        if stdout_content: # 僅在 strip 後還有內容時打印
            print(stdout_content)
        elif result.stdout is not None: # 如果原始 stdout 存在但 strip 後為空 (例如只包含換行符)
            print("(子腳本標準輸出為空或僅包含空白)") # 可以選擇打印提示或不打印
        else: # result.stdout 為 None
            print("(子腳本無標準輸出)")


        # 處理子腳本的錯誤輸出
        if result.stderr:
            print(f"--- [ {script_name} 錯誤輸出 ] ---", file=sys.stderr)
            stderr_content = result.stderr.strip() # 移除首尾空白
            if stderr_content: # 僅在 strip 後還有內容時打印
                print(stderr_content, file=sys.stderr)
            elif result.stderr.strip() == "": # 如果原始 stderr 存在但 strip 後為空
                 # 通常這種情況下不需要特別提示，因為 stderr 本身就是空的
                 pass

        print(f"--- === [ 完成執行: {script_name} (成功) ] === ---")
        return True

    except subprocess.CalledProcessError as e:
        print(f"!!! 運行 {script_name} 時發生錯誤 (返回碼: {e.returncode}) !!!", file=sys.stderr)
        print(f"--- [ {script_name} 錯誤時的標準輸出 ] ---", file=sys.stderr)
        print(e.stdout if e.stdout else "(無)", file=sys.stderr)
        print(f"--- [ {script_name} 錯誤時的錯誤輸出 ] ---", file=sys.stderr)
        print(e.stderr if e.stderr else "(無)", file=sys.stderr)
        return False
    except Exception as e:
        print(f"!!! 運行 {script_name} 時發生意外錯誤: {e} !!!", file=sys.stderr)
        return False


# --- 主執行流程 ---
if __name__ == '__main__':
    print("========================================")
    print("=== 碧藍航線數據預處理主控腳本 (v2) ===")
    print("========================================")
    print(f"項目根目錄: {PROJECT_ROOT}")
    print(f"Python 包目錄: {PACKAGE_ROOT}")
    print(f"預處理腳本目錄: {SCRIPT_DIR}")
    print(f"數據庫文件: {DB_FILE}")
    print(f"JSON 數據目錄: {JSON_DATA_DIR}")
    print(f"處理步驟腳本目錄: {STEPS_DIR}")
    print("-" * 40)

    # 步驟 0: 檢查 JSON 數據目錄是否存在
    if not JSON_DATA_DIR.is_dir():
        print(f"!!! 致命錯誤: JSON 數據目錄未找到: {JSON_DATA_DIR} !!!", file=sys.stderr)
        print("請確保 'AzurLaneData/sharecfgdata' 目錄存在於項目根目錄下。", file=sys.stderr)
        sys.exit(1)

    # 步驟 1: 初始化數據庫結構
    create_all_tables(DB_FILE)

    # 步驟 2: 定義要運行的腳本列表 (使用 steps/ 目錄下的路徑)
    scripts_to_run = [
        PROCESS_STATS_SCRIPT,          # 1. 首先處理 equip_data_statistics.json (插入主要裝備數據)
        PROCESS_WEAPON_PROP_SCRIPT,    # 2. 處理 weapon_property.json (依賴 weapon_id)
        PROCESS_WEAPON_NAME_SCRIPT,    # 3. 處理 weapon_name.json (其確切用途和更新目標待進一步確認)
        PROCESS_SHIPS_SCRIPT,          # 4. 處理艦船數據
        PROCESS_SKILLS_SCRIPT,         # 5. 處理技能數據
        # ... 添加更多子腳本的路徑 ...
    ]

    # 步驟 3: 按順序執行子腳本，並傳遞路徑參數
    all_success = True
    for script_path in scripts_to_run:
        # 將 JSON 目錄和 DB 文件路徑傳遞給 run_script 函數
        success = run_script(script_path, JSON_DATA_DIR, DB_FILE)
        if not success:
            all_success = False
            print(f"\n!!! 由於腳本 {script_path.name} 執行失敗，預處理流程已中斷 !!!", file=sys.stderr)
            break

    print("\n" + "=" * 40)
    if all_success:
        print("=== 所有預處理腳本已成功執行完畢 ===")
    else:
        print("=== 預處理流程因錯誤而中止 ===", file=sys.stderr)
    print("=" * 40)