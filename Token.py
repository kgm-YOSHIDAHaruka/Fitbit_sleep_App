# =======================
# ✅ アプリ② 管理者用 データ一括取得ページ（refresh_token対応、UI改善）
# ファイル名: admin_sleep_data_collector.py
# =======================

import streamlit as st
import pandas as pd
import json
import requests
import zipfile
import io
import os
from datetime import date, timedelta
import base64


# -----------------------
# ユーティリティ
# -----------------------
def refresh_access_token(token_data):
    refresh_url = "https://api.fitbit.com/oauth2/token"
    credentials = f"{token_data['client_id']}:{token_data['client_secret']}"
    credentials_b64 = base64.b64encode(credentials.encode()).decode()
    headers = {
        "Authorization": f"Basic {credentials_b64}",
        "Content-Type": "application/x-www-form-urlencoded"
        }
    data = {
        "grant_type": "refresh_token",
        "refresh_token": token_data["refresh_token"],
        }
    try:
        response = requests.post(refresh_url, headers=headers, data=data)
    except Exception as e:
        st.error(f"トークン更新失敗: ネットワーク例外 {e}")
        return None
    
    if response.status_code == 200:
        new_token = response.json()
        token_data.update(new_token)
        return token_data
    else:
        st.error(f"トークン更新失敗: {response.status_code} - {response.text}")
        return None




def safe_get(dct, *keys, default=None):
    """ネストした辞書からキーを安全に取得する。"""
    cur = dct
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


# -----------------------
# Streamlit レイアウト
# -----------------------
st.set_page_config(page_title="Fitbit睡眠データ一括取得", page_icon="📊")
st.title("📊 Fitbit睡眠データ一括取得")

st.markdown("""
このページでは、被験者のFitbitトークン（アカウント情報）をもとに、指定期間の睡眠データを一括取得します。

### 主な出力
- **【サマリーCSV】**: 1晩の睡眠ログ単位の要約（durationや各ステージminutes/countなど）
- **【levels_data CSV】**: 30秒刻みのステージ時系列（deep/light/rem/wake）。サイクル再構成の材料に。
- **【levels_short CSV】**: 3分以下の短い覚醒などのイベント。
- **【token_*.json】**: 更新済みトークン（各被験者ごと）。

### 📝 操作手順
0．研究対象者から提出いただいたファイル名は、研究対象者識別番号が含まれています。これを、解析用識別番号に変換してください。
> 例 「token_T001.json」 → 「token_Y001.json」
> ※トークンファイルは `token_解析ID.json` という形式で保存されている必要があります。


1. 取得したい期間（試験期間）を選択してください。


2. ファイル名を変更したファイルをアップロードしてください。


3. 「一括取得＆ZIPでダウンロード」ボタンを押してください。


**注意**: Fitbit Web APIは\"Sleep Score\"を提供していません。本アプリでは `efficiency`（Fitbit定義の睡眠効率）を保存します。
""")

start_date = st.date_input("取得開始日", value=date.today() - timedelta(days=7))
end_date = st.date_input("取得終了日", value=date.today())
uploaded_files = st.file_uploader(
    "トークンファイル（複数選択可）をアップロード", type="json", accept_multiple_files=True
)

DEBUG = st.checkbox("デバッグログをZIPに保存", value=True)

if st.button("データ取得を開始"):
    if not uploaded_files:
        st.warning("少なくとも1つのトークンファイルをアップロードしてください。")
    elif start_date > end_date:
        st.warning("取得開始日が終了日より後になっています。期間を確認してください。")
    else:
        total_users = len(uploaded_files)
        pbar = st.progress(0)
        status_area = st.empty()
        
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
            for idx, uploaded_file in enumerate(uploaded_files, start=1):
                user_id = uploaded_file.name.replace("token_", "").replace(".json", "")
                status_area.info(f"[{idx}/{total_users}] {user_id}: 取得開始")
                try:
                    token_data = json.load(uploaded_file)
                except Exception as e:
                    st.error(f"{user_id}: トークンJSONの読み込みに失敗しました: {e}")
                    continue

                # トークン更新
                token_data = refresh_access_token(token_data)
                if not token_data:
                    st.warning(f"{user_id} のトークン更新に失敗しました。スキップします。")
                    continue

                access_token = token_data.get("access_token")
                headers = {"Authorization": f"Bearer {access_token}"}

                #スコープ確認
                scopes = set(str(token_data.get("scope", "")).split())
                if DEBUG:
                    zipf.writestr(f"{user_id}_token_scopes.txt", " ".join(sorted(scopes)))
                if "sleep" not in scopes:
                    st.error(f"{user_id}: このトークンには 'sleep' スコープがありません。再認可が必要です。")
                    # エラーログもZIPへ
                    if DEBUG:
                        zipf.writestr(f"errors/{user_id}_missing_scope.txt", "missing 'sleep' scope")
                    continue

                #取得範囲
                start_buf = (start_date - timedelta(days=1)).strftime("%Y-%m-%d")
                end_buf = (end_date + timedelta(days=1)).strftime("%Y-%m-%d")
                start_str = start_date.strftime("%Y-%m-%d")
                end_str = end_date.strftime("%Y-%m-%d")

                url = f"https://api.fitbit.com/1.2/user/-/sleep/date/{start_buf}/{end_buf}.json"
                try:
                    r = requests.get(url, headers=headers, timeout=60)
                except Exception as e:
                    if DEBUG:
                        zipf.writestr(f"errors/{user_id}_range_request.txt", f"Exception: {e}")
                    continue

                if DEBUG:
                    remain = r.headers.get("fitbit-rate-limit-remaining", "")
                    zipf.writestr(
                        f"debug/{user_id}_range_status.txt",
                        f"status={r.status_code} remain={remain}
url={url}"
                    )

                if r.status_code != 200:
                    if DEBUG:
                        zipf.writestr(f"errors/{user_id}_range_status.txt", r.text[:2000])
                    continue

                try:
                    d = r.json()
                except Exception as e:
                    if DEBUG:
                        zipf.writestr(
                            f"errors/{user_id}_range_json.txt",
                           f"JSON error: {e}


{r.text[:2000]}"
                        )
                    continue

                sleeps = d.get("sleep", []) if isinstance(d, dict) else []
                if DEBUG:
                    zipf.writestr(
                        f"debug/{user_id}_range.json",
                        json.dumps({"count": len(sleeps), "sample": sleeps[:2]}, ensure_ascii=False, indent=2)
                    )
                    if not sleeps:
                        status_area.warning(f"{user_id}: 指定期間に睡眠ログなし")

                
                
                # 出力用の入れ物
                summary_rows = []
                levels_data_rows = []
                levels_short_rows = []

                all_dates = pd.date_range(start=start_date, end=end_date)
                for single_date in all_dates:
                    date_str = single_date.strftime("%Y-%m-%d")
                    url = f"https://api.fitbit.com/1.2/user/-/sleep/date/{date_str}.json"
                    try:
                        r = requests.get(url, headers=headers, timeout=30)
                        if r.status_code != 200:
                            # 詳細はZIPにログとしても書き出せるようにするならここで別途保持
                            continue
                        d = r.json()
                    except Exception:
                        continue
                        
                    sleeps = d.get("sleep", []) if isinstance(d, dict) else []
                    if not sleeps:
                        continue
                            
                    for s in sleeps:
                        # 基本フィールド
                        log_id = s.get("logId")
                        date_of_sleep = s.get("dateOfSleep")
                        start_time = s.get("startTime")
                        end_time = s.get("endTime")
                        duration = s.get("duration") # ms
                        efficiency = s.get("efficiency")
                        is_main = s.get("isMainSleep") # bool
                        log_type = s.get("logType")
                        typ = s.get("type") # STAGES or CLASSIC
                        info_code = s.get("infoCode")
                            
                        # 追加サマリー（全体）
                        minutes_asleep = s.get("minutesAsleep")
                        minutes_awake = s.get("minutesAwake")
                        minutes_after_wakeup = s.get("minutesAfterWakeup")
                        minutes_to_fall_asleep = s.get("minutesToFallAsleep")
                        time_in_bed = s.get("timeInBed")
                            
                            
                        # ステージサマリー
                        levels = s.get("levels", {}) if isinstance(s, dict) else {}
                        summary = levels.get("summary", {}) if isinstance(levels, dict) else {}
                            
                        # 動的に（deep/light/rem/wake など）存在するキーを列化
                        stage_minutes = {}
                        stage_counts = {}
                        for stage_key, val in summary.items():
                            if not isinstance(val, dict):
                                continue
                            stage_minutes[stage_key] = val.get("minutes")
                            stage_counts[stage_key] = val.get("count")

                        #合計分
                        total_stage_minutes = sum(
                            [m for m in stage_minutes.values() if isinstance(m, (int, float))]
                        )
                            
                        #ステージ比率（%）
                        stage_pct = {}
                        for k, m in stage_minutes.items():
                            if isinstance(m, (int, float)) and total_stage_minutes and total_stage_minutes > 0:
                                stage_pct[f"{k}_pct"] = round(m / total_stage_minutes * 100, 1)
                            else:
                                stage_pct[f"{k}_pct"] = None

                        #サマリー
                        row = {
                            "user_id": user_id,
                            "log_id": log_id,
                            "dateOfSleep": date_of_sleep,
                            "startTime": start_time,
                            "endTime": end_time,
                            "duration_ms": duration,
                            "efficiency": efficiency,
                            "isMainSleep": is_main,
                            "logType": log_type,
                            "type": typ,
                            "infoCode": info_code,
                            "minutesAsleep": minutes_asleep,
                            "minutesAwake": minutes_awake,
                            "minutesAfterWakeup": minutes_after_wakeup,
                            "minutesToFallAsleep": minutes_to_fall_asleep,
                            "timeInBed": time_in_bed,
                            "totalStageMinutes": total_stage_minutes,
                        }
                        # ステージ minutes/count/pct を結合
                        for k, v in stage_minutes.items():
                            row[f"{k}_minutes"] = v
                        for k, v in stage_counts.items():
                            row[f"{k}_count"] = v
                        row.update(stage_pct)
                        summary_rows.append(row)
                            
                        #詳細: levels.data (30sec)
                        data_list = levels.get("data", []) if isinstance(levels, dict) else []
                        for rec in data_list:
                            # 典型レコード: {"dateTime": "2025-09-19T00:15:00.000", "level": "light", "seconds": 30}
                            levels_data_rows.append(
                                {
                                    "user_id": user_id,
                                    "log_id": log_id,
                                    "dateOfSleep": date_of_sleep,
                                    "dateTime": rec.get("dateTime"),
                                    "level": rec.get("level"),
                                    "seconds": rec.get("seconds"),
                                }
                            )
                            
                        #詳細: levels.shorData
                        short_list = levels.get("shortData", []) if isinstance(levels, dict) else []
                        for rec in short_list:
                            levels_short_rows.append(
                                {
                                    "user_id": user_id,
                                    "log_id": log_id,
                                    "dateOfSleep": date_of_sleep,
                                    "dateTime": rec.get("dateTime"),
                                    "level": rec.get("level"),
                                    "seconds": rec.get("seconds")
                                }
                            )

                # ---- CSV 書き出し（ユーザー単位） ----
                if summary_rows:
                    df_sum = pd.DataFrame(summary_rows)
                    zipf.writestr(f"{user_id}_sleep_summary.csv",
                                  df_sum.to_csv(index=False).encode("utf-8-sig"))
                if levels_data_rows:
                    df_ld = pd.DataFrame(levels_data_rows)
                    zipf.writestr(f"{user_id}_sleep_levels_data.csv", 
                                  df_ld.to_csv(index=False).encode("utf-8-sig"))
                if levels_short_rows:
                    df_ls = pd.DataFrame(levels_short_rows)
                    zipf.writestr(f"{user_id}_sleep_levels_short.csv",
                                  df_ls.to_csv(index=False).encode("utf-8-sig"))

                                                           
                # ✅ トークンファイルもzipに保存
                zipf.writestr(f"token_{user_id}.json", 
                              json.dumps(token_data, indent=2, ensure_ascii=False))
                    
                status_area.success(
                    f"[{idx}/{total_users}] {user_id}: 取得完了（summary={len(summary_rows)}行, levels_data={len(levels_data_rows)}行, levels_short={len(levels_short_rows)}行)"
                )
                pbar.progress(int(idx / total_users * 100))
                
            # README（列説明の簡易版）
            readme = (
                "# Fitbit Sleep Export README\n"
                "- *_sleep_summary.csv*: 1ログ=1行（分割睡眠や昼寝も含む）。主な列:\n"
                " - duration_ms, efficiency, minutesAsleep, minutesAwake, minutesAfterWakeup, minutesToFallAsleep, timeInBed\n"
                " - deep/light/rem/wake _minutes / _count / _pct（存在するステージのみ）\n"
                " - isMainSleep, logType, type, infoCode\n"
                "- *_sleep_levels_data.csv*: 30秒刻みのステージ列（dateTime, level, seconds）。\n"
                "- *_sleep_levels_short.csv*: 短時間イベント列（dateTime, level, seconds）。\n"
                "\n注意: Fitbit Web APIはSleep Score（点数）を提供しません。efficiencyはFitbit定義の睡眠効率です。\n"
            )
            zipf.writestr("README.txt", readme)
                
        zip_buffer.seek(0)
        st.success("✅ データ取得が完了しました！以下からダウンロードしてください。")
        st.download_button(
            label="ZIPファイルをダウンロード",
            data=zip_buffer,
            file_name=f"fitbit_sleep_data_{start_date}_to_{end_date}.zip",
            mime="application/zip"
        )










