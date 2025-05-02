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

def refresh_access_token(token_data):
    refresh_url = "https://api.fitbit.com/oauth2/token"
    
    # ✅ client_id:client_secret を base64 エンコード
    credentials = f"{token_data['client_id']}:{token_data['client_secret']}"
    credentials_b64 = base64.b64encode(credentials.encode()).decode()
    
    headers = {
        "Authorization": f"Basic {credentials_b64}",  # ← 修正済みの base64 文字列を使う
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "grant_type": "refresh_token",
        "refresh_token": token_data["refresh_token"]
    }
    response = requests.post(refresh_url, headers=headers, data=data)
    if response.status_code == 200:
        new_token = response.json()
        token_data.update(new_token)
        return token_data
    else:
        # 👇 エラー内容を Streamlit に表示する
        st.error(f"トークン更新失敗: {response.status_code} - {response.text}")
        return None

st.set_page_config(page_title="Fitbit睡眠データ一括取得", page_icon="📊")
st.title("📊 Fitbit睡眠データ一括取得")

st.markdown("""
このページでは、被験者のFitbitトークン（アカウント情報）をもとに、指定期間の睡眠データを一括取得します。

### 📝 操作手順
0．研究対象者から提出いただいたファイル名は、研究対象者識別番号が含まれています。これを、解析用識別番号に変換してください。
> 例 「token_T001.json」 → 「token_Y001.json」
> ※トークンファイルは `token_解析ID.json` という形式で保存されている必要があります。


1. 取得したい期間（試験期間）を選択してください。


2. ファイル名を変更したファイルをアップロードしてください。


3. 「一括取得＆ZIPでダウンロード」ボタンを押してください。



""")

start_date = st.date_input("取得開始日", value=date.today() - timedelta(days=7))
end_date = st.date_input("取得終了日", value=date.today())
uploaded_files = st.file_uploader("トークンファイル（複数選択可）をアップロード", type="json", accept_multiple_files=True)

if st.button("データ取得を開始"):
    if not uploaded_files:
        st.warning("少なくとも1つのトークンファイルをアップロードしてください。")
    else:
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
            for uploaded_file in uploaded_files:
                user_id = uploaded_file.name.replace("token_", "").replace(".json", "")
                token_data = json.load(uploaded_file)

                # トークン更新
                token_data = refresh_access_token(token_data)
                if not token_data:
                    st.warning(f"{user_id} のトークン更新に失敗しました。スキップします。")
                    continue

                access_token = token_data.get("access_token")
                headers = {"Authorization": f"Bearer {access_token}"}
                all_days = []

                for single_date in pd.date_range(start=start_date, end=end_date):
                    date_str = single_date.strftime("%Y-%m-%d")
                    url = f"https://api.fitbit.com/1.2/user/-/sleep/date/{date_str}.json"
                    r = requests.get(url, headers=headers)
                    d = r.json()
                    if "sleep" not in d or len(d["sleep"]) == 0:
                        st.warning(f"{user_id} の {date_str} にデータがありませんでした。レスポンス: {d}")

                    if "sleep" in d and len(d["sleep"]) > 0:
                        s = d["sleep"][0]
                        levels = s.get("levels", {}).get("summary", {})
                        total = sum(level.get("minutes", 0) for level in levels.values())
                        row = {
                            "date": date_str,
                            "sleep_score": s.get("efficiency"),
                            "start_time": s.get("startTime"),
                            "end_time": s.get("endTime")
                        }
                        for k in ["deep", "light", "rem", "wake"]:
                            minutes = levels.get(k, {}).get("minutes", 0)
                            row[f"{k}_minutes"] = minutes
                            row[f"{k}_pct"] = round((minutes / total * 100), 1) if total > 0 else 0
                        all_days.append(row)

                if all_days:
                    df = pd.DataFrame(all_days)
                    csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
                    zipf.writestr(f"{user_id}_sleep_data.csv", csv_bytes)
                    
                    # ✅ トークンファイルもzipに保存（更新済）
                    updated_token_json = json.dumps(token_data, indent=2, ensure_ascii=False)
                    zipf.writestr(f"token_{user_id}.json", updated_token_json)

        zip_buffer.seek(0)
        st.success("✅ データ取得が完了しました！以下からダウンロードしてください。")
        st.download_button(
            label="ZIPファイルをダウンロード",
            data=zip_buffer,
            file_name=f"fitbit_sleep_data_{start_date}_to_{end_date}.zip",
            mime="application/zip"
        )
