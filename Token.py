import streamlit as st
import pandas as pd
import json
import requests
from datetime import date, timedelta

st.set_page_config(page_title="Fitbit睡眠データ取得", page_icon="🛌")
st.title("🛌 Fitbit睡眠データ取得ツール")
st.markdown("""
このツールでは、Fitbitの認証トークンを使って、指定日の睡眠データを取得し、CSVとして保存できます。
""")

# 入力フォーム
解析用ID = st.text_input("解析用IDを入力してください")
取得日 = st.date_input("取得したい日付を選んでください", value=date.today() - timedelta(days=1))
uploaded_file = st.file_uploader("トークンファイル（token_XXX.json）をアップロード", type="json")

if uploaded_file and 解析用ID:
    if st.button("データを取得してCSVをダウンロード"):
        try:
            token_data = json.load(uploaded_file)
            access_token = token_data["access_token"]

            date_str = 取得日.strftime("%Y-%m-%d")
            url = f"https://api.fitbit.com/1.2/user/-/sleep/date/{date_str}.json"
            headers = {"Authorization": f"Bearer {access_token}"}
            response = requests.get(url, headers=headers)
            data = response.json()

            if "sleep" in data and len(data["sleep"]) > 0:
                sleep_data = data["sleep"][0]["levels"]["data"]
                df = pd.DataFrame(sleep_data)
                df["minutes"] = df["seconds"] / 60

                csv = df.to_csv(index=False).encode("utf-8-sig")
                st.success("データ取得に成功しました！")
                st.download_button(
                    label="CSVをダウンロード",
                    data=csv,
                    file_name=f"{解析用ID}_sleep_{date_str}.csv",
                    mime="text/csv"
                )
            else:
                st.warning("指定日に睡眠データが存在しません。Fitbitアプリをご確認ください。")

        except Exception as e:
            st.error(f"エラーが発生しました: {str(e)}")
else:
    st.info("上の3項目をすべて入力してください。")
