import pandas as pd
from sqlalchemy import create_engine
import pickle
import requests, json
import time
import pickle
from datetime import datetime
import os
from tqdm import tqdm


class PostalLatLon(object):
    def __init__(self, engine, response_path="data/response/response_dev.txt"):
        self.engine = engine
        self.url = "http://geoapi.heartrails.com/api/json?method=searchByPostal&postal="
        self.response_path = response_path

    def create_datadir(self):
        if not os.path.exists("data"):
            os.mkdir("data")
        if not os.path.exists("data/input"):
            os.mkdir("data/input")
        if not os.path.exists("data/tmp"):
            os.mkdir("data/tmp")
        if not os.path.exists("data/output"):
            os.mkdir("data/output")
        if not os.path.exists("data/response"):
            os.mkdir("data/response")
        if not os.path.exists("data/manual_latlon"):
            os.mkdir("data/manual_latlon")

    def create_postal_master_table(self, ken_all_csv_path):
        """日本郵便のCSVを読み込んでDBに書き込む"""
        # 列名の設定
        columns = [
            "全国地方公共団体コード",
            "旧郵便番号",
            "postal",
            "都道府県名カナ",
            "市区町村名カナ",
            "町域名カナ",
            "pref",
            "city",
            "town",
            "一町域が二以上の郵便番号で表される場合の表示",
            "小字毎に番地が起番されている町域の表示",
            "丁目を有する町域の場合の表示",
            "一つの郵便番号で二以上の町域を表す場合の表示",
            "更新の表示",
            "変更理由",
        ]
        # data/input/utf_ken_all.csvを読み込む
        df = pd.read_csv(ken_all_csv_path, encoding="utf-8", header=None, names=columns)
        # postalcodeをstringにする
        df["postal"] = df["postal"].astype(str)
        # postalを0つめで7桁にする
        df["postal"] = df["postal"].str.zfill(7)
        # DataFrameをデータベースに書き込む
        df.to_sql("postal_master", self.engine, if_exists="replace", index=False)

    def get_postals(self, prefs=None):
        """dbからpostalを読み込む"""
        # postalcodeテーブルを読み込む
        df = pd.read_sql_table("postal_master", self.engine)
        if prefs:
            df = df[df["pref"].isin(prefs)]
        return df["postal"].tolist()

    def get_postal_table(self, prefs=None, postals=None):
        """dbからpostal,pref,city,townを読み込む"""
        # postalテーブルを読み込む
        df = pd.read_sql_table("postal_master", self.engine)
        if prefs:
            df = df[df["pref"].isin(prefs)]
        if postals:
            df = df[df["postal"].isin(postals)]
        # 列の絞り込み [郵便番号,都道府県名,市区町村名,町域名]
        df = df[["postal", "pref", "city", "town"]]
        return df

    def get_latlon_from_api(self, postal):
        """APIからlatlonを取得"""
        try:
            response_dict = requests.get(self.url + postal).json()["response"]
        except Exception as e:
            error_message = f"API request error: {postal}. Error: {str(e)}"
            response_dict = {"response_error": error_message}
        response_dict["request"] = postal
        return response_dict

    def write_response(self, response_dict):
        """APIのレスポンスをファイルに書き込む"""
        # txtに書き込み
        with open(self.response_path, "a") as f:
            f.write(f"{json.dumps(response_dict, ensure_ascii=False)}\n")

    def get_latlons_from_api(self, postals, sleep_time=2):
        # もしファイルが存在しない場合は新規作成
        if not os.path.exists(self.response_path):
            with open(self.response_path, "w") as f:
                f.write("")
        # APIからlatlonを取得
        for postal in tqdm(postals):
            response_dict = self.get_latlon_from_api(postal)
            self.write_response(response_dict)
            time.sleep(sleep_time)

    def get_response_dicts(self):
        """APIのレスポンスを読み込む"""
        # sample
        # {"location": [{"city": "狭山市", "city_kana": "さやまし", "town": "入間川", "town_kana": "いるまがわ", "x": "139.410559", "y": "35.854609", "prefecture": "埼玉県", "postal": "3501305"}], "request": "3501305"}
        # {"error": "Cities of postal code:'3230060' do not exist.", "request": "3230060"}
        # {"response_error": "API request error: 0600020. Error: Expecting value: line 1 column 1 (char 0)", "request": "0600020"}
        with open(self.response_path, "r") as f:
            response_lines = f.readlines()
        response_dicts = [json.loads(line) for line in response_lines]
        return response_dicts

    def get_postals_results(self, postals, response_dicts):
        """response_からpostalsごとの実行結果dictを取得する"""
        postals_result = {}
        for response_dict in response_dicts:
            if "location" in response_dict:
                postals_result[response_dict["request"]] = "success"
            elif "error" in response_dict:
                postals_result[response_dict["request"]] = "error"
            elif "response_error" in response_dict:
                postals_result[response_dict["request"]] = "response_error"
        not_yet_postals = set(postals) - set(postals_result.keys())
        for postal in not_yet_postals:
            postals_result[postal] = "not_yet"
        return postals_result

    def extract_postals(self, postals_results, result="response_error"):
        postals = [
            postal for postal, status in postals_results.items() if status == result
        ]
        # 件数を表示
        print(f"Number of {result}: {len(postals)}")
        return postals

    def get_postal_latlon_df(self, response_dicts):
        postals = []
        lats = []
        lons = []
        # prefs = []
        # citys = []
        # towns = []
        for response_dict in response_dicts:
            if "location" in response_dict:
                for place in response_dict["location"]:
                    postal = place["postal"]
                    # pref = place["prefecture"]
                    # city = place["city"]
                    # town = place["town"]
                    lat = place["y"]
                    lon = place["x"]
                    postals.append(postal)
                    # prefs.append(pref)
                    # citys.append(city)
                    # towns.append(town)
                    lats.append(lat)
                    lons.append(lon)
        df = pd.DataFrame(
            {
                "postal": postals,
                # "pref": prefs,
                # "city": citys,
                # "town": towns,
                "lat": lats,
                "lon": lons,
            }
        )
        df["lat"] = df["lat"].astype(float)
        df["lon"] = df["lon"].astype(float)
        # postalでグループ化し、lat,lonの平均を求める
        df = df.groupby("postal").agg({"lat": "mean", "lon": "mean"}).reset_index()
        return df

    def read_manual_latlon(self, manual_latlon_path):
        # excelを読み込み
        df = pd.read_excel(manual_latlon_path)
        df["postal"] = df["postal"].astype(str)
        return df

    def make_postal_latlon_master(self):
        postal = pd.read_sql_table("postal_master", self.engine)
        postalcode2latlon = pd.read_sql_table("postal_latlon", self.engine)
        df = pd.merge(postal, postalcode2latlon, on="postal", how="left")
        # 出力列を整形
        df = df[
            [
                "postal",
                "lat",
                "lon",
                "pref",
                "city",
                "town",
                "全国地方公共団体コード",
                # "旧郵便番号",
                # "都道府県名カナ",
                # "市区町村名カナ",
                # "町域名カナ",
                # "一町域が二以上の郵便番号で表される場合の表示",
                # "小字毎に番地が起番されている町域の表示",
                # "丁目を有する町域の場合の表示",
                # "一つの郵便番号で二以上の町域を表す場合の表示",
                # "更新の表示",
                # "変更理由",
            ]
        ]
        # postal_latlon_masterテーブルを作成
        df.to_sql("postal_latlon_master", self.engine, if_exists="replace", index=False)

    def read_postal_latlon_master(self):
        df = pd.read_sql_table("postal_latlon_master", self.engine)
        return df
