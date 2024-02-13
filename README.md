# 郵便番号7桁から緯度経度を取得

## 概要

日本郵便から郵便番号一覧を取得し、APIで郵便番号7桁から緯度経度を取得し結合

## Script

- latlon/latlon.py
- run_api.py
- make_table.py

## 最終生成物

data/output/postal_latlon_master.db(sqlite)

tableは以下の3つ

- postal_masterは郵便番号一覧
- latlonはAPIで取得したdata一覧
- postal_latlon_masterは郵便番号一覧と緯度経度を結合したもの

## DataSource

### 郵便番号一覧

住所の郵便番号（1レコード1行、UTF-8形式）（CSV形式）

2024年1月31日更新

### API

HeartRailesGeoAPI

https://geoapi.heartrails.com/


### APIでHITしなかった郵便番号は、緯度経度を手動で入力

- data/manual_latlon/manual_latlon.xlsx

