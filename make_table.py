# %%
import os
from sqlalchemy import create_engine
import pandas as pd
from postal_latlon.postal_latlon import PostalLatLon

# %%
# Initialize Latlon
print(os.getcwd())
db_path = "data/output/postal_latlon_master.db"
ENGINE = create_engine("sqlite:///" + db_path)
response_path = "data/response/response_pred.txt"
pl = PostalLatLon(engine=ENGINE, response_path=response_path)

# %%
# Get postals
postals = pl.get_postals()
print(len(postals))


# %%
# result check
response_dicts = pl.get_response_dicts()
postals_results = pl.get_postals_results(postals, response_dicts)

success_postals = pl.extract_postals(postals_results, result="success")
error_postals = pl.extract_postals(postals_results, result="error")
response_error_postals = pl.extract_postals(postals_results, result="response_error")
not_yet_postals = pl.extract_postals(postals_results, result="not_yet")


# %%
# 再実行 response_errorは再実行で成功した
# postals = response_error_postals
# pl.get_latlons_from_api(postals, sleep_time=1)


# %%
# 再実行 error_postalsは再実行しても件数が減らなかった
# postals = error_postals
# pl.get_latlons_from_api(postals, sleep_time=1)


# %%
# error例をexcel出力
# error_df = pl.get_postal_table(postals=error_postals)
# error_df.to_excel("data/tmp/error_postals.xlsx")


# %%
# postalとlatlonのテーブルを作成
df = pl.get_postal_latlon_df(response_dicts)
print(df)


# %%
# error例の緯度経度を手入力したものをよみこみ
manual_latlon_path = "data/manual_latlon/manual_latlon.xlsx"
df2 = pl.read_manual_latlon(manual_latlon_path)
print(df2)


# %%
df3 = pd.concat([df, df2], axis=0)
df3.to_sql("postal_latlon", ENGINE, if_exists="replace", index=False)


# %%
# postal_masterとpostal_latlonを結合して、postal_latlon_masterを作成
pl.make_postal_latlon_master()


# %%
# 最終結果
postal_latlon_master = pd.read_sql_table("postal_latlon_master", ENGINE)
print(postal_latlon_master)
postal_latlon_master.to_excel("data/output/postal_latlon_master.xlsx")
# %%
