# %%
import os
from sqlalchemy import create_engine

from postal_latlon.postal_latlon import PostalLatLon

# %%
# Initialize Latlon
print(os.getcwd())
db_path = "postal_latlon_master.db"
ENGINE = create_engine("sqlite:///" + db_path)
pl = PostalLatLon(engine=ENGINE)

# %%
# Create data directory
pl.create_datadir()

# %%
# Create postal table
ken_all_csv_path = "data/input/utf_ken_all.csv"
pl.create_postal_master_table(ken_all_csv_path)


# %%
# Get postals
postals = pl.get_postals()

# %%
# Get latlons from API
response_path = "data/response/response_pred.txt"
pl.get_latlons_from_api(postals, response_path, sleep_time=1)


# %%
# sample
# postal = "3501305"
# response_dict = pl.get_latlon_from_api(postal)
# print(response_dict)
# pl.write_response(response_dict)


# %%
# error例
# postal = "3230060"
# response_dict = pl.get_latlon_from_api(postal)
# print(response_dict)
# pl.write_response(response_dict)


# %%
# 結果が複数返ってくる例
# postal = "3300000"
# response_dict = pl.get_latlon_from_api(postal)
# print(response_dict)
# pl.write_response(response_dict)
