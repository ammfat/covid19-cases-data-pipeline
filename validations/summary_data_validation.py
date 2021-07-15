import os
from datetime import datetime
import glob
import great_expectations as ge
import pandas as pd

project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
partition_date = datetime.today().strftime("%Y%m%d")

files = glob.glob(os.path.join(project_dir, f"output/summary_by_province_{partition_date}/*.csv"))
list_of_csv = []

for filename in files:
    df = pd.read_csv(filename)
    list_of_csv.append(df)

data_frame = pd.concat(list_of_csv)
summary_df = ge.from_pandas(data_frame)

check_null = summary_df.expect_column_values_to_not_be_null(column='Province')
assert check_null['success'] == True, \
    f"Column 'Province' expected to not be null. Test result:\n{check_null}"

check_unique = summary_df.expect_column_values_to_be_unique(column='Province')
assert check_unique['success'] == True, \
    f"Column 'Province''s value expected to be unique. Test result:\n{check_unique}"

list_of_provinces = [
    'DKI JAKARTA'
    , 'JAWA BARAT'
    , 'JAWA TIMUR'
    , 'JAWA TENGAH'
    , 'SULAWESI SELATAN'
    , 'BANTEN'
    , 'NUSA TENGGARA BARAT'
    , 'BALI'
    , 'PAPUA'
    , 'KALIMANTAN SELATAN'
    , 'SUMATERA BARAT'
    , 'SUMATERA SELATAN'
    , 'KALIMANTAN TENGAH'
    , 'KALIMANTAN TIMUR'
    , 'SUMATERA UTARA'
    , 'DAERAH ISTIMEWA YOGYAKARTA'
    , 'KALIMANTAN UTARA'
    , 'KEPULAUAN RIAU'
    , 'KALIMANTAN BARAT'
    , 'SULAWESI TENGGARA'
    , 'LAMPUNG'
    , 'SULAWESI UTARA'
    , 'SULAWESI TENGAH'
    , 'RIAU'
    , 'PAPUA BARAT'
    , 'SULAWESI BARAT'
    , 'JAMBI'
    , 'MALUKU UTARA'
    , 'MALUKU'
    , 'GORONTALO'
    , 'KEPULAUAN BANGKA BELITUNG'
    , 'ACEH'
    , 'BENGKULU'
    , 'NUSA TENGGARA TIMUR'
]

check_province = summary_df.expect_column_values_to_be_in_set(column='Province', value_set=list_of_provinces)     # It's cAsE sENsITivE. ACEH != ACeh
assert check_province['success'] == True, \
    f"There unexpected data in 'Province'. Test result:\n{check_province}"

list_of_zones = [
    'Green'
    , 'Yellow'
    , 'Orange'
    , 'Red'
]

check_zone = summary_df.expect_column_values_to_be_in_set(column='Zone', value_set=list_of_zones)
assert check_zone['success'] == True, \
    f"There unexpected data in 'Zone'. Test result:\n{check_zone}"
