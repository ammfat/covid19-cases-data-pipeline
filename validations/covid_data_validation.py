import os
import great_expectations as ge

project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
case_df = ge.read_csv(os.path.join(project_dir, 'data/Indonesia_coronavirus_daily_data.csv'))

check_null = case_df.expect_column_values_to_not_be_null(column='Province')
assert check_null['success'] == True, \
    f"Column 'Province' expected to not be null. Test result:\n{check_null}"

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

check_province = case_df.expect_column_values_to_be_in_set(column='Province', value_set=list_of_provinces)     # It's cAsE sENsITivE. ACEH != ACeh
assert check_province['success'] == True, \
    f"There unexpected data in 'Province'. Test result:\n{check_province}"

case_df['Total_Cumulative'] = case_df['Cumulative_Recovered'] + case_df['Cumulative_Death']\
                                + case_df['Cumulative_Active_Case']

check_cumulative_case = case_df.expect_column_pair_values_to_be_equal(column_A = 'Total_Cumulative',
                                                            column_B = 'Cumulative_Case')
assert check_cumulative_case['success'] == True, \
    f"Invalid Cumulative_Case. Test result:\n{check_cumulative_case}"

