 import pandas as pd
import pickle
import feather as ft
import requests
import json
from itertools import product

import aggregater

catalogs = pd.read_excel("..\Machinery subsidy data\Catalog data\catalogs.xlsx", usecols= "A:M")
with open('../Machinery subsidy data/scrape.machinery.sub/all_data.pkl', 'rb') as k:
    all_data = pickle.load(k)
#%% calculate the size
catalogs['大类'] = catalogs['大类'].str.lstrip()
power = catalogs.loc[catalogs.大类 == "动力机械"].copy()

# get the max horsepower of each tractor type (fendang)
power['分档名称'] = power['分档名称'].str.replace("-", "—")
power['分档名称'] = power['分档名称'].str.replace("－", "—")
power['分档名称'] = power['分档名称'].str.replace("马力以上", "马力及以上")
power['分档名称'] = power['分档名称'].str.replace("\xa0马力", "马力")
power['分档名称'] = power['分档名称'].str.replace("\xa00", "0")

power['horsepowerrange'] = power['分档名称'].str.split(pat = '马', expand = True)[0]
power['horsepowermin'] = power['horsepowerrange'].str.split(pat = "—", expand = True)[0]
power['horsepowermax'] = power['horsepowerrange'].str.split(pat = "—", expand = True)[1]


power.loc[power['分档名称'].str.contains("马力以下"), "horsepowermax"] = power['分档名称'].str.split(pat = '马力以下', expand = True)[0]
power.loc[power['分档名称'].str.contains("马力以下"), "horsepowermin"] = 0

power.loc[power['分档名称'].str.contains("马力及以上"), "horsepowermin"] = power['分档名称'].str.split(pat = '马力及以上', expand = True)[0]
power.loc[power['分档名称'].str.contains("马力及以上"), "horsepowermax"] = power.horsepowermin


power.loc[power['horsepowermax'].isna(), "horsepowermax"] = power['基本配置和参数'].str.split(pat = '＜', expand = True)[1].str.split(pat = '马力', expand = True)[0]
power.loc[(power['horsepowermax'].isna() & power['基本配置和参数'].str.contains("≥")), "horsepowermin"] = power['基本配置和参数'].str.split(pat = '≥', expand = True)[1].str.split(pat = '马力', expand = True)[0]
power.loc[(power['horsepowermax'].isna() & power['基本配置和参数'].str.contains("≥")), "horsepowermax"] = power.horsepowermin


power["horsepowermax"] = pd.to_numeric(power['horsepowermax'], errors='coerce')
power["horsepowermin"] = pd.to_numeric(power['horsepowermin'], errors='coerce')
power['horsepower'] = (power["horsepowermax"]+ power["horsepowermin"])/2
power['中央补贴额(元)'] = pd.to_numeric(power['中央补贴额(元)'], errors='coerce') # get the central government subsidy level for each tractor type

# In the purchase data, there is no information about the exact tractor type.
# I can only guess the tractor type in the purchase records by matching the catalog on ['省','年', '品目','中央补贴额(元)'].
# However, there are duplicates in the catalog under these four variable keys. Mostly because different tractor sizes can receive the same subsidy level.
# This poses a difficulty in identifying the horsepower of the purchased tractor because it could be matched to more then one tractor type.
# Therefore, I take the average horsepower of the duplicates defined under ['省','年', '品目','中央补贴额(元)'] as the horsepower to be counted.
power['dp'] = power.duplicated(subset = ['省','年', '品目','中央补贴额(元)'], keep=False)
dup0 = power.loc[power['dp'] == True, ['省','年', '品目','中央补贴额(元)', 'horsepower']].copy()
dup0 = dup0.sort_values(['省','年', '中央补贴额(元)'])
dup1= dup0.groupby(by = ['省','年', '品目','中央补贴额(元)'],as_index = False).mean()

power = pd.merge(power, dup1, on = ['省','年', '品目','中央补贴额(元)'], how = 'left', validate="m:1")
power.loc[power.horsepower_y.isna(), 'horsepower_y'] = power.loc[power.horsepower_y.isna(), 'horsepower_x']
power['horsepower'] = power['horsepower_y']
#%% size code the catalog
sizes = pd.read_excel("..\Machinery subsidy data\Catalog data\拖拉机中央通用类分档.xlsx")
sizes['horsepowerrange'] = sizes['size'].str.split(pat = '马', expand = True)[0]
sizes['item_short'] = sizes.item.str.slice(stop = 2)
sizes.loc[(sizes['item_short'].str.contains('轮式') & sizes['size'].str.contains('两轮')), 'drive'] = 2
sizes.loc[(sizes['item_short'].str.contains('轮式') & sizes['size'].str.contains('四轮')),'drive'] = 4


sizes['size_code'] = pd.to_numeric(sizes['size_code'])
sizes['horsepowermax'] = sizes['horsepowerrange'].str.split(pat = "—", expand = True)[1]
sizes['horsepowermin'] = sizes['horsepowerrange'].str.split(pat = "—", expand = True)[0]

sizes.loc[sizes['size'].str.contains("马力以下"), "horsepowermax"] = sizes['size'].str.split(pat = '马力以下', expand = True)[0]
sizes.loc[sizes['size'].str.contains("马力以下"), "horsepowermin"] = 0

sizes.loc[sizes['size'].str.contains("马力及以上"), "horsepowermin"] = sizes['size'].str.split(pat = '马力及以上', expand = True)[0]
sizes.loc[sizes['size'].str.contains("马力及以上"), "horsepowermax"] = sizes.horsepowermin
sizes["horsepowermax"] = pd.to_numeric(sizes['horsepowermax'], errors='coerce')
sizes["horsepowermin"] = pd.to_numeric(sizes['horsepowermin'], errors='coerce')

sizes = sizes.drop_duplicates(subset = ['size'] )

power = pd.merge(power, sizes[['size','size_code']], how = 'left', left_on=['分档名称'], right_on= ['size'], validate = "m:1")
power['item_short'] = power.品目.str.slice(stop = 2)
power.loc[(power['item_short'].str.contains('轮式') & power['分档名称'].str.contains('两轮')), 'drive'] = 2
power.loc[(power['item_short'].str.contains('轮式') & power['分档名称'].str.contains('四轮')),'drive'] = 4

power = pd.merge(power, sizes[['item_short', 'horsepowermax','horsepowermin','size_code', 'drive']], how = 'left', on=['item_short', 'drive','horsepowermax','horsepowermin'], validate = "m:1")
power['size_code'] = power['size_code_x']
power.loc[power['size_code'].isna(), 'size_code'] = power['size_code_y']

power.loc[(power['horsepower_x'] >=8) & (power['horsepower_x'] <=10) & (power['item_short'] =='手扶'), 'size_code']= 40
power.loc[(power['horsepower_x'] > 10) & (power['horsepower_x'] <20) & (power['item_short'] =='手扶'), 'size_code']= 41

power.size_code = power.size_code.astype('category')

q = power.query("size_code.isna()")
q = q.loc[~(q.item_short.str.contains("手扶"))]
q = q.loc[~(q.分档名称.str.contains("型  号"))]
q = q.loc[q.年>2014]
q = q.loc[~(q['目录类型']=="非通用类")] # missing size_code
power['size'] = power['size_code']

#%% size code the purchase records
purchase_power = all_data.query("category_EN == 'tractor and power'").copy()

purchase_power['unit_subsidy'] = pd.to_numeric(purchase_power['unit_subsidy'], errors='coerce')
purchase_power.loc[purchase_power['unit_subsidy'].isna(), 'unit_subsidy'] = purchase_power['central_gov_subsidy']

purchase_power['item_short'] = purchase_power.item_matched.str.slice(stop = 2)
purchase_power = pd.merge(purchase_power[['adcode','province', 'year', 'item_short', 'unit_subsidy', 'central_gov_subsidy', 'local_gov_subsidy', 'retail_price']],
                power[['省','年', '品目','中央补贴额(元)','horsepowermin','horsepowermax','horsepower_x','horsepower_y', 'size', 'item_short', 'drive']].drop_duplicates(['省','年', 'item_short','中央补贴额(元)']),
                how = 'left',
                left_on = ['province', 'year', 'item_short', 'unit_subsidy'],
                right_on=['省','年', 'item_short','中央补贴额(元)'],
                validate='m:1')

# match with the previous year subsidy
view1 = purchase_power.loc[purchase_power['size'].isna(), ['adcode','province', 'year', 'item_short', 'unit_subsidy', 'central_gov_subsidy', 'local_gov_subsidy', 'retail_price']].copy(deep= True)
power1= power.copy()
power1.年 = power1.年 + 1
view1 = view1.merge(
                power1[['省','年', '品目','中央补贴额(元)','horsepowermin','horsepowermax','horsepower_x','horsepower_y', 'size', 'item_short', 'drive']].drop_duplicates(['省','年', 'item_short','中央补贴额(元)']),
                how = 'left',
                left_on = ['province', 'year', 'item_short', 'unit_subsidy'],
                right_on=['省','年', 'item_short','中央补贴额(元)'], validate='m:1').set_axis(view1.index)
purchase_power.update(view1)

# match with subsidy two years ago
view1 = purchase_power.loc[purchase_power['size'].isna(), ['adcode','province', 'year', 'item_short', 'unit_subsidy', 'central_gov_subsidy', 'local_gov_subsidy', 'retail_price']].copy()
power1= power.copy()
power1.年 = power1.年 + 2
view1 = pd.merge(view1,
                power1[['省','年', '品目','中央补贴额(元)','horsepowermin','horsepowermax','horsepower_x','horsepower_y', 'size', 'item_short', 'drive']].drop_duplicates(['省','年', 'item_short','中央补贴额(元)']),
                how = 'left',
                left_on = ['province', 'year', 'item_short', 'unit_subsidy'],
                right_on=['省','年', 'item_short','中央补贴额(元)'], validate='m:1').set_axis(view1.index)
purchase_power.update(view1)
#%% these are the ones where the size is not coded, including some large size tractors, manually code them
view1 = purchase_power.loc[purchase_power['size'].isna(), ['adcode', 'province', 'year', 'item_short', 'unit_subsidy', 'central_gov_subsidy',
                                  'local_gov_subsidy', 'retail_price']].copy()
pd.crosstab(view1.province, view1.year)
purchase_power.loc[(purchase_power.item_short == '轮式') & (purchase_power.drive ==4) & (purchase_power.horsepower_x == 160), 'size'] =23
purchase_power.loc[(purchase_power.item_short == '轮式') & (purchase_power.drive ==4) & (purchase_power.horsepower_x == 180), 'size'] =24
purchase_power.loc[(purchase_power.item_short == '轮式') & (purchase_power.drive ==4) & (purchase_power.horsepower_x == 102.5), 'size'] =20
purchase_power.loc[(purchase_power.item_short == '轮式') & (purchase_power.drive ==4) & (purchase_power.horsepower_x == 195), 'size'] =24
purchase_power.loc[(purchase_power.item_short == '轮式') & (purchase_power.drive ==4) & (purchase_power.horsepower_x == 90), 'size'] =19
purchase_power.loc[(purchase_power.item_short == '轮式') & (purchase_power.drive ==4) & (purchase_power.horsepower_x == 120), 'size'] =21
purchase_power.loc[(purchase_power.item_short == '轮式') & (purchase_power.drive ==4) & (purchase_power.horsepowermin == 70), 'size'] =17
purchase_power.loc[(purchase_power.item_short == '履带') & (purchase_power.horsepowermin == 50), 'size'] =27
purchase_power.loc[(purchase_power.item_short == '履带') & (purchase_power.horsepower_x >70) & (purchase_power.horsepower_x < 90), 'size'] =29
purchase_power.loc[(purchase_power.item_short == '履带') & (purchase_power.horsepower_x ==90), 'size'] =31
purchase_power.loc[(purchase_power.item_short == '履带') & (purchase_power.horsepower_x ==70), 'size'] =29
purchase_power.loc[(purchase_power.item_short == '履带') & (purchase_power.horsepower_x ==100), 'size'] =32
purchase_power.loc[(purchase_power.item_short == '履带') & (purchase_power.horsepower_x ==60), 'size'] =28

purchase_power['size'] = pd.to_numeric(purchase_power['size'])
view1 = purchase_power.loc[purchase_power['size'].isna(), ['adcode','province', 'year', 'item_short', 'unit_subsidy', 'central_gov_subsidy', 'local_gov_subsidy', 'retail_price']].copy()
pd.crosstab(view1.province, view1.year) # now the unmatched ones are less than 5% of the total purchase records



#%% Aggregate to the county level
purchase_power.loc[purchase_power.horsepower_y.isna(), 'horsepower_y'] = purchase_power.loc[purchase_power.horsepower_y.isna(), 'horsepower_x']

purchase_power['purchase'] = purchase_power['horsepower_y']

machinery_data_power = aggregater.aggregate_purchase_to_county(purchase_power)

#%% Aggregate to the city level
machinery_data_power = machinery_data_power + aggregater.aggregate_purchase_to_city(purchase_power)

#%% subsidy_{jpt}
machinery_data_power.append(aggregater.get_shocks(power, purchase_power))

#%% Wrap up and save
with open('dataprocessing/output/machinery_data_power.pkl', 'wb') as f:
    pickle.dump(machinery_data_power, f)


ft.write_dataframe(machinery_data_power[4], 'dataprocessing/output/power/city_year_size.feather')
ft.write_dataframe(machinery_data_power[5], 'dataprocessing/output/power/city_year_size_shares.feather')
ft.write_dataframe(machinery_data_power[1], 'dataprocessing/output/power/county_year_size.feather')
ft.write_dataframe(machinery_data_power[2], 'dataprocessing/output/power/county_year_size_shares.feather')
ft.write_dataframe(machinery_data_power[6], 'dataprocessing/output/power/shock.feather')
ft.write_dataframe(machinery_data_power[3], 'dataprocessing/output/power/city_year.feather')
ft.write_dataframe(machinery_data_power[0], 'dataprocessing/output/power/county_year.feather')










