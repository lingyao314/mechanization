import pandas as pd
import pickle
import feather as ft
import requests
import json
#%%
# Statistical yearbook data
with open('C:/Users/yaoan/OneDrive/解压数据/1998～2020年中国城市统计年鉴地级市面板数据.dta', 'rb') as m:
    employment_city0 = pd.read_stata(m)
with open('../China Statistical Year Books/county level/employment_data.pkl', 'rb') as d:
    employment_county = pickle.load(d)
# CHARLS data
with open ('dataprocessing/output/family.dta', 'rb') as c:
    employment_individual = pd.read_stata(c)
# %% Secondary and tertiary industry employment by prefecture city (2003 and 2019)
employment_city = employment_city0[['第二产业_采矿业从业人员_人_全市',  '第二产业_制造业从业人员_人_全市',  '第二产业_电力热力燃气及水生产和供应业从业人员_人_全市',  '第二产业_建筑业从业人员_人_全市',
                                   '第二产业从业人员_人_全市', '第二产业从业人员_人_市辖区',
                                   '第三产业_科学研究和技术服务和地质勘探业_人_全市', '第三产业_交通运输仓储和邮政业_人_全市', '第三产业_批发和零售业_人_全市', '第三产业_金融业_人_全市', '第三产业_房地产业_人_全市', '第三产业_社会服务业_全市',
                                   '第三产业_卫生体育社会福利业_人_全市', '第三产业_教育文艺广播影视业_人_全市', '第三产业_信息传输计算机服务和软件业_人_全市', '第三产业_住宿和餐饮业_人_全市', '第三产业_租赁和商务服务业_人_全市',
                                   '第三产业_水利环境和公共设施管理业_人_全市', '第三产业_居民服务修理和其他服务业_人_全市', '第三产业_教育_人_全市', '第三产业_卫生社会保障和社会福利业_人_全市', '第三产业_文化体育和娱乐业_人_全市',
                                   '第三产业_公共管理和社会组织_人_全市', '第三产业_科学研究和技术服务业_人_全市', '第三产业_卫生和社会工作_人_全市', '第三产业_公共管理社会保障和社会组织_人_全市',
                                   '第三产业从业人员_人_全市', '第三产业从业人员_人_市辖区',
                                   '城镇单位从业人员期末人数_人_全市', '城镇单位从业人员期末人数_人_市辖区', '城镇私营和个体从业人员_人_全市',
                                   '城镇私营和个体从业人员_人_市辖区', '年份', '省', '市', '市代码','规模以上工业企业数_个_全市']]
employment_city = employment_city.astype({'市代码': int}, copy=False)
employment_city = employment_city.rename(columns={"市代码": "city_code", '年份': 'year', '市': 'city'})

employment_city['secondary'] = employment_city[['第二产业_制造业从业人员_人_全市',  '第二产业_电力热力燃气及水生产和供应业从业人员_人_全市',  '第二产业_建筑业从业人员_人_全市']].sum(axis =1) # not including mining
employment_city['tertiary'] = employment_city['第三产业从业人员_人_全市']
employment_city.loc[employment_city['tertiary'].isna(), 'tertiary'] = employment_city[['第三产业_科学研究和技术服务和地质勘探业_人_全市', '第三产业_交通运输仓储和邮政业_人_全市', '第三产业_批发和零售业_人_全市', '第三产业_金融业_人_全市', '第三产业_房地产业_人_全市', '第三产业_社会服务业_全市',
                                   '第三产业_卫生体育社会福利业_人_全市', '第三产业_教育文艺广播影视业_人_全市', '第三产业_信息传输计算机服务和软件业_人_全市', '第三产业_住宿和餐饮业_人_全市', '第三产业_租赁和商务服务业_人_全市',
                                   '第三产业_水利环境和公共设施管理业_人_全市', '第三产业_居民服务修理和其他服务业_人_全市', '第三产业_教育_人_全市', '第三产业_卫生社会保障和社会福利业_人_全市', '第三产业_文化体育和娱乐业_人_全市',
                                   '第三产业_公共管理和社会组织_人_全市', '第三产业_科学研究和技术服务业_人_全市', '第三产业_卫生和社会工作_人_全市', '第三产业_公共管理社会保障和社会组织_人_全市']].sum(axis =1)


employment_city['nonag'] = (employment_city['secondary'] + employment_city['tertiary'])/1000
employment_city.loc[employment_city['nonag'] == 0, 'nonag'] = None
employment_city['manufacturing'] = employment_city['第二产业_制造业从业人员_人_全市']/1000
employment_city['construction'] = employment_city['第二产业_建筑业从业人员_人_全市']/1000
employment_city['wholesaleretail'] = employment_city['第三产业_批发和零售业_人_全市']/1000
employment_city['accommodationfood'] = employment_city['第三产业_住宿和餐饮业_人_全市']/1000
employment_city['logistic'] = employment_city['第三产业_交通运输仓储和邮政业_人_全市']/1000
employment_city['industrial_firms'] = employment_city['规模以上工业企业数_个_全市']


employment_city['city_code'] = employment_city['city_code'].astype(str)
employment_city['city_code'] = employment_city['city_code'].str[0:4]
employment_city['prv_code'] = employment_city['city_code'].str[0:2]
employment_city['period'] = pd.cut(employment_city['year'], range(2014, 2021, 3))


# employment_city = employment_city.query("year > 2015 and year < 2019") # match the years in the regression
#
# employment_city['nonag_diff'] = employment_city.sort_values(by = 'year').groupby(by = 'city_code')['nonag'].diff()
# employment_city['manufacturing_diff'] = employment_city.sort_values(by = 'year').groupby(by = 'city_code')['manufacturing'].diff()
# employment_city['construction_diff'] = employment_city.sort_values(by = 'year').groupby(by = 'city_code')['construction'].diff()
# employment_city['wholesaleretail_diff'] = employment_city.sort_values(by = 'year').groupby(by = 'city_code')['wholesaleretail'].diff()
# employment_city['accommodationfood_diff'] = employment_city.sort_values(by = 'year').groupby(by = 'city_code')['accommodationfood'].diff()
# employment_city['logistic_diff'] = employment_city.sort_values(by = 'year').groupby(by = 'city_code')['logistic'].diff()
#
# temp = employment_city[['city_code', 'nonag_diff']].groupby(['city_code'],as_index = False).mean()
# temp.columns = ['city_code', 'nonag_diff_ct_mean']
# employment_city = pd.merge(employment_city, temp, on='city_code', how='left', validate='m:1')
# employment_city['nonag_diff_ct_demeand'] = employment_city['nonag_diff'] - employment_city['nonag_diff_ct_mean']
# temp = employment_city[['period', 'nonag_diff_ct_demeand']].groupby(['period'],as_index = False).mean()
# temp.columns = ['period', 'nonag_diff_ct_demeand_prd_mean']
# employment_city = pd.merge(employment_city, temp, on='period', how='left', validate='m:1')
# employment_city['nonag_diff_ct_demeand_prd_demeand'] = employment_city['nonag_diff_ct_demeand'] - employment_city['nonag_diff_ct_demeand_prd_mean']
#
# temp = employment_city[['city_code', 'manufacturing_diff']].groupby(['city_code'],as_index = False).mean()
# temp.columns = ['city_code', 'manufacturing_diff_ct_mean']
# employment_city = pd.merge(employment_city, temp, on='city_code', how='left', validate='m:1')
# employment_city['manufacturing_diff_ct_demeand'] = employment_city['manufacturing_diff'] - employment_city['manufacturing_diff_ct_mean']
# temp = employment_city[['period', 'manufacturing_diff_ct_demeand']].groupby(['period'],as_index = False).mean()
# temp.columns = ['period', 'manufacturing_diff_ct_demeand_prd_mean']
# employment_city = pd.merge(employment_city, temp, on='period', how='left', validate='m:1')
# employment_city['manufacturing_diff_ct_demeand_prd_demeand'] = employment_city['manufacturing_diff_ct_demeand'] - employment_city['manufacturing_diff_ct_demeand_prd_mean']
#
# temp = employment_city[['city_code', 'construction_diff']].groupby(['city_code'],as_index = False).mean()
# temp.columns = ['city_code', 'construction_diff_ct_mean']
# employment_city = pd.merge(employment_city, temp, on='city_code', how='left', validate='m:1')
# employment_city['construction_diff_ct_demeand'] = employment_city['construction_diff'] - employment_city['construction_diff_ct_mean']
# temp = employment_city[['period', 'construction_diff_ct_demeand']].groupby(['period'],as_index = False).mean()
# temp.columns = ['period', 'construction_diff_ct_demeand_prd_mean']
# employment_city = pd.merge(employment_city, temp, on='period', how='left', validate='m:1')
# employment_city['construction_diff_ct_demeand_prd_demeand'] = employment_city['construction_diff_ct_demeand'] - employment_city['construction_diff_ct_demeand_prd_mean']
#
# temp = employment_city[['city_code', 'wholesaleretail_diff']].groupby(['city_code'],as_index = False).mean()
# temp.columns = ['city_code', 'wholesaleretail_diff_ct_mean']
# employment_city = pd.merge(employment_city, temp, on='city_code', how='left', validate='m:1')
# employment_city['wholesaleretail_diff_ct_demeand'] = employment_city['wholesaleretail_diff'] - employment_city['wholesaleretail_diff_ct_mean']
# temp = employment_city[['period', 'wholesaleretail_diff_ct_demeand']].groupby(['period'],as_index = False).mean()
# temp.columns = ['period', 'wholesaleretail_diff_ct_demeand_prd_mean']
# employment_city = pd.merge(employment_city, temp, on='period', how='left', validate='m:1')
# employment_city['wholesaleretail_diff_ct_demeand_prd_demeand'] = employment_city['wholesaleretail_diff_ct_demeand'] - employment_city['wholesaleretail_diff_ct_demeand_prd_mean']
#
# temp = employment_city[['city_code', 'accommodationfood_diff']].groupby(['city_code'],as_index = False).mean()
# temp.columns = ['city_code', 'accommodationfood_diff_ct_mean']
# employment_city = pd.merge(employment_city, temp, on='city_code', how='left', validate='m:1')
# employment_city['accommodationfood_diff_ct_demeand'] = employment_city['accommodationfood_diff'] - employment_city['accommodationfood_diff_ct_mean']
# temp = employment_city[['period', 'accommodationfood_diff_ct_demeand']].groupby(['period'],as_index = False).mean()
# temp.columns = ['period', 'accommodationfood_diff_ct_demeand_prd_mean']
# employment_city = pd.merge(employment_city, temp, on='period', how='left', validate='m:1')
# employment_city['accommodationfood_diff_ct_demeand_prd_demeand'] = employment_city['accommodationfood_diff_ct_demeand'] - employment_city['accommodationfood_diff_ct_demeand_prd_mean']
#
# temp = employment_city[['city_code', 'logistic_diff']].groupby(['city_code'],as_index = False).mean()
# temp.columns = ['city_code', 'logistic_diff_ct_mean']
# employment_city = pd.merge(employment_city, temp, on='city_code', how='left', validate='m:1')
# employment_city['logistic_diff_ct_demeand'] = employment_city['logistic_diff'] - employment_city['logistic_diff_ct_mean']
# temp = employment_city[['period', 'logistic_diff_ct_demeand']].groupby(['period'],as_index = False).mean()
# temp.columns = ['period', 'logistic_diff_ct_demeand_prd_mean']
# employment_city = pd.merge(employment_city, temp, on='period', how='left', validate='m:1')
# employment_city['logistic_diff_ct_demeand_prd_demeand'] = employment_city['logistic_diff_ct_demeand'] - employment_city['logistic_diff_ct_demeand_prd_mean']

#%%
employment_county[['employment_2ind','employment_3ind','industrial_firms']] = employment_county[['employment_2ind','employment_3ind', 'industrial_firms']].apply(pd.to_numeric, errors = 'coerce')
employment_county['nonag'] = (employment_county['employment_2ind'] + employment_county['employment_3ind'])/1000
employment_county = employment_county.astype({'year': int})
employment_county= employment_county.query("year > 2014 ")
# employment_county['nonag_diff'] = employment_county.sort_values(by = 'year').groupby(by = 'adcode')['nonag'].diff()
# employment_county['industrial_firms_diff'] = employment_county.sort_values(by = 'year').groupby(by = 'adcode')['industrial_firms'].diff()
#
# employment_county= employment_county.query("year > 2015 and year < 2019") # match the years in the regression
#
# temp = employment_county[['adcode', 'nonag_diff']].groupby(['adcode'],as_index = False).mean()
# temp.columns = ['adcode', 'nonag_diff_ct_mean']
# employment_county = pd.merge(employment_county, temp, on='adcode', how='left', validate='m:1')
# employment_county['nonag_diff_ct_demeand'] = employment_county['nonag_diff'] - employment_county['nonag_diff_ct_mean']
# employment_county['period'] = pd.cut(employment_county['year'], range(2014, 2021, 3))
# temp = employment_county[['period', 'nonag_diff_ct_demeand']].groupby(['period'],as_index = False).mean()
# temp.columns = ['period', 'nonag_diff_ct_demeand_prd_mean']
# employment_county = pd.merge(employment_county, temp, on='period', how='left', validate='m:1')
# employment_county['nonag_diff_ct_demeand_prd_demeand'] = employment_county['nonag_diff_ct_demeand'] - employment_county['nonag_diff_ct_demeand_prd_mean']
#
# temp = employment_county[['adcode', 'industrial_firms_diff']].groupby(['adcode'],as_index = False).mean()
# temp.columns = ['adcode', 'industrial_firms_diff_ct_mean']
# employment_county = pd.merge(employment_county, temp, on='adcode', how='left', validate='m:1')
# employment_county['industrial_firms_diff_ct_demeand'] = employment_county['industrial_firms_diff'] - employment_county['industrial_firms_diff_ct_mean']
# employment_county['period'] = pd.cut(employment_county['year'], range(2014, 2021, 3))
# temp = employment_county[['period', 'industrial_firms_diff_ct_demeand']].groupby(['period'],as_index = False).mean()
# temp.columns = ['period', 'industrial_firms_diff_ct_demeand_prd_mean']
# employment_county = pd.merge(employment_county, temp, on='period', how='left', validate='m:1')
# employment_county['industrial_firms_diff_ct_demeand_prd_demeand'] = employment_county['industrial_firms_diff_ct_demeand'] - employment_county['industrial_firms_diff_ct_demeand_prd_mean']

#%%
CHARLS_city = employment_individual[['province', 'city']].drop_duplicates()
CHARLS_city['city_abbr'] = CHARLS_city['city'].str[0:2]
CHARLS_city['adcode'] = None

base_url = 'https://restapi.amap.com/v3/config/district?'  # Gaode's administrative district service url
key = 'bd10f4df0a6a3e795c5886446f7af65c'  # my token
url = base_url + 'keywords=' + '中国' + '&subdistrict=3' + '&key=' + key
response = requests.get(url)
temp = json.loads(response.content)
for i in range(len(CHARLS_city)):
    for p in range(len(temp['districts'][0]['districts'])):
        if CHARLS_city.iloc[i,0][0:2] in temp['districts'][0]['districts'][p]['name']:
            for c in range(len(temp['districts'][0]['districts'][p]['districts'])):
                if CHARLS_city.iloc[i,2] in temp['districts'][0]['districts'][p]['districts'][c]['name']:
                    CHARLS_city.iloc[i,3] = temp['districts'][0]['districts'][p]['districts'][c]['adcode']
CHARLS_city.loc[CHARLS_city['city'] == '哈尔滨', 'adcode'] = 230100
CHARLS_city.loc[CHARLS_city['city'] == '哈尔滨市', 'adcode'] = 230100

CHARLS_city['city_code'] = CHARLS_city['adcode'].str[0:4]



employment_individual = employment_individual.merge(CHARLS_city[['province','city_code', 'city']], on = ['province','city'], how= "left", validate="m:1")

employment_gen1 = employment_individual[['householdID', 'year', 'ID', 'communityID', 'age', 'liverural','gen2ID', 'workag',
       'worknonag', 'agworkhired', 'agworkself', 'work', 'male', 'city_code']].query("gen2ID == '' & year > 2014 & age < 71")
employment_gen1 = employment_gen1.sort_values(by = ['year'])
employment_gen1['workag_diff'] = employment_gen1.groupby(['ID'],as_index = False)['workag'].diff()
employment_gen1['worknonag_diff'] = employment_gen1.groupby(['ID'],as_index = False)['worknonag'].diff()
employment_gen1['agworkhired_diff'] = employment_gen1.groupby(['ID'],as_index = False)['agworkhired'].diff()
employment_gen1['agworkself_diff'] = employment_gen1.groupby(['ID'],as_index = False)['agworkself'].diff()
employment_gen1['work_diff'] = employment_gen1.groupby(['ID'],as_index = False)['work'].diff()
employment_gen1['liverual_diff'] = employment_gen1.groupby(['ID'],as_index = False)['liverural'].diff()

employment_gen2 = employment_individual[['householdID', 'year', 'ID', 'communityID', 'age', 'liverural', 'workag','gen2ID', 'male', 'city_code']].query("gen2ID != '' &  year > 2012 & age < 71")
employment_gen2 = employment_gen2.sort_values(by = ['year'])
employment_gen2['liverual_diff'] = employment_gen2.groupby(['gen2ID'],as_index = False)['liverural'].diff()
employment_gen2['workag_diff'] = employment_gen2.groupby(['gen2ID'],as_index = False)['workag'].diff(2)

#%%
ft.write_dataframe(employment_city, "dataprocessing/output/employment_city.feather")
ft.write_dataframe(employment_county, "dataprocessing/output/employment_county.feather")
ft.write_dataframe(employment_gen1, "dataprocessing/output/employment_gen1.feather")
ft.write_dataframe(employment_gen2, "dataprocessing/output/employment_gen2.feather")





