#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 24 10:46:50 2020

@author: evantesei
"""

import pandas as pd
import matplotlib.pyplot as plt
import connect_db as connect_db


# goal is to compare three sets of SoC data:
# 1. SoC query using TAS used in Joe's forecast -- merchant level, industry average from 2019 data
# 2. SoC query using Enhance Analytics -- merchant level, industry average from 2020 data, eliminating outlier merchants
# 3. #1. but only limited to merchants with valid Enhance Analytics data, then compare


# establish connection
con = connect_db.connect_db()

soc_tos = """
select 
merchant_ari,
industry,
sum(tos) as sales,
sum(loan_vol) as vol_tos,
sum(loan_count) as count,
vol_tos/count as aov,
vol_tos/sales as soc_tos
FROM
(
select
lv.merchant_ari,
industry,
max(case when signing_account_tos is not null and signing_account_tos <> 0 then signing_account_tos else 1 end) as tos,
extract(year from lv.created) as year,
sum(auth_money_amount) as loan_vol,
count(lv.ari) as loan_count
FROM users_charge lv
left join merchants_merchant mm on lv.merchant_ari = mm.ari
left join merchants_merchantconf mc on mm.conf_id = mc.id
left join fpna.salesforce_deduped_v2 so on lv.merchant_ari = so.merchant_ari
where signing_account_tos is not null
and state in ('authorized','captured','merchant_captured')
and extract(year from initial_launch_date) < 2019
and name not like 'Peloton%%'
group by 1,2,4
order by year desc
)
where year = 2019
group by 1,2
order by 2 desc, 1;
"""

ea_query = """
with cart as
(
select
merchant_ari,
sum(order_total) as order_total
from dbt.ea_confirmed_orders soc
where
extract('year' from order_ts) = 2020
group by 1
)
,
vol as
(
select
name,
uc.merchant_ari,
industry,
sum(auth_money_amount) as vol
from
users_charge uc
left join merchants_merchant mm on uc.merchant_ari = mm.ari
left join merchants_merchantconf mc on mm.conf_id = mc.id
where state in ('authorized','captured','merchant_captured')
and extract(year from uc.created) = 2020
group by 1,2,3
)

select
vl.merchant_ari,
vl.name,
vl.industry,
vl.vol,
order_total
from
vol vl
inner join
cart ct
on vl.merchant_ari = ct.merchant_ari
left join merchants_merchant mm on vl.merchant_ari = mm.ari
left join merchants_merchantconf mc on mm.conf_id = mc.id

"""

test_query = """
select 
industry,
sum(tos) as sales,
sum(loan_vol) as vol,
sum(loan_count) as count,
vol/count as aov,
vol/sales as share,
count(distinct name) as merch_count
FROM
(
select
name,
industry,
max(case when signing_account_tos is not null and signing_account_tos <> 0 then signing_account_tos else 1 end) as tos,
extract(year from lv.created) as year,
sum(auth_money_amount) as loan_vol,
count(distinct lv.ari) as loan_count
FROM users_charge lv
left join merchants_merchant mm on lv.merchant_ari = mm.ari
left join merchants_merchantconf mc on mm.conf_id = mc.id
left join fpna.salesforce_deduped_v2 so on lv.merchant_ari = so.merchant_ari
where signing_account_tos is not null
and state in ('authorized','captured','merchant_captured')
and extract(year from initial_launch_date) < 2019
and name not like 'Peloton%%'
group by 1,2,4
order by year desc
)
where year = 2019
group by 1
order by 2 desc, 1
"""


# read in data
tos = pd.read_sql(soc_tos,con)
ea = pd.read_sql(ea_query,con)
old = pd.read_sql(test_query,con)
valid = pd.read_excel('ea_validity.xlsx')
valid = valid.loc[valid['Valid'] == 1]['Merchant Ari'].unique()
# sanitized merchant level
tos_san = tos.loc[tos['soc_tos'] < 0.5]
ea['soc_ea'] = ea['vol'] / ea['order_total'] 
ea = ea.loc[ea['merchant_ari'].isin(valid)]
ea = ea.loc[ea['soc_ea'] < 0.5]
# sanitized industry level
tos_san_ind = tos_san.groupby('industry').sum().reset_index()
tos_san_merch = tos_san.groupby('industry').merchant_ari.count().reset_index().rename(columns = {'merchant_ari':'tos_san_merch'})
tos_san_ind['tos_soc_san'] = tos_san_ind['vol_tos'] / tos_san_ind['sales']
tos_san_ind = tos_san_ind.merge(tos_san_merch[['industry','tos_san_merch']], on = 'industry' ,how = 'inner')

ea_ind = ea.groupby('industry').sum().reset_index()
ea_ind['soc_ea'] = ea_ind['vol'] / ea_ind['order_total']
ea_merch = ea.groupby('industry').merchant_ari.count().reset_index().rename(columns={'merchant_ari':'ea_merch'})
ea_ind = ea_ind.merge(ea_merch[['industry','ea_merch']],on = 'industry', how = 'inner')
merged = ea_ind[['industry','soc_ea','ea_merch']].merge(tos_san_ind[['industry','tos_soc_san','tos_san_merch']],on = 'industry', how = 'inner')



# compare to usual method and unsanitized tos
tos_unsan = tos.groupby('industry').sum().reset_index()
tos_unsan['soc_tos_unsan'] = tos_unsan['vol_tos'] / tos_unsan['sales']
tos_unsan_merch = tos.groupby('industry').merchant_ari.count().reset_index().rename(columns = {'merchant_ari':'tos_unsan_merch'})
tos_unsan = tos_unsan.merge(tos_unsan_merch[['industry','tos_unsan_merch']],on = 'industry', how = 'inner')


total = merged.merge(old, on = 'industry',how = 'inner')
total = total.merge(tos_unsan[['industry','soc_tos_unsan','tos_unsan_merch']], on = 'industry', how = 'inner')
total.to_csv('soc_v5.csv')

# unsanitized




