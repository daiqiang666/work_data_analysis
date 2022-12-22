#!/usr/bin/env python
# coding: utf-8

# In[1]:




import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
import sklearn.cluster as skc
from sklearn import metrics
from numpy import float64
import scipy.signal as signal
import numpy as np
#engine=create_engine('mysql+pymysql://root:dqdqdq@localhost:3306/db1?charset=utf8mb4')
engine=create_engine('mysql+pymysql://root:dqdqdq@localhost:3306/db1?charset=utf8',encoding='utf-8')
dq_work_engine=create_engine('mysql+pymysql://root:dqdqdq@localhost:3306/dq_work?charset=utf8',encoding='utf-8')

class analysis_index:
    '''
    def __init__(self,pct_num,max_min1,max_min2,max_min3):
        self.pct_num = pct_num
        self.max_min1 = max_min1
        self.max_min2 = max_min2
        self.max_min3= max_min3
    '''   
    
    def quantile_first(self,x,in_name):
        #
        in_df=pd.DataFrame(x)
        quan_1=in_df[in_name].quantile(0.8)
        
        bool_1=in_df[in_name] >= quan_1
        
        return_df=in_df.loc[bool_1]
        return return_df
    
    def fentan_oi_qudao(self,x):
        in_df=pd.DataFrame(x)
        
        #计算供应商+大类的销售额，作为分摊分母
        fenmu = in_df.loc[:,['月-销售净额']].sum()['月-销售净额']
        if fenmu >=0.01:
            in_df['OI分摊比例']= in_df['月-销售净额'] / fenmu                                  
        else:
            in_df['OI分摊比例']=0
        return_df=in_df
        return return_df
    
    def fentan_oi_sku(self,x):
        in_df=pd.DataFrame(x)
        
        #计算供应商+大类的销售额，作为分摊分母
        fenmu = in_df.loc[:,['销售净额']].sum()['销售净额']
        if fenmu >=0.01:
            in_df['OI分摊比例']= in_df['销售净额'] / fenmu                                  
        else:
            in_df['OI分摊比例']=0
        return_df=in_df
        return return_df
    def fentan_quan_gys(self,x):
        in_df=pd.DataFrame(x)
        
        #计算sku单品的销售额，作为分摊分母
        fenmu = in_df.loc[:,['销售净额']].sum()['销售净额']
        if fenmu >=0.01:
            in_df['quan分摊比例']= in_df['销售净额'] / fenmu                                  
        else:
            in_df['quan分摊比例']=0
        return_df=in_df
        return return_df
    
class outliers_find:
    
                
    def find_anomalies_gross_profit(self,x,in_name):
        in_df=pd.DataFrame(x)

        
        data_std=in_df[in_name].std()
        data_mean=in_df[in_name].mean()
        anomalies_cut_off=data_std *6
        
        lower_limit=data_mean - anomalies_cut_off
        upper_limit=data_mean + anomalies_cut_off
        #得到离群值
       
        bool_1=in_df[in_name] > upper_limit 
        bool_2=in_df[in_name] < lower_limit
        bool_3 = bool_1 | bool_2
        
        ret_df=in_df[bool_3]
        
        return ret_df  
  


    def find_not_anomalies_gross_profit(self,x,in_name):
        in_df=pd.DataFrame(x)

        
        data_std=in_df[in_name].std()
        data_mean=in_df[in_name].mean()
        anomalies_cut_off=data_std *72
        
        lower_limit=data_mean - anomalies_cut_off
        upper_limit=data_mean + anomalies_cut_off
        #得到离群值
       
        bool_1=in_df[in_name] < upper_limit 
        bool_2=in_df[in_name] > lower_limit
        bool_3 = bool_1 & bool_2
        
        ret_df=in_df[bool_3]
        return ret_df

    def find_anomalies_number(self,x,in_name):
        in_df=pd.DataFrame(x)

        
        data_std=in_df[in_name].std()
        data_mean=in_df[in_name].mean()
        anomalies_cut_off=data_std *6
        
        lower_limit=data_mean - anomalies_cut_off
        upper_limit=data_mean + anomalies_cut_off
        #得到离群值
       
        bool_1=in_df[in_name] > upper_limit 
        bool_2=in_df[in_name] < lower_limit
        bool_3 = bool_1 | bool_2
        
        ret_df=in_df[bool_3]
        
        return ret_df  
