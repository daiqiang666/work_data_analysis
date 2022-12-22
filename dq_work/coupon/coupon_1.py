
#!/usr/bin/env python
# coding: utf-8
import numpy as np
from numpy import float64
from numpy import int64
import pandas as pd
from dq_work import rw_file
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
engine=create_engine('mysql+pymysql://root:dqdqdq@localhost:3306/db1?charset=utf8',encoding='utf-8')
dq_work_engine=create_engine('mysql+pymysql://root:dqdqdq@localhost:3306/dq_work?charset=utf8',encoding='utf-8')

class coupon:
    def __init__(self,in_company,in_date_Range,in_date_Range_all,in_path,out_path,public_path):
        self.in_company = in_company
        self.in_date_Range = in_date_Range
        self.in_date_Range_all=in_date_Range_all
        self.in_path=in_path
        self.out_path=out_path
        self.public_path=public_path
           
    
    #导入原始文件
    def load_data(self,*file_list):
        #配置参数

        company=self.in_company
        date_Range=self.in_date_Range
        date_Range_all=self.in_date_Range_all
        load_data_path=self.in_path
        put_file_path=self.out_path
        load_public_data_path=self.public_path
        
        in_file1=file_list[0]
        in_file2=file_list[1]
        in_file3=file_list[2]
        in_file4=file_list[3]
        read_file = rw_file.rw_csv()
        
        df_券=pd.read_excel(io=load_data_path+in_file1,header=0,sheet_name=0,skiprows=0,index_col=None)
        df_券.rename(columns={'券类型':'券类型编码'},inplace=True)
        df_券.rename(columns={'券名称':'券类型名称'},inplace=True)
        
        #df_券_sku=pd.read_csv(filepath_or_buffer=load_public_data_path+in_file2,header=0,skiprows=0,index_col=None,dtype={'券类型编码':str})
        
        df_券_sku=pd.read_csv(filepath_or_buffer=load_public_data_path+in_file2,header=0,skiprows=0,index_col=None)
        
        
        #df_券_sku['券类型编码1'] = round(df_券_sku['券类型编码'])
        df_券_sku=df_券_sku.replace([np.inf, -np.inf], 0)
        df_券_sku['券类型编码']=df_券_sku['券类型编码'].fillna(0)
        
        
        df_券_sku[['券类型编码']]=df_券_sku[['券类型编码']].astype(int64)
        #df_券_sku['券类型编码'] = round(df_券_sku['券类型编码'])
        
        
        
         #处理门店主档数据，去重
        df_门店_主档 = read_file.r_csv_utf8(load_public_data_path+in_file3,0,0)
        df_门店_主档=df_门店_主档.loc[:,['门店DC编码','业态','门店DC名称']]
        df_门店_主档.drop_duplicates(subset = ['门店DC编码'],inplace=True)
        df_门店_主档.rename(columns={'门店DC编码':'门店编码'},inplace=True)
        
        ###导入类别主档数据
        df_类别 = read_file.r_csv_utf8(self.public_path+in_file4,0,0)
        
        
        df_券[['门店编码']]=df_券[['门店编码']].astype(str)
        df_券['门店编码']=df_券['门店编码'].str.strip()
        df_券[['券类型编码']]=df_券[['券类型编码']].astype(str)
        df_券['券类型编码']=df_券['券类型编码'].str.strip()
        
        df_券_sku[['门店编码']]=df_券_sku[['门店编码']].astype(str)
        df_券_sku['门店编码']=df_券_sku['门店编码'].str.strip()
        df_券_sku[['券类型编码']]=df_券_sku[['券类型编码']].astype(str)
        df_券_sku['券类型编码']=df_券_sku['券类型编码'].str.strip()
        
        df_门店_主档[['门店编码']]=df_门店_主档[['门店编码']].astype(str)
        df_门店_主档['门店编码']=df_门店_主档['门店编码'].str.strip()
        
        
        #
        #df_原始销售.rename(columns={'供应商业务码':'供应商编码'},inplace=True)

        print(df_券.loc[:,['毛利净额_用券']].sum()) 
        print(df_券.loc[:,['毛利净额_发券']].sum()) 
        print(df_券_sku.loc[:,['万家承担金额（去税）']].sum()) 
        
        print('导入原始数据 over')
        return df_券,df_券_sku,df_门店_主档,df_类别
    
    
        
    
    #  
    def coupons_join(self,*df_list):
        #导入原始DF
        df_券=df_list[0]
        df_券_sku=df_list[1]

        #券类型数据处理
        g_券 =df_券.groupby(['门店编码','券类型编码'])
        df_券_gp=pd.DataFrame(g_券[['销售净额_发券','毛利净额_发券','销售净额_用券','毛利净额_用券','分摊中门补差净额','分摊的促销毛利补差']].sum())
        #取消索引
        df_券_gp.reset_index(inplace=True)



        #sku券数据的汇总处理
        g_券_sku =df_券_sku.groupby(['门店编码','券类型编码'])
        df_券_sku_gp=pd.DataFrame(g_券_sku[['万家承担金额（去税）']].sum())
        #取消索引
        df_券_sku_gp.reset_index(inplace=True)
  

        #合并

        df_合并1=pd.merge(df_券_gp,df_券_sku_gp,on=['门店编码','券类型编码'],how='outer')

        
        

        print('券数据合并完成')

        return df_合并1
    

    #导出文件
    def put_outfile(self,out_df1,out_df2,out_df3): 
        
        
        #配置导出结果的目录与文件名
        out_file_dir=self.out_path

        out_file='券毛利-'+ self.in_company+ '-'+self.in_date_Range_all+'.xlsx'
        
        #导出xlsx
        writer=pd.ExcelWriter(out_file_dir+out_file)
        out_df1.to_excel(writer,header=True,sheet_name='券毛利',index=False)
        out_df2.to_excel(writer,header=True,sheet_name='券主档',index=False)
        out_df3.to_excel(writer,header=True,sheet_name='门店主档',index=False)
        writer.save()
        
        writer.close()
        
        print('券数据的excel文件导出 over')
        return
    
     #形成券类型名称主档
    def create_coupon_master_data(self,*df_list):
         
        df_券=df_list[0]
        df_券_sku=df_list[1]
        
        df_1=df_券.loc[:,[ '券类型编码', '券类型名称']]
        df_2=df_券_sku.loc[:,[ '券类型编码', '券类型名称']]

        df_coupon_master = df_1.append([df_2])
        df_coupon_master.drop_duplicates(subset = ['券类型编码'],keep='first',inplace=True)
        return df_coupon_master
        

        



    

