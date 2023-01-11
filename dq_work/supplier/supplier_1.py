
#!/usr/bin/env python
# coding: utf-8
import numpy as np
from numpy import float64
import pandas as pd
from dq_work import rw_file
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
engine=create_engine('mysql+pymysql://root:dqdqdq@localhost:3306/db1?charset=utf8',encoding='utf-8')
dq_work_engine=create_engine('mysql+pymysql://root:dqdqdq@localhost:3306/dq_work?charset=utf8',encoding='utf-8')

class supplier_analysis:
    def __init__(self,in_company,in_date_Range,in_date_Range_all,in_path,out_path,public_path):
        self.in_company = in_company
        self.in_date_Range = in_date_Range
        self.in_date_Range_all=in_date_Range_all
        self.in_path=in_path
        self.out_path=out_path
        self.public_path=public_path
           
    #根据券的原始数据，直接计算供应商维度的券分摊金额
    def Coupons_share(self):
        #配置参数

        company=self.in_company
        date_Range=self.in_date_Range
        date_Range_all=self.in_date_Range_all
        load_data_path=self.in_path
        put_file_path=self.out_path
        load_public_data_path=self.public_path
        
        rw_file1 = rw_file.rw_csv()
        
        #需要准备的导入文件清单
        ######################################
        file_dir_0=load_public_data_path

        file_1='01-用劵分摊-商品层级-原版-'+company+ '-all-'+date_Range_all+'.csv'
        df_券金额原始数据 =  rw_file1.r_csv_utf8(file_dir_0+file_1,0,0)
        
        #导出的文件
        file_out='供应商券分摊_out'+company+ '-all-'+date_Range+'.csv'
        
        #取得sku的券金额
        g_大类_供应商=df_券金额原始数据.groupby(['大类编码','供应商业务码'])
        df_大类_供应商_quan=pd.DataFrame(g_大类_供应商[['万家承担金额（去税）']].sum())
        #取消索引
        df_大类_供应商_quan.reset_index(inplace=True)
        df_大类_供应商_quan.rename(columns={'万家承担金额（去税）':'券分摊金额'},inplace=True)

        #导出供应商的分摊文件
        rw_file1.w_csv(df_大类_供应商_quan,load_data_path+file_out)

        print('供应商的券分摊csv文件导出完成')
        return
    
    ##根据券的原始数据，直接计算供应商维度的券分摊金额，券原始文件作为函数参数输入
    def Coupons_share_2(self,in_file1):
        #配置参数

        company=self.in_company
        date_Range=self.in_date_Range
        date_Range_all=self.in_date_Range_all
        load_data_path=self.in_path
        put_file_path=self.out_path
        load_public_data_path=self.public_path
        
        rw_file1 = rw_file.rw_csv()
        
        #需要准备的导入文件清单
        ######################################
        file_dir_0=load_public_data_path

        file_1=in_file1
        df_券金额原始数据 =  rw_file1.r_csv_utf8(file_dir_0+file_1,0,0)
        
        #导出的文件
        file_out='供应商券分摊_out'+company+ '-Partial-'+date_Range+'.csv'
        
        #取得sku的券金额
        g_大类_供应商=df_券金额原始数据.groupby(['大类编码','供应商业务码'])
        df_大类_供应商_quan=pd.DataFrame(g_大类_供应商[['万家承担金额（去税）']].sum())
        #取消索引
        df_大类_供应商_quan.reset_index(inplace=True)
        df_大类_供应商_quan.rename(columns={'万家承担金额（去税）':'券分摊金额'},inplace=True)

        #导出供应商的分摊文件
        rw_file1.w_csv(df_大类_供应商_quan,load_data_path+file_out)

        print('供应商的券分摊csv文件导出完成')
        return
    
    
    
    
    
    
    
    
    #根据销售占比，计算分摊供应商维度的券分摊金额
    def Coupons_sales_volume_share(self):
        from dq_work import work1
        
        #配置参数

        company=self.in_company
        date_Range=self.in_date_Range
        date_Range_all=self.in_date_Range_all
        load_data_path=self.in_path
        put_file_path=self.out_path
        load_public_data_path=self.public_path
        
        rw_file1 = rw_file.rw_csv()
        
        #导入原始文件数据，并进行合规调整
        ######################################
        ###导入原始券数据文件
        file_dir_0=load_public_data_path
        file_1='01-用劵分摊-商品层级-原版-'+company+ '-all-'+date_Range_all+'.csv'
        df_券金额原始数据 =  rw_file1.r_csv_utf8(file_dir_0+file_1,0,0)
        ####调整供应商业务码与编码问题
        df_券金额原始数据.rename(columns={'供应商业务码':'供应商编码'},inplace=True)
        
        
        
        ###导入供应商的销售额数据
        in_file_dir=load_data_path
        in_file='02_业务报表-销售报表-大类层级-供应商-'+company+ '-'+date_Range+'.csv'
        df_销售原始数据 = rw_file1.r_csv_utf8(in_file_dir + in_file,0,0)
        ####调整供应商业务码与编码问题
        df_销售原始数据.drop('供应商编码',axis=1,inplace=True)
        df_销售原始数据.rename(columns={'供应商业务码':'供应商编码'},inplace=True)
        df_销售原始数据.rename(columns={'月-销售净额':'销售净额'},inplace=True)
        
        
        ##强制转换原始df的供应商编码类型为str      
        df_券金额原始数据[['供应商编码']]=df_券金额原始数据[['供应商编码']].astype(str) 
        df_券金额原始数据['供应商编码']=df_券金额原始数据['供应商编码'].str.strip()
        df_销售原始数据[['供应商编码']]=df_销售原始数据[['供应商编码']].astype(str) 
        df_销售原始数据['供应商编码']=df_销售原始数据['供应商编码'].str.strip()
        
            
        
        
        
        ##################################################    
        #根据供应商销售额，计算券分摊
        analysis_1=work1.analysis_index()
        
        ####计算到大类、供应商的销售比例
        g_dl_gys =df_销售原始数据.groupby(['大类编码','供应商编码'])
        df_dl_gys_xs=pd.DataFrame(g_dl_gys[['销售净额']].sum())
        ####取消索引
        df_dl_gys_xs.reset_index(inplace=True)

        bool2=df_dl_gys_xs['销售净额']>0
        df_dl_gys_xs=df_dl_gys_xs[bool2]


        ####按大类+供应商，计算销售净额占比
        g_dl =df_dl_gys_xs.groupby(['大类编码'])
        df_quan_fentan=pd.DataFrame(g_dl[['大类编码','供应商编码','销售净额']] \
                                       .apply(analysis_1.fentan_quan_gys))
        #####取消索引
        df_quan_fentan.reset_index(inplace=True)


        #####取得大类层级维度的券金额
        g_dl_quan_yuanshi=df_券金额原始数据.groupby(['大类编码'])
        df_dl_quan_yuanshi=pd.DataFrame(g_dl_quan_yuanshi[['万家承担金额（去税）']].sum())


        df_out1=pd.merge(df_dl_quan_yuanshi,df_quan_fentan,on=['大类编码'],how='left')

        #####计算分摊金额
        df_out1['券分摊金额']=df_out1['quan分摊比例']*df_out1['万家承担金额（去税）']
        

        ###处理券分摊比例为0、null的数据
        bool3=df_out1['quan分摊比例'] == 0
        bool4=df_out1['quan分摊比例'].isna()
        bool5= bool3 | bool4
        ###取有效数据
        df_out2=df_out1.loc[~bool5]

     
        ###取为空、为零的数据
        df_out1_null=df_out1.loc[bool5]

        ###取得需要补充进来的券原始数据
        df_券原始_null=pd.merge(df_券金额原始数据,df_out1_null,on=['大类编码'])
        ###加工补充进来的数据
        df_1=df_券原始_null.loc[:,['大类编码','供应商编码_x','万家承担金额（去税）_x']]
        g_tmp_1=df_1.groupby(['大类编码','供应商编码_x'])
        df_tmp1=pd.DataFrame(g_tmp_1[['万家承担金额（去税）_x']].sum())
        df_tmp1.reset_index(inplace=True)
        df_tmp1.rename(columns={'供应商编码_x':'供应商编码','万家承担金额（去税）_x':'券分摊金额'},inplace=True)
        #=======================
        ###合并
        df_out3=df_out2.append([df_tmp1])

        ###按照供应商编码进行汇聚

        g_gys=df_out3.groupby(['供应商编码','大类编码'])

        df_out=pd.DataFrame(g_gys[[ '券分摊金额']].sum())
        ###取消索引
        df_out.reset_index(inplace=True)

        
        
        
        
        
        #导出的文件
        file_out='供应商券分摊_out'+company+ '-all-'+date_Range+'.csv'

        #导出供应商的分摊文件
        rw_file1.w_csv(df_out,load_data_path+file_out)
        
        

        print('供应商的券分摊csv文件导出完成')
        return    
    
    
    
    
    
    
    
    def Net_gross_profit_out_df(self):
        
        #配置参数

        company=self.in_company
        date_Range=self.in_date_Range
        date_Range_all=self.in_date_Range_all
        load_data_path=self.in_path
        put_file_path=self.out_path
        load_public_data_path=self.public_path

        read_file = rw_file.rw_csv()
        #需要准备的导入文件清单
        ######################################
        in_file_dir=load_data_path
        #supplier_data
        in_file='02_业务报表-销售报表-大类层级-供应商-'+company+ '-'+date_Range+'.csv'
        
        #public_data
        in_file2='采购收入_'+company+ '_'+date_Range_all+'.csv'
        #supplier_data
        in_file3='供应商券分摊_out'+company+ '-all-'+date_Range+'.csv'
        
        #public_data
        in_file4='01-用劵分摊-商品层级-原版-'+company+ '-all-'+date_Range_all+'.csv'

        ######################################
      
        #导入供应商的净销售xls
        df_供应商原始数据 = read_file.r_csv_utf8(in_file_dir + in_file,0,0)
        
        ####调整供应商业务码与编码问题
        df_供应商原始数据.drop('供应商编码',axis=1,inplace=True)
        df_供应商原始数据.rename(columns={'供应商业务码':'供应商编码'},inplace=True)
        
        
        #导入采购收入xls
        df_采购收入原始数据 = read_file.r_csv_ansi(load_public_data_path + in_file2,0,0)
        # 删选省区业绩的采购收入
        bool2=~df_采购收入原始数据['扣项类型'].str.contains('装卸|搬运')
        df_采购收入原始数据=df_采购收入原始数据[bool2]
        
        #导入供应商维度的券数据xls
        df_券金额原始数据 = read_file.r_csv_utf8(in_file_dir + in_file3 ,0,0)
        ####调整供应商业务码与编码问题
        df_券金额原始数据.rename(columns={'供应商业务码':'供应商编码'},inplace=True)
        
        
        #导入供应商主档的原始数据
        df_券金额原始数据_zsj = read_file.r_csv_utf8(load_public_data_path + in_file4 ,0,0)
        ####调整供应商业务码与编码问题
        df_券金额原始数据_zsj.rename(columns={'供应商业务码':'供应商编码'},inplace=True)
        
        
        ##强制转换原始df的供应商编码类型为str
        
       
        df_供应商原始数据[['供应商编码']]=df_供应商原始数据[['供应商编码']].astype(str) 
        df_供应商原始数据['供应商编码']=df_供应商原始数据['供应商编码'].str.strip()
        ###
        df_采购收入原始数据[['供应商编码']]=df_采购收入原始数据[['供应商编码']].astype(str)  
        df_采购收入原始数据['供应商编码']=df_采购收入原始数据['供应商编码'].str.strip()
        
        df_券金额原始数据[['供应商编码']]=df_券金额原始数据[['供应商编码']].astype(str) 
        df_券金额原始数据['供应商编码']=df_券金额原始数据['供应商编码'].str.strip()
        
        df_券金额原始数据_zsj[['供应商编码']]=df_券金额原始数据_zsj[['供应商编码']].astype(str) 
        df_券金额原始数据_zsj['供应商编码']=df_券金额原始数据_zsj['供应商编码'].str.strip()
        
        
        
        
        print(df_供应商原始数据.loc[:,['月-销售净额','月-销售毛利']].sum())
        print(df_采购收入原始数据.loc[:,['金额']].sum())
        print(df_券金额原始数据_zsj.loc[:,['万家承担金额（去税）']].sum())
        
        print('load  data is over')

        
        #计算处理
        ####计算销售净额等
        g_销售_供应商 =df_供应商原始数据.groupby(['大类编码', '供应商编码'])
        df_供应商_销售=pd.DataFrame(g_销售_供应商[['月-销售净额','月-销售毛利']].sum())
        df_供应商_销售.reset_index(inplace=True)

        
        ####计算采购收入
        g_采收_供应商 =df_采购收入原始数据.groupby(['大类编码', '供应商编码'])
        df_供应商_采收=pd.DataFrame(g_采收_供应商[['金额']].sum())
        df_供应商_采收.reset_index(inplace=True)
        
        
        ###计算券数据
        g_券_供应商 =df_券金额原始数据.groupby(['大类编码','供应商编码'])
        df_供应商_用券金额=pd.DataFrame(g_券_供应商[['券分摊金额']].sum())
        df_供应商_用券金额.reset_index(inplace=True)

        
        #形成供应商主档
        
        df_gys1=df_供应商原始数据.loc[:,[ '供应商编码', '供应商名称']]
        df_gys2=df_采购收入原始数据.loc[:,[ '供应商编码', '供应商名称']]
        df_gys3=df_券金额原始数据_zsj.loc[:,[ '供应商编码', '供应商名称']]
        df_供应商主档 =df_gys1.append([df_gys2,df_gys3])
        
        #转换数据类型
        '''
        df_供应商主档[['供应商编码']]=df_供应商主档[['供应商编码']].astype(str) 
        df_供应商主档['供应商编码']=df_供应商主档['供应商编码'].str.strip()
        '''
        df_供应商主档.drop_duplicates(subset = ['供应商编码'],keep='first',inplace=True)

    
        
        #计算供应商纯毛利的数据
        df_供应商_1=pd.merge(df_供应商_销售,df_供应商_采收,on=['大类编码','供应商编码'],how='outer')
        df_供应商_2=pd.merge(df_供应商_1,df_供应商_用券金额,on=['大类编码','供应商编码'],how='outer')

        df_供应商_out_tmp=pd.merge(df_供应商_2,df_供应商主档,on=['供应商编码'],how='left')

        df_供应商_out_tmp.rename(columns={'券分摊金额':'万家承担金额（去税）'},inplace=True)
        df_供应商_out_tmp=df_供应商_out_tmp.fillna(0)

        df_供应商_out_tmp['券后总毛利']=df_供应商_out_tmp['月-销售毛利']-df_供应商_out_tmp['万家承担金额（去税）']+df_供应商_out_tmp['金额']

        df_供应商_out_tmp['月-销售净额'] = round(df_供应商_out_tmp['月-销售净额'],2)
        df_供应商_out_tmp['月-销售毛利'] = round(df_供应商_out_tmp['月-销售毛利'],2)
        df_供应商_out_tmp['金额'] = round(df_供应商_out_tmp['金额'],2)
        df_供应商_out_tmp['万家承担金额（去税）'] = round(df_供应商_out_tmp['万家承担金额（去税）'],2)
        df_供应商_out_tmp['券后总毛利'] = round(df_供应商_out_tmp['券后总毛利'],2)

        df_供应商_out_tmp.rename(columns={'金额':'采购收入分摊'},inplace=True)
        df_供应商_out_tmp.rename(columns={'万家承担金额（去税）':'用券金额分摊'},inplace=True)


        df_供应商_out=df_供应商_out_tmp.loc[:,['大类编码','供应商编码','供应商名称','月-销售净额','月-销售毛利','采购收入分摊','用券金额分摊', \
                                                    '券后总毛利']]

        print('data DF finish')
        return df_供应商_out
    
    def Net_gross_profit_out_df_2(self,*in_file_list):
        
        #配置参数

        company=self.in_company
        date_Range=self.in_date_Range
        date_Range_all=self.in_date_Range_all
        load_data_path=self.in_path
        put_file_path=self.out_path
        load_public_data_path=self.public_path

        read_file = rw_file.rw_csv()
        #需要准备的导入文件清单
        ######################################
        in_file_dir=load_data_path
        #supplier_data
        
        in_file=in_file_list[0]
        #public_data
        in_file2=in_file_list[1]
        #supplier_data
        in_file3=in_file_list[2]
        
        #public_data

        in_file4=in_file_list[3]
        
    

        ######################################
      
        #导入供应商的净销售xls
        df_供应商原始数据 = read_file.r_csv_utf8(in_file_dir + in_file,0,0)
        
        ####调整供应商业务码与编码问题
        df_供应商原始数据.drop('供应商编码',axis=1,inplace=True)
        df_供应商原始数据.rename(columns={'供应商业务码':'供应商编码'},inplace=True)

 
        #导入采购收入xls
        df_采购收入原始数据 = read_file.r_csv_ansi(load_public_data_path + in_file2,0,0)
        # 删选省区业绩的采购收入
        bool2=~df_采购收入原始数据['扣项类型'].str.contains('装卸|搬运')
        df_采购收入原始数据=df_采购收入原始数据[bool2]
        
        #导入供应商维度的券数据xls
        df_券金额原始数据 = read_file.r_csv_utf8(in_file_dir + in_file3 ,0,0)
        ####调整供应商业务码与编码问题
        df_券金额原始数据.rename(columns={'供应商业务码':'供应商编码'},inplace=True)
        
        
        #导入供应商主档的原始数据
        df_券金额原始数据_zsj = read_file.r_csv_utf8(load_public_data_path + in_file4 ,0,0)
        ####调整供应商业务码与编码问题
        df_券金额原始数据_zsj.rename(columns={'供应商业务码':'供应商编码'},inplace=True)
        
        
        ##强制转换原始df的供应商编码类型为str
        
       
        df_供应商原始数据[['供应商编码']]=df_供应商原始数据[['供应商编码']].astype(str) 
        df_供应商原始数据['供应商编码']=df_供应商原始数据['供应商编码'].str.strip()
        ###
        df_采购收入原始数据[['供应商编码']]=df_采购收入原始数据[['供应商编码']].astype(str)  
        df_采购收入原始数据['供应商编码']=df_采购收入原始数据['供应商编码'].str.strip()
        
        df_券金额原始数据[['供应商编码']]=df_券金额原始数据[['供应商编码']].astype(str) 
        df_券金额原始数据['供应商编码']=df_券金额原始数据['供应商编码'].str.strip()
        
        df_券金额原始数据_zsj[['供应商编码']]=df_券金额原始数据_zsj[['供应商编码']].astype(str) 
        df_券金额原始数据_zsj['供应商编码']=df_券金额原始数据_zsj['供应商编码'].str.strip()
        
        
        
        
        print(df_供应商原始数据.loc[:,['销售净额','销售毛利额']].sum())
        print(df_采购收入原始数据.loc[:,['金额']].sum())
        print(df_券金额原始数据_zsj.loc[:,['万家承担金额（去税）']].sum())
        
        print('load  data is over')

        
        #计算处理
        ####计算销售净额等
        g_销售_供应商 =df_供应商原始数据.groupby(['大类编码', '供应商编码'])
        df_供应商_销售=pd.DataFrame(g_销售_供应商[['销售净额','销售毛利额']].sum())
        df_供应商_销售.reset_index(inplace=True)

        
        ####计算采购收入
        g_采收_供应商 =df_采购收入原始数据.groupby(['大类编码', '供应商编码'])
        df_供应商_采收=pd.DataFrame(g_采收_供应商[['金额']].sum())
        df_供应商_采收.reset_index(inplace=True)
        
        
        ###计算券数据
        g_券_供应商 =df_券金额原始数据.groupby(['大类编码','供应商编码'])
        df_供应商_用券金额=pd.DataFrame(g_券_供应商[['券分摊金额']].sum())
        df_供应商_用券金额.reset_index(inplace=True)

        
        #形成供应商主档
        
        df_gys1=df_供应商原始数据.loc[:,[ '供应商编码', '供应商名称']]
        df_gys2=df_采购收入原始数据.loc[:,[ '供应商编码', '供应商名称']]
        df_gys3=df_券金额原始数据_zsj.loc[:,[ '供应商编码', '供应商名称']]
        df_供应商主档 =df_gys1.append([df_gys2,df_gys3])
        
        #转换数据类型
        '''
        df_供应商主档[['供应商编码']]=df_供应商主档[['供应商编码']].astype(str) 
        df_供应商主档['供应商编码']=df_供应商主档['供应商编码'].str.strip()
        '''
        df_供应商主档.drop_duplicates(subset = ['供应商编码'],keep='first',inplace=True)

    
        
        #计算供应商纯毛利的数据
        df_供应商_1=pd.merge(df_供应商_销售,df_供应商_采收,on=['大类编码','供应商编码'],how='outer')
        df_供应商_2=pd.merge(df_供应商_1,df_供应商_用券金额,on=['大类编码','供应商编码'],how='outer')

        df_供应商_out_tmp=pd.merge(df_供应商_2,df_供应商主档,on=['供应商编码'],how='left')

        df_供应商_out_tmp.rename(columns={'券分摊金额':'万家承担金额（去税）'},inplace=True)
        df_供应商_out_tmp=df_供应商_out_tmp.fillna(0)

        df_供应商_out_tmp['券后总毛利']=df_供应商_out_tmp['销售毛利额']-df_供应商_out_tmp['万家承担金额（去税）']+df_供应商_out_tmp['金额']

        df_供应商_out_tmp['销售净额'] = round(df_供应商_out_tmp['销售净额'],2)
        df_供应商_out_tmp['销售毛利额'] = round(df_供应商_out_tmp['销售毛利额'],2)
        df_供应商_out_tmp['金额'] = round(df_供应商_out_tmp['金额'],2)
        df_供应商_out_tmp['万家承担金额（去税）'] = round(df_供应商_out_tmp['万家承担金额（去税）'],2)
        df_供应商_out_tmp['券后总毛利'] = round(df_供应商_out_tmp['券后总毛利'],2)

        df_供应商_out_tmp.rename(columns={'金额':'采购收入分摊'},inplace=True)
        df_供应商_out_tmp.rename(columns={'万家承担金额（去税）':'用券金额分摊'},inplace=True)


        df_供应商_out=df_供应商_out_tmp.loc[:,['大类编码','供应商编码','供应商名称','销售净额','销售毛利额','采购收入分摊','用券金额分摊', \
                                                    '券后总毛利']]

        print('data DF finish')
        return df_供应商_out
        

    def Net_gross_profit_out_xlsx(self,w_df):
        #配置参数
        company=self.in_company
        date_Range=self.in_date_Range
        date_Range_all=self.in_date_Range_all
        load_data_path=self.in_path
        put_file_path=self.out_path
        
        
       
        #配置导出结果的目录与文件名
        out_file_dir=put_file_path
        out_file='10.供应商毛利分析_out_'+company+ '-'+date_Range_all+'.xlsx'
        
        
        writer=pd.ExcelWriter(out_file_dir+out_file)
        w_df.to_excel(writer,header=True,sheet_name='供应商毛利分析',index=False)
        writer.save()
        print('供应商纯毛利的xls导出 over')
        writer.close()
        return
    
    #将结果落库到mysql
    def input_mysql(self,in_df,in_table):
        try:
            from sqlalchemy.orm import sessionmaker
            
            
            #删除清理数据表
            Session = sessionmaker(bind=dq_work_engine)
            session = Session()
            session.begin()
            sql_proc1='drop table if exists '+ in_table;
                   
            session.execute(sql_proc1)
    
            session.commit()
            session.close()
            in_df = in_df.replace([np.inf, -np.inf], 0)
            in_df.to_sql(in_table,dq_work_engine,index = False,if_exists='append')
 
            print('input mysql')
        except Exception as e:
            session.rollback()
            session.close()
            print('Error:', e)

    
    
