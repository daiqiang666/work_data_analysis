
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

class search_abnormal:
    def __init__(self,in_company,in_date_Range,in_date_Range_all,in_path,out_path,public_path):
        self.in_company = in_company
        self.in_date_Range = in_date_Range
        self.in_date_Range_all=in_date_Range_all
        self.in_path=in_path
        self.out_path=out_path
        self.public_path=public_path
           
    
    #导入原始csv文件
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
        
        read_file = rw_file.rw_csv()
        df_原始销售 = read_file.r_csv_utf8(load_data_path+in_file1,0,0)
        df_原始销售.rename(columns={'毛利额':'销售毛利额'},inplace=True)
        df_原始销售.rename(columns={'供应商业务码':'供应商编码'},inplace=True)

        df_原始用券 = read_file.r_csv_utf8(load_data_path+in_file2,0,0)
        
        #处理门店主档数据，去重
        df_门店_主档 = read_file.r_csv_utf8(load_public_data_path+in_file3,0,0)
        df_门店_主档=df_门店_主档.loc[:,['门店DC编码','业态','门店DC名称']]
        df_门店_主档.drop_duplicates(subset = ['门店DC编码'],inplace=True)
        
        ##强制转换原始df的门店编码类型为str      
        df_原始销售[['门店DC编码']]=df_原始销售[['门店DC编码']].astype(str)
        df_原始销售['门店DC编码']=df_原始销售['门店DC编码'].str.strip()
        
        df_原始用券[['门店编码']]=df_原始用券[['门店编码']].astype(str)
        df_原始用券['门店编码']=df_原始用券['门店编码'].str.strip()
        
        df_门店_主档[['门店DC编码']]=df_门店_主档[['门店DC编码']].astype(str)
        df_门店_主档['门店DC编码']=df_门店_主档['门店DC编码'].str.strip()
        
               
        print('导入原始数据 over')
        return df_原始销售,df_原始用券,df_门店_主档
    
    #加工数据，形成df ，作为第一个执行的函数，显式的给到原始csv文件入参   
    def working_data(self,*file_list):
        #导入原始DF
        in_file1=file_list[0]
        in_file2=file_list[1]
        in_file3=file_list[2]
        
        df_原始销售,df_原始用券,df_门店_主档=self.load_data(in_file1,in_file2,in_file3)
        
        #到单店单品级维度的销售类数据的处理
        g_1 =df_原始销售.groupby(['大类编码','门店DC编码','商品编码','商品名称'])
        df_1=pd.DataFrame(g_1[['销售净额','销售毛利额','折扣净额', '补差', '生鲜损耗']].sum())
        #取消索引
        df_1.reset_index(inplace=True)
        df_1['销售毛利率']=df_1['销售毛利额'] / df_1['销售净额']

        ################处理券数据###############
        #到单店单品级维度的券数据的处理
        g_1_quan =df_原始用券.groupby(['大类编码','门店编码','商品编码','商品名称'])
        df_1_quan=pd.DataFrame(g_1_quan[['万家承担金额（去税）']].sum())
        #取消索引
        df_1_quan.reset_index(inplace=True)
        df_1_quan.rename(columns={'门店编码':'门店DC编码'},inplace=True)

        #销售数据与券数据的合并
        df_合并1=pd.merge(df_1,df_1_quan,on=['大类编码','门店DC编码','商品编码','商品名称'],how='outer')
        df_合并1['销售用券率']=df_合并1['万家承担金额（去税）'] / df_合并1['销售净额']
        
        #准备券后数据
        df_合并2=df_合并1.fillna(0)
        df_合并2['券后毛利额']=df_合并2['销售毛利额']-df_合并2['万家承担金额（去税）']
        df_合并2['券后毛利率']=df_合并2['券后毛利额']/df_合并2['销售净额']
        df_合并2=df_合并2.replace([np.inf, -np.inf], 0)

        print('数据加工完成')
        return df_合并2,df_原始销售,df_原始用券,df_门店_主档
    
    #发现异常数据，需要显式的给到2个df的入参
    def find_abnormal(self,in_df1,in_df2):
        try:
            from dq_work import work1
            import imp
            imp.reload(work1)#再次引入模块，默认只有一次，调试程序时有用
            df_合并2,df_门店_主档=in_df1,in_df2
            outliers_find =work1.outliers_find()
            
            
            
            #计算券后毛利额与券后毛利率的异常值
            g_2_quanhou=df_合并2.groupby(['大类编码'])
            df_anomalies_券后毛利额=pd.DataFrame(g_2_quanhou[['门店DC编码','商品编码','商品名称','券后毛利额']] 
                                           .apply(outliers_find.find_anomalies_gross_profit,in_name='券后毛利额'))
            df_anomalies_券后毛利额.reset_index(inplace=True)

            df_anomalies_券后毛利率=pd.DataFrame(g_2_quanhou[['门店DC编码','商品编码','商品名称','券后毛利率']] 
                                           .apply(outliers_find.find_anomalies_gross_profit,in_name='券后毛利率'))
            df_anomalies_券后毛利率.reset_index(inplace=True)
            

            df_anomalies_out3=pd.merge(df_anomalies_券后毛利额,df_anomalies_券后毛利率,on=['门店DC编码','商品编码'],how='left')
            #修改数据类型，便于后续的join连接
            df_anomalies_out3['门店DC编码']=df_anomalies_out3.loc[:,['门店DC编码']].astype(str)
            #连接门店主档
            df_anomalies_out3=pd.merge(df_anomalies_out3,df_门店_主档,on=['门店DC编码'],how='left')
            
            #返回，是最原始的异常数据df
            
            print('券后毛利异常数据计算 finish，返回df')
            return df_anomalies_out3
            

        except Exception as e:

            print('Error:', e)
            
            
            
    #进一步精加工输出的文件格式与内容
    def finish_machining_outfile(self,in_df1,in_df2,in_df3):
        df_anomalies_out3=in_df1
        df_原始销售=in_df2
        df_原始用券=in_df3
        
        #剔除生鲜模块与正数毛利的数据
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        dq_work_engine=create_engine('mysql+pymysql://root:dqdqdq@localhost:3306/dq_work?charset=utf8',encoding='utf-8')
        
        sql1='''SELECT DISTINCT 模块名称,大类编码,大类名称 FROM xs_pinlei
        WHERE 大类名称 NOT IN ('删除禁用') AND 模块编码 not IN (20,22);
        '''
        df_category=pd.read_sql(sql=sql1,con=dq_work_engine)

        bool_not_fresh= df_anomalies_out3['大类编码_x'].isin(df_category.大类编码) 
        bool_less_than_0=df_anomalies_out3['券后毛利额'] < 0

        df_anomalies_out4=df_anomalies_out3[bool_not_fresh & bool_less_than_0]

        ##梳理输出的dataframe
        df_anomalies_out4_select=df_anomalies_out4.loc[:,['业态','门店DC编码','门店DC名称','大类编码_x','商品编码','商品名称_x','券后毛利额']]
        df_anomalies_out4_select['券后毛利额'] = round(df_anomalies_out4_select['券后毛利额'],2)
        
        #分销售渠道
        #获取要输出的异常负毛利的数据
        df_anomalies_out5=df_anomalies_out4.loc[:,['门店DC编码','门店DC名称','商品编码','商品名称_x']] 


        #到单品级、渠道维度的销售类数据的汇聚sum
        g_sku_channel =df_原始销售.groupby(['门店DC编码','商品编码','渠道'])
        df_sku_channel=pd.DataFrame(g_sku_channel[['销售净额','销售毛利额','折扣净额', '补差', '销售数量','销售成本']].sum())
        df_sku_channel.reset_index(inplace=True)


        #到单品级、渠道维度的券数据的汇聚sum
        g_quan_channel =df_原始用券.groupby(['门店编码','商品编码','渠道'])
        df_quan_channel=pd.DataFrame(g_quan_channel[['万家承担金额（去税）']].sum())
        df_quan_channel.reset_index(inplace=True)
        df_quan_channel.rename(columns={'门店编码':'门店DC编码'},inplace=True)


        df_sku_channel['门店DC编码']=df_sku_channel.loc[:,['门店DC编码']].astype(str)
        df_quan_channel['门店DC编码']=df_quan_channel.loc[:,['门店DC编码']].astype(str)

        #销售与券连接合并
        df_xs_quan=pd.merge(df_sku_channel,df_quan_channel,on=['门店DC编码','商品编码','渠道'],how='outer')
        df_xs_quan=df_xs_quan.fillna(0)
        df_xs_quan['券后毛利额']=df_xs_quan['销售毛利额']-df_xs_quan['万家承担金额（去税）']
        df_xs_quan['不含税单价']=df_xs_quan['销售净额']/df_xs_quan['销售数量']
        df_xs_quan['不含税单位成本']=df_xs_quan['销售成本']/df_xs_quan['销售数量']
        df_xs_quan=df_xs_quan.replace([np.inf, -np.inf], 0)
        df_xs_quan.rename(columns={'万家承担金额（去税）':'万家承担券费用'},inplace=True)

        #取2位小数
        df_xs_quan['不含税单价'] = round(df_xs_quan['不含税单价'],2)
        df_xs_quan['不含税单位成本'] = round(df_xs_quan['不含税单位成本'],2)
        df_xs_quan['券后毛利额'] = round(df_xs_quan['券后毛利额'],2)
        df_xs_quan['万家承担券费用'] = round(df_xs_quan['万家承担券费用'],2)
        df_xs_quan['销售净额'] = round(df_xs_quan['销售净额'],2)
        df_xs_quan['销售成本'] = round(df_xs_quan['销售成本'],2)
        df_xs_quan['补差'] = round(df_xs_quan['补差'],2)
        df_xs_quan['销售毛利额'] = round(df_xs_quan['销售毛利额'],2)

        #连接
        df_anomalies_out6=pd.merge(df_anomalies_out5,df_xs_quan,on=['门店DC编码','商品编码'])

        df_anomalies_out6_select=df_anomalies_out6.loc[:,['门店DC编码','门店DC名称','商品编码','商品名称_x','渠道','销售净额','销售成本', \
                                   '补差','销售毛利额','万家承担券费用','券后毛利额','销售数量','不含税单价','不含税单位成本']]

        print('精加工 dataframe over')
        return df_anomalies_out3,df_anomalies_out4_select,df_anomalies_out6_select

    #导出文件
    def put_outfile(self,in_df1,in_df2,in_df3): 
        df_anomalies_out3=in_df1
        df_anomalies_out4_select=in_df2
        df_anomalies_out6_select=in_df3
        
        #配置导出结果的目录与文件名
        out_file_dir=self.out_path
        out_file3='outliers_out-券后毛利-'+self.in_company+ '-'+self.in_date_Range_all+'.csv'
        out_file4='券后较大负毛利商品清单-'+ self.in_company+ '-'+self.in_date_Range_all+'.xlsx'
        
        #导出直接给到业务部门的xlsx
        writer=pd.ExcelWriter(out_file_dir+out_file4)
        df_anomalies_out4_select.to_excel(writer,header=True,sheet_name='汇总',index=False)
        df_anomalies_out6_select.to_excel(writer,header=True,sheet_name='商品明细',index=False)
        writer.save()
        
        writer.close()
        
        read_file = rw_file.rw_csv()
        read_file.w_csv_ansi(df_anomalies_out3,out_file_dir+out_file3)
        print('文件导出 over')
        return
    
    #####以下，分摊OI到单店、单品######
    
    #导入OI原始csv文件
    def load_data_oi(self):
        #配置参数

        company=self.in_company
        date_Range=self.in_date_Range
        date_Range_all=self.in_date_Range_all
        load_data_path=self.in_path
        put_file_path=self.out_path
        load_public_data_path=self.public_path
       
        #public_data
        in_file2='采购收入_'+company+ '_'+date_Range_all+'.csv'
        in_file3='B-分析-品类基础资料.csv'
        
        read_file = rw_file.rw_csv()
        #导入采购收入xls
        df_采购收入原始数据 = read_file.r_csv_ansi(load_public_data_path + in_file2,0,0)
        # 删选省区业绩的采购收入
        bool2=~df_采购收入原始数据['扣项类型'].str.contains('装卸|搬运')
        df_采购收入原始数据=df_采购收入原始数据[bool2]
        df_采购收入原始数据.rename(columns={'发生机构':'门店DC编码'},inplace=True)
        
        df_采购收入原始数据[['门店DC编码']]=df_采购收入原始数据[['门店DC编码']].astype(str)
        df_采购收入原始数据['门店DC编码']=df_采购收入原始数据['门店DC编码'].str.strip()
        
        ###导入类别主档数据
        df_类别 = read_file.r_csv_utf8(self.public_path+in_file3,0,0)
        
        
        print('原始数据load finish')
        return df_采购收入原始数据,df_类别
    
    def shop_sku_oi_share(self,df_list0,df_list1,df_list2,*model_list):
        df_单品销售原始数据=df_list0
        df_采收原始数据=df_list1
        df_类别=df_list2
        
        from dq_work import work1
        import imp
        imp.reload(work1)#再次引入模块，默认只有一次，调试程序时有用
        analysis_1=work1.analysis_index()

        #计算单品的销售比例
        g_供应商_大类 =df_单品销售原始数据.groupby(['供应商编码','大类编码','门店DC编码'])
        df_OI_fentan=pd.DataFrame(g_供应商_大类[['供应商编码','大类编码','门店DC编码','渠道','商品编码','销售净额']] \
                                       .apply(analysis_1.fentan_oi_sku))



        g_供应商_大类_OI=df_采收原始数据.groupby(['供应商编码','大类编码','门店DC编码'])
        df_OI=pd.DataFrame(g_供应商_大类_OI[['金额']].sum())
        #取消索引
        df_OI.reset_index(inplace=True)

        df_out1=pd.merge(df_OI_fentan,df_OI,on=['供应商编码','大类编码','门店DC编码'])

        #计算分摊金额
        df_out1['OI分摊金额']=df_out1['OI分摊比例']*df_out1['金额']


        
        #属于分析模块的，但是，上述到供应商+大类+门店没有匹配上的，进一步再分摊
        ###筛选属于分析模块的大类数据

        df_类别=df_类别.fillna(value='aa')
        bool_11= df_类别['模块编码'].isin(model_list) 
        bool_12=~df_类别['大类名称'].str.contains('删除禁用')
        df_选择类别=df_类别[bool_11 & bool_12]

        ###没有匹配上的、但是属于选择类别的采购收入
        df_not_join =pd.merge(df_OI_fentan,df_OI,on=['供应商编码','大类编码','门店DC编码'],how='right')
        bool_no_join = df_not_join ['商品编码'].isna()
        bool_in_dl = df_not_join['大类编码'].isin(df_选择类别.大类编码)
        df_not_join_oi=df_not_join.loc[bool_no_join & bool_in_dl]
        
        #将没有匹配上的所属类别的采购收入，再按照大类匹配到单品上
        #根据大类，计算单品的销售比例
        g__大类 =df_单品销售原始数据.groupby([ '大类编码'])
        df_OI_fentan_no_join=pd.DataFrame(g__大类[[ '大类编码','商品编码','销售净额']] \
                                       .apply(analysis_1.fentan_oi_sku))

        g__大类_OI=df_not_join_oi.groupby(['大类编码'])
        df_OI_no_join=pd.DataFrame(g__大类_OI[['金额']].sum())
        df_OI_no_join.reset_index(inplace=True)

        df_out1_1=pd.merge(df_OI_fentan_no_join,df_OI_no_join,on=['大类编码'],how='right')
        #计算分摊金额
        df_out1_1['OI分摊金额']=df_out1_1['OI分摊比例']*df_out1_1['金额']

        bool3_11=df_out1_1['商品编码'].isna()
        df_out1_1.loc[bool3_11,'OI分摊金额']=df_out1_1['金额']
        df_out1_1.loc[bool3_11,'商品编码']=1
        

        
        
        
        #输出原始的所选大类的OI总额，用于比对正确性
        
        bool_check = df_采收原始数据['大类编码'].isin(df_选择类别.大类编码)
        df_采收原始数据_选择大类=df_采收原始数据.loc[bool_check]
        print(df_采收原始数据_选择大类.loc[:,['金额']].sum())   
               
        print('OI 分摊计算完成')
        return df_out1,df_out1_1
        
        
        
        
        
        
        

        



    

