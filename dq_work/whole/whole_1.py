
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

class whole(object):
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
        
        #public_data
        in_file4=file_list[3]
        in_file5=file_list[4]
                 
        read_file = rw_file.rw_csv()
        
        df_原始销售 = read_file.r_csv_utf8(load_data_path+in_file1,0,0)


        df_原始用券 = read_file.r_csv_utf8(load_public_data_path+in_file2,0,0)
        
        #处理门店主档数据，去重
        df_门店_主档 = read_file.r_csv_utf8(load_public_data_path+in_file3,0,0)
        df_门店_主档=df_门店_主档.loc[:,['门店DC编码','业态','门店DC名称']]
        df_门店_主档.drop_duplicates(subset = ['门店DC编码'],inplace=True)
        
        
        ###导入采购收入xls
        df_采购收入原始数据 = read_file.r_csv_ansi(load_public_data_path + in_file4,0,0)
        # 删选省区业绩的采购收入
        bool2=~df_采购收入原始数据['扣项类型'].str.contains('装卸|搬运')
        df_采购收入原始数据=df_采购收入原始数据[bool2]
        df_采购收入原始数据.rename(columns={'发生机构':'门店DC编码'},inplace=True)

        ###导入类别主档数据
        df_类别 = read_file.r_csv_utf8(self.public_path+in_file5,0,0)
               
                
        ##强制转换原始df的门店编码类型为str，并去除空格      
        df_原始销售[['门店DC编码']]=df_原始销售[['门店DC编码']].astype(str)
        df_原始销售['门店DC编码']=df_原始销售['门店DC编码'].str.strip()
        
        df_原始用券[['门店DC编码']]=df_原始用券[['门店DC编码']].astype(str)
        df_原始用券['门店DC编码']=df_原始用券['门店DC编码'].str.strip()
        
        df_门店_主档[['门店DC编码']]=df_门店_主档[['门店DC编码']].astype(str)
        df_门店_主档['门店DC编码']=df_门店_主档['门店DC编码'].str.strip()
        
        df_采购收入原始数据[['门店DC编码']]=df_采购收入原始数据[['门店DC编码']].astype(str)
        df_采购收入原始数据['门店DC编码']=df_采购收入原始数据['门店DC编码'].str.strip()
        

        
        print('导入原始数据 over')
        return df_原始销售,df_原始用券,df_门店_主档,df_采购收入原始数据,df_类别
    
    
   
    def sale_coupons_join(self,*df_list):
        #导入原始DF
        df_原始销售=df_list[0]
        df_原始用券=df_list[1]
        #原始券文件是到大类层级的，修改相关字段名
        df_原始用券.rename(columns={'渠道名称':'渠道'},inplace=True)
        df_原始用券.rename(columns={'万家承担金额（元）':'万家承担金额（去税）'},inplace=True)
        #如果券原始文件是到单品级的，则上述2个修改字段的命令要注销

        
        #到单店、大类、渠道层级维度的销售数据的处理
        g_1 =df_原始销售.groupby(['大类编码','门店DC编码','渠道'])
        df_1=pd.DataFrame(g_1[['销售净额','销售成本','销售数量','销售毛利额','折扣净额','补差','生鲜损耗','客流量']].sum())
        #取消索引
        df_1.reset_index(inplace=True)
        df_1['销售毛利率']=df_1['销售毛利额'] / df_1['销售净额']

        ################处理券数据###############
        #到单店单品级维度的券数据的处理
        g_1_quan =df_原始用券.groupby(['大类编码','门店DC编码','渠道'])
        df_1_quan=pd.DataFrame(g_1_quan[['万家承担金额（去税）']].sum())
        #取消索引
        df_1_quan.reset_index(inplace=True)
        #df_1_quan.rename(columns={'门店编码':'门店DC编码'},inplace=True)

        #销售数据与券数据的合并
        ###按照销售数据为标准，剔除不在销售数据模块范围内的券金额
        df_合并1=pd.merge(df_1,df_1_quan,on=['大类编码','门店DC编码','渠道'],how='outer')
        df_合并1['销售用券率']=df_合并1['万家承担金额（去税）'] / df_合并1['销售净额']
        
        #准备券后数据
        df_合并2=df_合并1.fillna(0)
        df_合并2['券后毛利额']=df_合并2['销售毛利额']-df_合并2['万家承担金额（去税）']
        df_合并2['券后毛利率']=df_合并2['券后毛利额']/df_合并2['销售净额']
        df_合并2=df_合并2.replace([np.inf, -np.inf], 0)

        print('销售数据与券数据合并完成')
        print(df_原始销售.loc[:,['销售净额']].sum()) 
        print(df_原始用券.loc[:,['万家承担金额（去税）']].sum()) 
        print(df_合并2.loc[:,['万家承担金额（去税）']].sum()) 
        
        return df_合并2
    
    
    #####分摊OI到单店、大类######
    def shop_category_oi_share(self,df_list0,df_list1,df_list2):
        df_销售原始数据=df_list0
        df_采收原始数据=df_list1
        df_类别=df_list2
        
        from dq_work import work1
        import imp
        imp.reload(work1)#再次引入模块，默认只有一次，调试程序时有用
        analysis_1=work1.analysis_index()

        #计算单品的销售比例
        g_供应商_大类 =df_销售原始数据.groupby(['供应商编码','大类编码','门店DC编码'])
        df_OI_fentan=pd.DataFrame(g_供应商_大类[['供应商编码','大类编码','门店DC编码','渠道','销售净额']] \
                                       .apply(analysis_1.fentan_oi_sku))



        g_供应商_大类_OI=df_采收原始数据.groupby(['供应商编码','大类编码','门店DC编码'])
        df_OI=pd.DataFrame(g_供应商_大类_OI[['金额']].sum())
        #取消索引
        df_OI.reset_index(inplace=True)

        df_out1=pd.merge(df_OI_fentan,df_OI,on=['供应商编码','大类编码','门店DC编码'])

        #计算分摊金额
        df_out1['OI分摊金额']=df_out1['OI分摊比例']*df_out1['金额']


        
        #上述到供应商+大类+门店没有匹配上的，进一步再分摊
        ###筛选有效大类数据

        df_类别=df_类别.fillna(value='aa')
       
        bool_12=~df_类别['大类名称'].str.contains('删除禁用')
        df_选择类别=df_类别[bool_12]

        ###没有匹配上的采购收入
        df_not_join =pd.merge(df_OI_fentan,df_OI,on=['供应商编码','大类编码','门店DC编码'],how='right')
        bool_no_join = df_not_join ['渠道'].isna()
        bool_in_dl = df_not_join['大类编码'].isin(df_选择类别.大类编码)
        df_not_join_oi=df_not_join.loc[bool_no_join & bool_in_dl]
        
        #将没有匹配上的所属类别的采购收入，再按照大类匹配到渠道上
        #根据大类，计算渠道的销售比例
        g__大类 =df_销售原始数据.groupby([ '大类编码'])
        df_OI_fentan_no_join=pd.DataFrame(g__大类[[ '大类编码','渠道','销售净额']] \
                                       .apply(analysis_1.fentan_oi_sku))

        g__大类_OI=df_not_join_oi.groupby(['大类编码'])
        df_OI_no_join=pd.DataFrame(g__大类_OI[['金额']].sum())
        df_OI_no_join.reset_index(inplace=True)

        df_out1_1=pd.merge(df_OI_fentan_no_join,df_OI_no_join,on=['大类编码'],how='right')
        #计算分摊金额
        df_out1_1['OI分摊金额']=df_out1_1['OI分摊比例']*df_out1_1['金额']

        bool3_11=df_out1_1['渠道'].isna()
        df_out1_1.loc[bool3_11,'OI分摊金额']=df_out1_1['金额']
        df_out1_1.loc[bool3_11,'渠道']='XXXX'
        
        df_out1_1['供应商编码']=999999
        df_out1_1['门店DC编码']=9999

        print(df_采收原始数据.loc[:,['金额']].sum())   
               
        print('完整的OI在大类加门店字段上分摊计算完成')
        return df_out1,df_out1_1
    
    
    def merge_out_df(self,*df_list):
        df_sale_coupons=df_list[0]
        df_oi1=df_list[1]
        df_oi2=df_list[2]

        df_oi=df_oi1.append(df_oi2)
        
        #去掉供应商编码的重复因素
        g_oi =df_oi.groupby(['大类编码','门店DC编码','渠道'])
        df_oi_out1=pd.DataFrame(g_oi[['销售净额','OI分摊比例','金额', 'OI分摊金额']].sum())
        #取消索引
        df_oi_out1.reset_index(inplace=True)
        
        df_oi_out1=df_oi_out1.loc[:,['大类编码','门店DC编码','渠道','OI分摊比例','OI分摊金额']]
        
        df_out=pd.merge(df_sale_coupons,df_oi_out1,on=['大类编码','门店DC编码','渠道'],how='outer')
        print('df合并处理finish')
        return df_out
        
        
             
    #进一步精加工输出的文件格式与内容
    def finish_machining_outfile(self,*df_list):
        
        df_out1=df_list[0]

        df_类别=df_list[1]
        df_门店_主档=df_list[2]
        
        df_大类主档=df_类别.loc[:,['模块编码','模块名称','大类编码','大类名称']]
        df_大类主档.drop_duplicates(subset = ['大类编码'],keep='first',inplace=True)
        

        df_out3=pd.merge(df_out1,df_大类主档,on=['大类编码'],how='left')
        df_out4=pd.merge(df_out3,df_门店_主档,on=['门店DC编码'],how='left')
        
        

      

        #取2位小数
        '''
        df_xs_quan['不含税单价'] = round(df_xs_quan['不含税单价'],2)
        df_xs_quan['不含税单位成本'] = round(df_xs_quan['不含税单位成本'],2)
        df_xs_quan['券后毛利额'] = round(df_xs_quan['券后毛利额'],2)
        df_xs_quan['万家承担券费用'] = round(df_xs_quan['万家承担券费用'],2)
        df_xs_quan['销售净额'] = round(df_xs_quan['销售净额'],2)
        df_xs_quan['销售成本'] = round(df_xs_quan['销售成本'],2)
        df_xs_quan['补差'] = round(df_xs_quan['补差'],2)
        df_xs_quan['销售毛利额'] = round(df_xs_quan['销售毛利额'],2)
        '''




        print('精加工 dataframe over')
        return df_out4

    #导出文件
    def put_outfile(self,out_df1): 
        
        
        #配置导出结果的目录与文件名
        out_file_dir=self.out_path

        out_file='省公司整体总毛利-'+ self.in_company+ '-'+self.in_date_Range_all+'.xlsx'
        
        #导出直接给到业务部门的xlsx
        writer=pd.ExcelWriter(out_file_dir+out_file)
        out_df1.to_excel(writer,header=True,sheet_name='总毛利',index=False)
        writer.save()
        
        writer.close()
        
        print('整体销售毛利数据的excel文件导出 over')
        return
    
    #####################################################################################
    ##库存周转的分析
    #####导入库存的原始数据
    def load_data_stock(self,*file_list):
        #配置参数

        company=self.in_company
        date_Range=self.in_date_Range
        date_Range_all=self.in_date_Range_all
        load_data_path=self.in_path
        put_file_path=self.out_path
        load_public_data_path=self.public_path
        
        in_file1=file_list[0]
        
         
        read_file = rw_file.rw_csv()
        
        df_原始库存 = read_file.r_csv_utf8(load_data_path+in_file1,0,0)

                
        ##强制转换原始df的门店编码类型为str，并去除空格      
        df_原始库存[['门店DC编码']]=df_原始库存[['门店DC编码']].astype(str)
        df_原始库存['门店DC编码']=df_原始库存['门店DC编码'].str.strip()
        

        print('导入原始库存数据 over')
        print(df_原始库存.loc[:,['库存金额']].sum()) 
        return df_原始库存
    
    #库存数据的计算
    def stock_compute(self,*df_list):
        df_原始库存=df_list[0]
        
        
        
        g_stock =df_原始库存.groupby(['大类编码','门店DC编码'])
        df_stock_out=pd.DataFrame(g_stock[['自营-销售成本','库存数量','库存金额']].sum())
        #取消索引
        df_stock_out.reset_index(inplace=True)
        
        
        print('库存计算finish')
        return df_stock_out
    
    #销售与库存的合并
    def merge_sale_stock(self,*df_list):
        df_sale=df_list[0]
        df_stock=df_list[1]
        
        #去掉销售df的渠道
        #g_sale =df_sale.groupby(['门店DC编码','门店DC名称','业态','模块编码','模块名称','大类编码','大类名称','商品编码','商品名称'])
        
        g_sale =df_sale.groupby(['门店DC编码','大类编码'])
        df_sale_out1=pd.DataFrame(g_sale[['销售净额','销售成本','销售数量','销售毛利额','折扣净额','补差','生鲜损耗', \
                                          '万家承担金额（去税）','券后毛利额','OI分摊金额']].sum())
        #取消索引
        df_sale_out1.reset_index(inplace=True)
        
        
        
        df_out=pd.merge(df_sale_out1,df_stock,on=['大类编码','门店DC编码'],how='outer')
        
        df_out['日库存金额']=df_out['库存金额']/31
        df_out['周转天数']=df_out['库存金额']/df_out['自营-销售成本']
        

        
        print('销售与库存合并处理finish')
        return df_out
    
    #导出销售与库存合并后的文件
    def put_outfile_stock(self,out_df1): 
        
        
        #配置导出结果的目录与文件名
        out_file_dir=self.out_path

        out_file='整体的销售毛利库存-'+ self.in_company+ '-'+self.in_date_Range_all+'.xlsx'
        
        #导出直接给到业务部门的xlsx
        writer=pd.ExcelWriter(out_file_dir+out_file)
        out_df1.to_excel(writer,header=True,sheet_name='销售毛利库存',index=False)
        writer.save()
        
        writer.close()
        
        print('整体的销售毛利库存数据的excel文件导出 over')
        return
    
    ######针对全年、大类维度的总体分析(2023/01/04)############
 
    #导入大类维度的原始的销售csv文件
    def load_data_category_sale(self,*file_list):
        #配置参数

        company=self.in_company
        date_Range=self.in_date_Range
        date_Range_all=self.in_date_Range_all
        load_data_path=self.in_path
        put_file_path=self.out_path
        load_public_data_path=self.public_path
        
        read_file = rw_file.rw_csv()
        
        in_file_sale=file_list[0]
        in_file_category=file_list[1]

        #导入销售数据
        df_原始销售 = read_file.r_csv_utf8(load_data_path+in_file_sale,0,0)

        #导入类别主档数据
        df_类别 = read_file.r_csv_utf8(self.public_path+in_file_category,0,0)


        print('导入原始销售数据、类别主档 over')
        return df_原始销售,df_类别
   
    #导入大类维度的原始的券csv文件
    def load_data_category_coupon(self,*file_list):
        #配置参数

        company=self.in_company
        date_Range=self.in_date_Range
        date_Range_all=self.in_date_Range_all
        load_data_path=self.in_path
        put_file_path=self.out_path
        load_public_data_path=self.public_path
        
        read_file = rw_file.rw_csv()
        
        in_file_coupon=file_list[0]
        

        #导入销售数据
        df_原始用券 = read_file.r_csv_utf8(load_public_data_path+in_file_coupon,0,0)

        print('导入原始用券数据 over')
        return df_原始用券
    
    #导入大类维度的原始的OIcsv文件
    def load_data_category_oi(self,*file_list):
        #配置参数

        company=self.in_company
        date_Range=self.in_date_Range
        date_Range_all=self.in_date_Range_all
        load_data_path=self.in_path
        put_file_path=self.out_path
        load_public_data_path=self.public_path
        
        read_file = rw_file.rw_csv()
        
        
        in_file_oi=file_list[0]
        
        ###导入采购收入xls
        df_采购收入原始数据 = read_file.r_csv_ansi(load_public_data_path + in_file_oi,0,0)
        # 删选省区业绩的采购收入
        bool2=~df_采购收入原始数据['扣项类型'].str.contains('装卸|搬运')
        df_采购收入原始数据=df_采购收入原始数据[bool2]
        df_采购收入原始数据.rename(columns={'发生机构':'门店DC编码'},inplace=True)


        print('导入原始OI数据 over')
        return df_采购收入原始数据
    

        



    

