
#!/usr/bin/env python
# coding: utf-8
from numpy import float64
import pandas as pd
from dq_work import rw_file
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
engine=create_engine('mysql+pymysql://root:dqdqdq@localhost:3306/db1?charset=utf8',encoding='utf-8')
dq_work_engine=create_engine('mysql+pymysql://root:dqdqdq@localhost:3306/dq_work?charset=utf8',encoding='utf-8')

class fresh_analysis:
    def __init__(self,in_company,in_date_Range,in_date_Range_all,in_path,out_path,public_path):
        self.in_company = in_company
        self.in_date_Range = in_date_Range
        self.in_date_Range_all=in_date_Range_all
        self.in_path=in_path
        self.out_path=out_path
        self.public_path=public_path
           
    def Coupons_fresh(self):
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
        file_out='01-用劵分摊-商品层级-原版-'+company+ '-生鲜-'+date_Range_all+'.csv'
        
        #选择生鲜模块
        bool_fresh1= df_券金额原始数据['部门名称'].str.contains('美食|新鲜食材')
        #bool_fresh2= df_券金额原始数据['业态名称'].str.contains('CiTY|大卖场|MART')
        #bool_fresh= bool_fresh1&bool_fresh2
        
        df_fresh=df_券金额原始数据[bool_fresh1]
        


        #导出生鲜券文件
        rw_file1.w_csv(df_fresh,load_data_path+file_out)

        print('生鲜模块的券数据的csv文件导出完成')
        return
    
    #load 生鲜的原始数据进入mysql数据库
    def load_data_sx_no_oi(self,*file_list):
        try:
            from sqlalchemy.orm import sessionmaker
            
            
            #删除清理数据表
            Session = sessionmaker(bind=dq_work_engine)
            session = Session()
            session.begin()
            sql_proc1='''drop table if exists xs_sku_all_sx;
                   '''
            sql_proc2='''
                     drop table if exists xs_quan_sx;
                   '''
            session.execute(sql_proc1)
            session.execute(sql_proc2)

            session.commit()
            session.close()
            #load数据表

            rw_file1 = rw_file.rw_csv()


            #in_file1='03_业务报表-销售报表-销售.csv'
            in_file1=file_list[0]
            #in_file2='01-用劵分摊-商品层级-原版.csv'
            in_file2=file_list[1]
            #导入文件的目录
            in_file_dir=file_list[2]

            df_in1 = rw_file1.r_csv_utf8(in_file_dir + in_file1,0,0)
            df_in2 = rw_file1.r_csv_utf8(in_file_dir + in_file2,0,0)

            df_in1.to_sql('xs_sku_all_sx',dq_work_engine,index = False,if_exists='append')
            df_in2.to_sql('xs_quan_sx',dq_work_engine,index = False,if_exists='append')
            print('data load over')
            print(df_in1.loc[:,['销售净额','毛利额']].sum())
            print(df_in2.loc[:,['万家承担金额（去税）']].sum())

        except Exception as e:
            session.rollback()
            session.close()
            print('Error:', e)
            
            
            
            #生鲜毛利计划
    def gross_profit_sx_no_oi(self,company):
        from sqlalchemy.orm import sessionmaker
        #调用存储过程进行毛利分析的计算
        Session = sessionmaker(bind=dq_work_engine)
        session = Session()
        session.begin()
        sql_proc1= 'call xs_gross_profit_sx(\''
        sql_proc2= '\');'
        sql_proc=sql_proc1+company+sql_proc2

        session.execute(sql_proc)

        session.commit()
        session.close()
        print('生鲜不包含OI的毛利 put mysql over  table is xs_sx_out')
        
    def gross_profit_sx_oi(self):
        from dq_work import work1
        read_file = rw_file.rw_csv()
        #导入文件清单
        ######################################
    
        file1='03_业务报表-销售报表-销售-'+self.in_company+ '-生鲜-'+self.in_date_Range_all+'.csv'
        file2='采购收入_'+self.in_company+ '_'+self.in_date_Range_all+'.csv'
        file3='B-分析-品类基础资料.csv'

        #读入DF
        ######################################
        df_单品销售原始数据 = read_file.r_csv_utf8(self.in_path+file1,0,0)
        
        df_采收原始数据    =    read_file.r_csv_ansi(self.public_path+file2,0,0)
        # 删选省区业绩的采购收入
        bool1=~df_采收原始数据['扣项类型'].str.contains('装卸|搬运')
        df_采收原始数据=df_采收原始数据[bool1]

        ###导入类别主档数据
        df_类别 = read_file.r_csv_utf8(self.public_path+file3,0,0)

        print('FRESH oi data load over')
        
        
        #处理原始DF
        df_单品销售原始数据.rename(columns={'供应商业务码':'供应商编码'},inplace=True)
        bool2=df_单品销售原始数据['销售净额']>0
        df_单品销售原始数据=df_单品销售原始数据[bool2]
        
        
        #将采购收入按照供应商+大类分摊到单品上
        ##############################################
        analysis_1=work1.analysis_index()

        #根据供应商与大类，计算单品的销售比例
        g_供应商_大类 =df_单品销售原始数据.groupby([ '供应商编码','大类编码'])
        df_OI_fentan=pd.DataFrame(g_供应商_大类[[ '供应商编码','大类编码','商品编码','销售净额']] \
                                       .apply(analysis_1.fentan_oi_sku))


        g_供应商_大类_OI=df_采收原始数据.groupby(['供应商编码','大类编码'])
        df_OI=pd.DataFrame(g_供应商_大类_OI[['金额']].sum())
        #取消索引
        df_OI.reset_index(inplace=True)


        df_out1=pd.merge(df_OI_fentan,df_OI,on=['供应商编码','大类编码'])
        #计算分摊金额
        df_out1['OI分摊金额']=df_out1['OI分摊比例']*df_out1['金额']




        
        #属于生鲜的采购，但是，上述到供应商+大类没有匹配上的采购，进一步再分摊
        ###处理生鲜类别的大类数据

        df_类别=df_类别.fillna(value='aa')
        bool_11=df_类别['模块名称'].str.contains('新鲜食材|美食')
        bool_12=~df_类别['大类名称'].str.contains('删除禁用')
        df_生鲜类别=df_类别[bool_11 & bool_12]

        ###没有匹配上的、属于生鲜类别的采购收入
        df_not_join =pd.merge(df_OI_fentan,df_OI,on=['供应商编码','大类编码'],how='right')
        bool_no_join = df_not_join ['商品编码'].isna()
        bool_in_dl = df_not_join['大类编码'].isin(df_生鲜类别.大类编码)
        df_not_join_oi=df_not_join.loc[bool_no_join & bool_in_dl]
        
        #将没有匹配上的生鲜采购收入，在按照大类匹配到单品上
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
        
        #合并没有匹配上的oi
        df_out1_1_1=df_out1.append(df_out1_1)
        
        #df_out1将OI分摊到了商品编码，但是，因多渠道原因，还需要汇聚一下重复的sku编码
        g_out1_sku=df_out1_1_1.groupby([ '商品编码'])
        df_sku_OI=pd.DataFrame(g_out1_sku[['OI分摊金额']].sum())
        df_sku_OI.reset_index(inplace=True)
        
        #再各单品在不同门店的销售占比，进一步分摊oi到单店单品

        #计算到单店单品汇聚的销售额
        g_shop_sku =df_单品销售原始数据.groupby([ '门店DC编码','商品编码'])
        df_shop_sku_xsje=pd.DataFrame(g_shop_sku[['销售净额']].sum())
        df_shop_sku_xsje.reset_index(inplace=True)

        #再按sku分组，计算各店的占比
        g_sku_shop=df_shop_sku_xsje.groupby(['商品编码'])

        df_OI_fentan_sku_shop=pd.DataFrame(g_sku_shop[[ '门店DC编码','商品编码','销售净额']] \
                                       .apply(analysis_1.fentan_oi_sku))
        
        
        df_out2=pd.merge(df_OI_fentan_sku_shop,df_sku_OI,on=['商品编码'],how='right')
        bool3_2=df_out2['OI分摊比例'].isna()
        df_out2.loc[bool3_2,'OI分摊比例']=1

        df_out2['OI分摊金额_shop_sku']=df_out2['OI分摊比例']*df_out2['OI分摊金额']

        #删除清理数据库中的表，可选择性操作
        Session = sessionmaker(bind=dq_work_engine)
        session = Session()
        session.begin()
        sql_proc1='''
                 DROP TABLE if EXISTS  xs_sx_out_oi_shop_sku;
                  '''
        session.execute(sql_proc1)
        session.commit()
        session.close() 

        #导出到数据库
        df_out2.to_sql('xs_sx_out_oi_shop_sku',dq_work_engine,index = False,if_exists='append')
        
        
        #输出原始的生鲜大类的OI总额，用于比对正确性
        
        bool_check = df_采收原始数据['大类编码'].isin(df_生鲜类别.大类编码)
        df_采收原始数据_生鲜大类=df_采收原始数据.loc[bool_check]
        print(df_采收原始数据_生鲜大类.loc[:,['金额']].sum())   
               
        print('xs_sx_out_oi_shop_sku table load data  over')
     
    def fresh_oi_merge(self):
        #合并生鲜的销售、券、oi的单店单品数据到同一个表，利于查询效率

        Session = sessionmaker(bind=dq_work_engine)
        session = Session()
        session.begin()
        sql_proc1='''
                 call fresh_oi_merge();
                 
                  '''

        session.execute(sql_proc1)
       
        session.commit()
        session.close()  

        print('生鲜包含OI的总毛利 put mysql over  table is xs_sx_out_1 ')


