
#!/usr/bin/env python
# coding: utf-8
from numpy import float64
import pandas as pd
import numpy as np
from dq_work import rw_file


class public_data_prepare:
    def __init__(self,in_company,in_path,out_path):
        self.in_company = in_company
        
        self.in_path=in_path
        self.out_path=out_path
           
    def Coupons_data(self,*date_Range_list):
        
        #配置参数
        company=self.in_company
        date_Range_1=date_Range_list[0]
        date_Range_2=date_Range_list[1]
        date_Range_3=date_Range_list[2]
        date_Range_4=date_Range_list[3]
        date_Range_5=date_Range_list[4]
        date_Range_6=date_Range_list[5]
        
        date_Range  = date_Range_list[6]
        
        rw_file1 = rw_file.rw_csv()

        #需要准备的导入文件清单
        ######################################
        file_dir_0=self.in_path

        file_1='01-用劵分摊-商品层级-原版-'+company+ '-all-'+date_Range_1+'.csv'
        file_2='01-用劵分摊-商品层级-原版-'+company+ '-all-'+date_Range_2+'.csv'
        file_3='01-用劵分摊-商品层级-原版-'+company+ '-all-'+date_Range_3+'.csv'
        file_4='01-用劵分摊-商品层级-原版-'+company+ '-all-'+date_Range_4+'.csv'
        file_5='01-用劵分摊-商品层级-原版-'+company+ '-all-'+date_Range_5+'.csv'
        file_6='01-用劵分摊-商品层级-原版-'+company+ '-all-'+date_Range_6+'.csv'
        
        
        ######################################
        #合并导出的文件

        file_out='01-用劵分摊-商品层级-原版-'+company+ '-all-'+date_Range+'.csv'
        ######################################

        df_券金额1 = rw_file1.r_csv_utf8(file_dir_0 + file_1,0,0)
        df_券金额2 = rw_file1.r_csv_utf8(file_dir_0 + file_2,0,0)
        df_券金额3 = rw_file1.r_csv_utf8(file_dir_0 + file_3,0,0)
        df_券金额4 = rw_file1.r_csv_utf8(file_dir_0 + file_4,0,0)
        df_券金额5 = rw_file1.r_csv_utf8(file_dir_0 + file_5,0,0)
        df_券金额6 = rw_file1.r_csv_utf8(file_dir_0 + file_6,0,0)
        
        
        df_券金额 =df_券金额1.append([df_券金额2,df_券金额3,df_券金额4,df_券金额5,df_券金额6])

        #检查符合数据完整性
        print(df_券金额['日期'].max())
        show1=df_券金额['日期'].unique()
        show2=np.sort(show1)
        print(show2)

        rw_file1.w_csv(df_券金额,self.out_path + file_out)

        del df_券金额
        del df_券金额1
        del df_券金额2
        del df_券金额3
        del df_券金额4
        del df_券金额5
        del df_券金额6
        print('券文件合并 ok')
        return
    

