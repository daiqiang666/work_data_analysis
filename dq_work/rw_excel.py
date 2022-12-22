
#!/usr/bin/env python
# coding: utf-8

#import imp
#import t3
#imp.reload(t3)
import pandas as pd

class rw_excel:
    def __init__(self,f_name,sheet,head_row,data_row):
        self.f_name = f_name
        self.sheet = sheet
        self.head_row=head_row
        self.data_row = data_row
           
    
    def r_excel(self):
        df1=pd.read_excel(io=self.f_name,header=self.head_row,sheet_name=self.sheet,skiprows=self.data_row,index_col=None)
        return df1
   
    

    def w_excel(self,w_df,w_fname):
        #w_df.to_excel(w_fname)
        w_df.to_csv(w_fname,encoding = 'gbk',index = False)
        return

    
    def r_csv(self):
        #文件格式是UTF-8格式的，则encoding参数默认即可；如果是ANSI格式的，需要设置下面的encoding参数
        df1=pd.read_csv(filepath_or_buffer=self.f_name,header=self.head_row,skiprows=self.data_row,index_col=None)
        #df1=pd.read_csv(filepath_or_buffer=self.f_name,header=self.head_row,skiprows=self.data_row,index_col=None,encoding='mbcs')
        return df1

