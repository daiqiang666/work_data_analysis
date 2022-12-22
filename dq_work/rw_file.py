
#!/usr/bin/env python
# coding: utf-8

#import imp
#import t3
#imp.reload(t3)
import pandas as pd

class rw_csv:
    #def __init__(self):
         
    
    

    def w_csv(self,w_df,w_fname):
        #w_df.to_excel(w_fname)
        w_df.to_csv(w_fname,encoding = 'utf-8',index = False)
        return


    def w_csv_ansi(self,w_df,w_fname):
        
        w_df.to_csv(w_fname,encoding='mbcs',index = False)
        return

    def r_csv_utf8(self,f_name,head_row,data_row):
        #文件格式是UTF-8格式的，则encoding参数默认即可
        df1=pd.read_csv(filepath_or_buffer=f_name,header=head_row,skiprows=data_row,index_col=None,low_memory=False)
        return df1
    
    def r_csv_ansi(self,f_name,head_row,data_row):
        #文件格式是ANSI格式的，需要设置下面的encoding参数
        
        df1=pd.read_csv(filepath_or_buffer=f_name,header=head_row,skiprows=data_row,index_col=None,encoding='mbcs',low_memory=False)
        return df1
    
    
#待使用的其他参数
#分隔符不是‘,'
#df1=pd.read_csv(filepath_or_buffer=f_name,header=head_row,skiprows=data_row,index_col=None,sep='|')