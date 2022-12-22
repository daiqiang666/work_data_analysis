
#!/usr/bin/env python
# coding: utf-8

#import imp
#import t3
#imp.reload(t3)
import pandas as pd
import re

def f_mohu(row):
    return re.search(row['key1'],['shopname2'],re.IGNORECASE) is not None

