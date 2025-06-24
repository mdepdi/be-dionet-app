import pandas as pd

def marketShareType(optList):
    
    optList = sorted(optList)
    ms_type = None
    
    if optList == ['IOH', 'SF', 'TSEL', 'XL']:
        ms_type = "ms_type_1"
    elif optList == ['IOH', 'SF', 'TSEL']:
        ms_type = "ms_type_2"
    elif optList == ['SF', 'TSEL', 'XL']:
        ms_type = "ms_type_3"
    elif optList == ['IOH', 'TSEL', 'XL']:
        ms_type = "ms_type_4"
    elif optList == ['IOH', 'TSEL']:
        ms_type = "ms_type_5"
    elif optList == ['TSEL', 'XL']:
        ms_type = "ms_type_6"
    elif optList == ['TSEL', 'SF']:
        ms_type = "ms_type_7"
    elif optList == ['IOH', 'SF', 'XL']:
        ms_type = "ms_type_8"
    elif optList == ['IOH', 'SF']:
        ms_type = "ms_type_9"
    elif optList == ['IOH', 'XL']:
        ms_type = "ms_type_10"
    elif optList == ['SF', 'XL']:
        ms_type = "ms_type_11"
    elif optList == ['TSEL']:
        ms_type = "ms_type_12"
    elif optList == ['XL']:
        ms_type = "ms_type_12"
    elif optList == ['IOH']:
        ms_type = "ms_type_12"
    elif optList == ['SF']:
        ms_type = "ms_type_12"
    
    return ms_type