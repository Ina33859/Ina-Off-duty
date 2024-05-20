# -*- coding: UTF-8 -*-
''' 
 (c) Copyright 2020, XDMTECH All rights reserved.
  
 Source name: GlobalSetting.py
 Description: Python Loader global setting file
 
 Modification history:
 Date        Ver.   Author           Comment
 ----------  -----  ---------------  -----------------------------------------
 2021/12/01  I0.00  Jayce Wei        Initial Release
 '''
import XdmLib.Base
#===============================================================================
# Setting
#===============================================================================
DEBUG = False
# Oracle RAC Load Balance Ref: https://docs.oracle.com/cd/E57185_01/EPMIS/apbs01s01.html
CTRL_TABLE_CONN = ""
# POSTGRESQL
# SRC_ORA_EDA = "postgresql+psycopg2://User:Password@IP:Port/db_name"
# TAR_ORA_EDA = "postgresql+psycopg2://User:Password@IP:Port/db_name"
# ORACLE
SRC_ORA_EDA = "oracle+cx_oracle://vidatest:vidatest@192.168.1.73:1521/demodb"
TAR_ORA_EDA = "oracle+cx_oracle://vidatest:vidatest@192.168.1.73:1521/demodb"

ETL_LOG_RECORD = True
PASS_LOG_RECORD =  True
FAIL_LOG_RECORD = True
ETL_LOG_TABLE = 'CONVERTER'

CTRL_TABLE_CONN = CTRL_TABLE_CONN if CTRL_TABLE_CONN else TAR_ORA_EDA

ALLOW_DUPLICATE_EXECUTE = True
IS_USING_LATESTJSON = True
#===============================================================================
# logging.yaml Path Setting
#===============================================================================
LOG_PATH=r"..\..\logging.yaml"
#===============================================================================
# End logging.yaml Path Setting
#===============================================================================

#===============================================================================
# SMTP Setting
#===============================================================================
sServer = ''
sPort = 587
sUser   = ''
sPres = ''
sPWD   = ''
bAuth = False
bSSL = False
#===============================================================================
# File to DB Setting
#===============================================================================
SOURCE_FOLDER = r'.\\Data\\Source'
SKIP_FOLDER = r'.\\Data\\Skip'
ARCHIVE_FOLDER = r'.\\Data\\Archive'
ERROR_FOLDER = r'.\\Data\\Error'
SKIP_FOLDER= r".\\Data\\Source"
XSIFF_FOLDER = r".\\Data\\xsiff"
ARCHIVE_TYPE = XdmLib.Base.KeepType.Days.value
ARCHIVE_MAX_VALUE = 30
ARCHIVE_CHECK_MIDNIGHT = 0

# Check midnight, minutes: default = -1 = 1440 check every running, 0 = don't check, other num = check when running in OO:00:00 and 00:00:00+num(min)*60

# not use not take effect
ERROR_TYPE = XdmLib.Base.KeepType.Days.value
ERROR_MAX_VALUE = 30
ERROR_CHECK_MIDNIGHT = 0 # Check midnight, minutes: default = -1 = 1440, 0 = don't check
#================================================================================
# Constant Region
#================================================================================
FAB='FAB1'
GROUP = ""
HOSTNAME = ""
#================================================================================
# End Region
#================================================================================