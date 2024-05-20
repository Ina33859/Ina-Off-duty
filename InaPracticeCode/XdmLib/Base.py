# -*- coding: UTF-8 -*-
'''
 (c) Copyright 2017, XDMTECH All rights reserved.

 System name: XdmLib
 Source name: XdmLib/Base.py
 Description: XDM Common Libs package
'''
'''
 Modification history:
 Date        Ver.   Author           Comment
 ----------  -----  ---------------  -----------------------------------------
 2019/01/01  I0.00  Even Chen        Initial Release
 2023/05/02  M1.00  Kenshin          add FileType
'''
import enum


#===============================================================================
# System variable
#===============================================================================
class KeepType(enum.Enum):
    Days = 1
    Files = 2

class FileType(enum.Enum):
    Source = 'Source'
    Archive = 'Archive'
    Error = 'Error'
    Skip = 'Skip'