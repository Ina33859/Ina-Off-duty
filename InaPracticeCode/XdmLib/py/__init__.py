# -*- coding: UTF-8 -*-
'''
 (c) Copyright 2017, XDMTECH All rights reserved.

 System name: XdmLib
 Source name: XdmLib/__init__.py
 Description: XDM Common Libs package
 Version:     2.A.13
'''
'''
 Modification history:
 Date        Ver.   Author           Comment
 ----------  -----  ---------------  -----------------------------------------
 2017/12/07  I0.00  Even Chen        Initial Release
 2017/12/08  C0.01  Even Chen        Skip float convert(fix bits loss)
 2017/12/11  C0.02  Even Chen        MoveFile function enhance & Add timed rotating archive file.
 2017/12/28  A0.01  Even Chen        Add Timed rotating archive file & Control watchdog
 2018/04/18  M0.01  Even Chen        Rollback to latest version
 2018/04/20  A0.02  Even Chen        Add log: Watching control file&Path fix&(*disable)delete empty folders(source)
 2018/05/28  A0.03  Even Chen        Add percentage support
 2018/05/31  A0.04  Even Chen        Add ARCHIVE_FOLDER check(None = Disable, for Db2Db Type)
 2018/05/31  A0.05  Even Chen        Add Status 1=Done! default vaule
 2018/06/15  A0.06  Even Chen        Add error files count when SetStatus = 1(Done)
 2018/09/07  A0.07  Even Chen        Add delete error files(like Archive)&Remove empty folder(Archive, Error)&add Movefile delete folder support
 2019/01/15  A0.08  Even Chen        Keep log to DB
 2019/01/23  A0.09  Even Chen        Auto make logging folder(file base)
 2019/01/25  A0.10  Even Chen        Add ALLOW_RUN_ON_DIFF_SERVER support
 2019/02/13  A0.11  Even Chen        Add control table - comment(update by config.COMMENT)
 2019/06/01  M0.02  Even Chen        Change SQL to SQLAlchemy SQL Expression, Support any DB
 2019/06/19  A0.12  Even Chen        Add After execute SQLs
 2020/07/29  A0.13  Evelyn Sun       Support HA
 2020/08/23  A0.14  Owen Wang        Support Disable Quick Edit Mode
 2021/04/20  A0.15  Owen Wang        Logging.yaml from default File
 2021/07/22  Mo.16  Jie Dai          import GolbalSetting
 2021/11/25  M0.17  Vincent Zhou     function MoveFile() is_dir -> os.path.isdir
'''
'''
 Changelog:
 Date        Ver.     Author             Comment
 ----------  -------  -----------------  -----------------------------------------
 2022/07/15  2.A.0    Kenshin Huang      [Release] Version Control Alpha Release
 2022/07/15  2.A.1    Kenshin Huang      [Modify]  Modify skip_file parameter type
 2022/09/01  2.A.2    Kenshin Huang      [Modify]  Modify DB2DB init skip time,default not to skip.
 2022/09/02  2.A.3    Kenshin Huang      [Modify]  Modify File2DB DB commit&rollback,move file func try catch
 2022/09/02  2.A.4    Kenshin Huang      [Feat]    Add SHA256 to check base,customFuction,database
 2022/11/15  2.A.5    Kenshin Huang      [Feat]    Add ETL_LOG table record with file 2db etl(CustomFuction)
 2023/02/08  2.A.6    Kenshin Huang      [Modify]  Modify Move file by time/reason/folder(CustomFuction)
 2023/02/23  2.A.7    Kenshin Huang      [Modify]  Modify ETL Control table column name,group=>ETL_GROUP,comment=>ETL_COMMENT(init)
 2023/03/16  2.A.8    Kenshin Huang      [Modify]  Clean code;change hash check for py&pyc
 2023/03/20  2.A.9    Kenshin Huang      [Modify]  Database.py sql server do not use param
 2023/04/24  2.A.10   Kenshin Huang      [Modify]  ETL execute instance abspath,and Database.py sql server executemany fix
 2023/05/02  2.A.11   Kenshin Huang      [Fix]     Modify code,fix in some way data_time_flg would be init,fix file2DB archive/error file would not be delete,add system usage function
 2023/05/04  2.A.12   Kenshin Huang      [Fix]     Bug Fix
 2023/05/11  2.A.13   Kenshin Huang      [Modify]  Modify SQL Server Connnect driver
'''
import watchdog.observers
import XdmLib.Base
import logging.config
from sqlalchemy import Table, MetaData, func, case
from sqlalchemy.sql import select, and_
import yaml,datetime,sys,os,shutil,errno
import GlobalSetting
from glob2 import glob
import XdmLib.Database
import XdmLib.CustomFuction
try:
    import XdmLib.ExtendFunction
except Exception:
    pass

#===============================================================================
# Default variable
#===============================================================================
version = '2.A.13'

default_config_name = "config"
default_logging_config_file = 'logging.yaml'
default_global_logging_config_file = GlobalSetting.LOG_PATH
default_logging_level = logging.DEBUG
test_code_bool = False
#===============================================================================
# Global variable
#===============================================================================
message = ''
#===============================================================================
# Version Check
#===============================================================================
def check_xdmlib_sha256(func,check = False):
    fail = False
    import hashlib
    import pathlib
    import os
    fpath = pathlib.Path(__file__).parent.resolve()
    for file in os.listdir(fpath):
        hash_check = None
        if file.startswith('Base'):
            if file.endswith('.py'):
                hash_check = 'cf5ea04a657209e7befc17f15a95ea86f3c6f7f1a723130e7c7ce1b0dafa6366'
            elif file.endswith('.pyc'):
                hash_check = '6fa720c44f715886bb33aca4cc355955d5027e8fb041c3674595509d4a4a9f37'
        elif file.startswith('CustomFuction'):
            if file.endswith('.py'):
                hash_check = 'd54645d81d5badda066535298ed2c64c8f0c46f99d90e41c6c6f1c86a34d7a91'
            elif file.endswith('.pyc'):
                hash_check = 'b5c3a8938d030aa7e59e840c21601cefde3fafea787ff0df4040543d7db3dff4'
        elif file.startswith('Database'):
            if file.endswith('.py'):
                hash_check = '343fd3acf5c81501475b19cab29dd842881a2f11e688b80d9ff060b7d136dfe5'
            elif file.endswith('.pyc'):
                hash_check = 'b1992a262bec8a70dbe65ea8d79fc4d4f0383b4d0dc361502d5f6f24d857c659'
        if hash_check:
            with open(os.path.join(fpath,file),"rb") as f:
                bytes = f.read()
                readable_hash = hashlib.sha256(bytes).hexdigest()
                if hash_check != readable_hash:
                    func(f'Check {file} SHA256 Failed')
                    fail = True
        if fail and check and not test_code_bool:
            raise Exception('XdmLib Version Error')

def ver_control(print_check=1):
    if print_check == 1:
        print("# ===== [ XdmLib Ver. ] ===== #")
        print("XdmLib Ver. {0}".format(version))
        print("# ===== [ XdmLib Ver. ] ===== #")
        check_xdmlib_sha256(print,False)
    if print_check == 0:
        logging.info("# ===== [ XdmLib Ver. ] ===== #")
        logging.info("XdmLib Ver. {0}".format(version))
        logging.info("# ===== [ XdmLib Ver. ] ===== #")
        check_xdmlib_sha256(logging.error,True)
#===============================================================================
# Disable quick edit mode
#===============================================================================
def quickedit(enabled=1): # A0.14
    import ctypes
    '''
    Enable or disable quick edit mode to prevent system hangs, sometimes when using remote desktop
    Param (Enabled)
    enabled = 1(default), enable quick edit mode in python console
    enabled = 0, disable quick edit mode in python console
    '''
    # -10 is input handle => STD_INPUT_HANDLE (DWORD) -10 | https://docs.microsoft.com/en-us/windows/console/getstdhandle
    # default = (0x4|0x80|0x20|0x2|0x10|0x1|0x40|0x200)
    # 0x40 is quick edit, #0x20 is insert mode
    # 0x8 is disabled by default
    # https://docs.microsoft.com/en-us/windows/console/setconsolemode
    kernel32 = ctypes.windll.kernel32
    if enabled:
        kernel32.SetConsoleMode(kernel32.GetStdHandle(-10), (0x4|0x80|0x20|0x2|0x10|0x1|0x40|0x100))
        logging.info("Console Quick Edit Enabled")
    else:
        kernel32.SetConsoleMode(kernel32.GetStdHandle(-10), (0x4|0x80|0x20|0x2|0x10|0x1|0x00|0x100))
        logging.info("Console Quick Edit Disabled")
#===============================================================================
# Send Mail
#===============================================================================
def sendmail(sServer, sPort, sUser, sPres,
            sPWD, lstTo, lstCC, sSubject, sBody, bAuth=True, bSSL=True):
    # Import smtplib to provide email functions
    import smtplib, ssl
    # Import the email modules
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.header import Header
    from email.utils import formataddr
    # Construct email
    msg = MIMEText(sBody, 'plain')
    msg['To'] = ",".join(lstTo)
    msg['CC'] = ",".join(lstCC)
    msg['From'] = formataddr((str(Header(sPres, 'utf-8')), sUser))
    msg['Subject'] = sSubject
    mailList = lstTo + lstCC
    # Send the message via an SMTP server
    try:
        server = smtplib.SMTP(sServer,sPort)
        if bSSL:
            # Create a secure SSL context
            context = ssl.create_default_context()
            server.ehlo() # Can be omitted
            server.starttls(context=context) # Secure the connection
            server.ehlo() # Can be omitted
        if bAuth:
            server.login(sUser,sPWD)
        server.sendmail(sUser, mailList, msg.as_string())
        logging.info('Send Mail Success')
    except Exception as ex:
        logging.error(ex, exc_info=True, stack_info=True)
    finally:
        server.quit()
#===============================================================================
# timed_rotating_file
#===============================================================================
def timed_rotating_file(rotating_file_type:str=None):
    dst_folder = None
    _type = XdmLib.Base.KeepType.Days.value
    _value = 7
    _check_midnight = 0 # Check midnight, minutes: default = -1 = 1440, 0 = don't check
    _check = False
    try:
        src_folder = config.SOURCE_FOLDER
        if rotating_file_type == XdmLib.Base.FileType.Archive.value:
            _check = True
            _type = config.ARCHIVE_TYPE
            _value = config.ARCHIVE_MAX_VALUE
            _check_midnight = config.ARCHIVE_CHECK_MIDNIGHT
            tgt_folder = config.ARCHIVE_FOLDER
        elif rotating_file_type == XdmLib.Base.FileType.Error.value:
            _check = True
            _type = config.ERROR_TYPE
            _value = config.ERROR_MAX_VALUE
            _check_midnight = config.ERROR_CHECK_MIDNIGHT
            tgt_folder = config.ERROR_FOLDER
        dst_folder = os.path.realpath(tgt_folder)
        if dst_folder and _check:
            if _type == XdmLib.Base.KeepType.Days.value:
                _check = True
                logging.info(f"Load {rotating_file_type} setting - Type: Last {_value} days.")
            elif _type == XdmLib.Base.KeepType.Files.value:
                _check = True
                logging.info(f"Load {rotating_file_type} setting - Type: Keep last {_value} files.")
            if _check_midnight > 0:
                midnight = datetime.datetime.combine(start_time.date(), datetime.time())
                seconds = (start_time - midnight).seconds
                _check_midnight = -1 if seconds < _check_midnight * 60 else 0
            if _check_midnight == -1 and _check:
                if _type == XdmLib.Base.KeepType.Days.value:
                    deltatime = datetime.datetime.now() - datetime.timedelta(days=_value)
                    files = filter(os.path.isfile, glob(os.path.join(dst_folder, '**/*.*'), recursive=True))
                    count = 0
                    for fn in files:
                        if datetime.datetime.fromtimestamp(os.path.getmtime(fn)) < deltatime:
                            try:
                                os.remove(fn)
                                count += 1
                            except IOError as err:
                                logging.debug(err)
                    logging.info(f"{rotating_file_type} files({count}) deleted, Keep Last {_value} days.")
                elif _type == XdmLib.Base.KeepType.Files.value:
                    files = sorted(filter(os.path.isfile, glob(os.path.join(dst_folder, '**/*.*'), recursive=True)), key=os.path.getmtime, reverse=True)
                    count = 0
                    for fn in (files[_value:] if len(files) > _value else files):
                        try:
                            os.remove(fn)
                            count += 1
                        except IOError as err:
                            logging.debug(err)
                    logging.info(f"{rotating_file_type} files({count}) deleted, Keep last {_value} files.")
                try:
                    XdmLib.CustomFuction.delete_empty_folder(src_folder,src_folder,logging)
                    XdmLib.CustomFuction.delete_empty_folder(tgt_folder,tgt_folder,logging)
                except Exception as ex:
                    logging.warning("Delete empty folders fail.")
        else:
            logging.info(f"{rotating_file_type} folder non-setting, source file will be delete.")
    except Exception as ex:
        logging.info(f"{rotating_file_type} setting non-setting, {rotating_file_type} files will be kept.")
#===============================================================================
# table_access
#===============================================================================
def table_access(table_str:str,table_meta:MetaData,oracle_resolve_synonyms:bool=False):
    if oracle_resolve_synonyms:
        return Table(table_str, table_meta, autoload=True,oracle_resolve_synonyms=True)
    else:
        return Table(table_str, table_meta, autoload=True)
#===============================================================================
### 0=InProcess,1=Success,2=Testing,9=Down/CMD,-1:Run,-2=Cancel,-9=Stop
# 0=InProcess,1=Success,2=Testing,3=Skip,5=Error,9=Down/CMD
#===============================================================================
def SetProcessStatus(status,sBody=''):
    if status in [0, 2]:  # 0: InProcess, 2: Debug
        logging.info(f"SetProcessStatus 0, Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S.%f')}")
    global message
    global db
    global stmt
    global meta
    global xdm_prog_control
    db = None
    stmt = None
    meta = None
    xdm_prog_control = None
    try:
        db = XdmLib.Database.DbClient(config.CTRL_TABLE_CONN, logging_obj=logging, enable_debug=enable_debug, encoding="utf-8")
        meta = MetaData(db._dbEngine)
        xdm_prog_control = table_access('xdm_prog_control', meta,oracle_resolve_synonyms)
        db.begin()
        
        if status == 1:
            message = "Success"
            if log_db_path:
                log_file = None
                log_db_content = None
                try:
                    log_file = open(log_db_path, 'rb')
                    log_db_content = log_file.read()
                    stmt = xdm_prog_control.update().where(
                        and_(
                            xdm_prog_control.c.program == program,
                            xdm_prog_control.c.instance == instance
                        )
                    ).values(
                        {
                            'status': status,
                            'message': message,
                            'last_end_time': datetime.datetime.now(),
                            'data_time_flg': last_data_time,
                            'last_log': log_db_content
                        }
                    )
                    db._connObj.execute(stmt)
                    db.commit()
                except Exception as ex:
                    logging.error(ex)
                    logging.debug(ex, exc_info=True, stack_info=True)
                finally:
                    if log_file:
                        log_file.close()
                        log_file = None
                    del log_file
                    if log_db_content: log_db_content = None
                    del log_db_content
            else:
                stmt = xdm_prog_control.update().where(
                    and_(
                        xdm_prog_control.c.program == program,
                        xdm_prog_control.c.instance == instance
                    )
                ).values(
                    {
                        'status': status,
                        'message': message,
                        'last_end_time': datetime.datetime.now(),
                        'data_time_flg': last_data_time
                    }
                )
                db._connObj.execute(stmt)
                db.commit()
            logging.info(f"Set process status code: {status}, Last data time: {last_data_time if not last_data_time else last_data_time.strftime('%Y-%m-%d %H:%M:%S.%f')}.")
        else:
            if status == 9:
                message = sBody
                stmt = select(
                               [
                                   func.count()
                               ]
                        ).where(
                            and_(
                                xdm_prog_control.c.program == program,
                                xdm_prog_control.c.instance == instance,
                                xdm_prog_control.c.status == status
                                )
                        )
                count = db._connObj.execute(stmt).fetchall()[0][0]
                if count == 0:
                    errName = sBody.strip().split('\n')[-1]
                    sSubject = f'[Error] program: {program}, errmsg: {errName}'
                    message = f'hostname: {hostname} \n Error Message: {message}'
                    sendmail(config.sServer, config.sPort, config.sUser, config.sPres, config.sPWD, config.lstTo, config.lstCC, sSubject, sBody, config.bAuth, config.bSSL)    
            elif status == 0:
                message = 'InProcess'
            elif status == 3:
                message = 'Skip'
            elif status == 5:
                message = 'Error'
            stmt = xdm_prog_control.update().where(
                and_(
                    xdm_prog_control.c.server == hostname,
                    xdm_prog_control.c.program == program,
                    xdm_prog_control.c.instance == instance
                )
            ).values(
                {
                    'status': status,
                    'message': message,
                    'last_end_time': datetime.datetime.now()
                }
            )
            db._connObj.execute(stmt)
            db.commit()
            logging.info(f"Set process status code: {status}.")
    except Exception as ex:
        if db:
            logging.debug(stmt)
            db.rollback()
        logging.debug(ex, exc_info=True, stack_info=True)
        logging.error("Access XDM Loader control table fail.")
    finally:
        xdm_prog_control = None
        if meta:
            meta.clear()
            meta = None
        del meta
        if db:
            db.disconnect()
            db = None
        del db

def WriteToErrorLog(event_code, function_name, event_desc, errormsg ,isWriteToLogFile):
    db = None
    meta = None
    xdm_prog_err_log = None
    try:
        db = XdmLib.Database.DbClient(config.CTRL_TABLE_CONN, logging_obj=logging, enable_debug=enable_debug, encoding="utf-8")
        meta = MetaData(db._dbEngine)
        xdm_prog_err_log = table_access('xdm_prog_err_log', meta,oracle_resolve_synonyms)
        if errormsg != '':
            db.begin()
            stmt = xdm_prog_err_log.insert().values(
                {
                    'server': hostname,
                    'program': program,
                    'instance': instance,
                    'event_code': event_code,
                    'event_desc': event_desc,
                    'program_name': function_name,
                    'message': errormsg,
                    'update_time':datetime.datetime.now()
                }
            )
            
            db._connObj.execute(stmt)
            db.commit()
            
    except Exception as ex:
        if db:
            logging.debug(stmt)
            db.rollback()
        logging.debug(ex, exc_info=True, stack_info=True)
        logging.error("Access XDM Loader error table fail.")
    finally:
        if isWriteToLogFile:
            logging.error(event_desc+'\r\n                                                Error: '+errormsg)
        
        xdm_prog_err_log = None
        if meta:
            meta.clear()
            meta = None
        del meta
        if db:
            db.disconnect()
            db = None
        del db

#===============================================================================
# Convert & Check data raw_type
# cx_Oracle: http://www.oracle.com/technetwork/articles/dsl/prez-python-queries-101587.html
#===============================================================================
def DataConvert(dictdata, t, key, nullable=True, default=None, strfmt=None):
    if not dictdata or type(dictdata) is not dict:
        raise Exception("Input data must be dict raw_type.", type(dictdata))
    elif not nullable and not default and not dictdata[key]:
        raise Exception("Not nullable can't set default value is null.")
    elif t is int:
            dictdata[key] = int(dictdata[key]) if dictdata[key] else default
    elif t is float:
        if dictdata[key] and dictdata[key][-1:] == '%':
            import decimal
            dictdata[key] = decimal.Decimal(dictdata[key].rstrip('%')) / 100 if dictdata[key] else default
        else:
            dictdata[key] = dictdata[key].replace(' ', '')
            float(dictdata[key]) if dictdata[key] else default
            dictdata[key] = dictdata[key] if dictdata[key] else default
    elif t is datetime:
        dictdata[key] = datetime.datetime.strptime(str(dictdata[key]), strfmt) if dictdata[key] else default
    elif t is str:
        dictdata[key] = dictdata[key] if dictdata[key] else default
    else:
        raise Exception("The data raw_type({0}) is not supported".format(t))
#===============================================================================
# Move file
# src(Source): file/directory path.
# dst(Destination): file/directory path.
# sub_folder: Move with sub-folders structure
#===============================================================================
def MoveFile(src, dst, sub_folder=True):
    if not os.path.exists(src):
        raise Exception("Source: '{0}' isn't exists.".format(src))

    if dst:
        _dst = dst
        try:
            # Copy sub-folder(base on config.SOURCE_FOLDER)
            if sub_folder and os.path.isfile(src):
                rel = os.path.relpath(os.path.realpath(src), os.path.realpath(config.SOURCE_FOLDER))
                if rel:
                    _dst = os.path.realpath(os.path.join(_dst, rel))
            # Move file
            if os.path.isdir(_dst):
                return shutil.copy(src, _dst)
            else:
                return shutil.move(src, _dst, copy_function=shutil.copy2)
        except IOError as ex:
            if ex.errno == errno.ENOENT:  # No such file or directory
                # try creating parent directories
                os.makedirs(os.path.dirname(_dst))
                return shutil.move(src, _dst, copy_function=shutil.copy2)
            elif ex.errno == errno.EEXIST:  # File exists
                return shutil.move(src, os.path.join(_dst, os.path.basename(src) + ".{:%Y%m%d_%H%M%S.%f}".format(datetime.datetime.now())), copy_function=shutil.copy2)
            else:
                raise ex
    else:
        if os.path.isfile(src):
            return os.remove(src)
        else:
            return shutil.rmtree(src, ignore_errors=True)

#===============================================================================
# System Usage
#===============================================================================
def system_usage(function_name=print):
    try:
        import os, psutil
        process = psutil.Process()
        function_name(f'CPU Usage: {process.cpu_percent()} %')
        function_name(f'Memory Usage: {process.memory_info().rss/1024/1024} MB')
    except Exception as ex:
        pass

#===============================================================================
# XdmLib Start
#===============================================================================
ver_control(1)

start_time = datetime.datetime.now()
print("Starting datetime ....: %s" % start_time.strftime("%Y-%m-%d %H:%M:%S.%f"))

argv = sys.argv
instance = os.path.abspath(' '.join(str(x) for x in argv))
print("Starting instance ....: %s" % instance)

cwd = os.getcwd()
print("Current working folder: %s" % cwd)

program = os.path.basename(argv[0])

#===============================================================================
# Load logging configure file(YAML format)
#===============================================================================
try:
    logging_config_file = ''
    if os.path.exists(default_logging_config_file):
        logging_config_file = default_logging_config_file
    elif os.path.exists(default_global_logging_config_file):
        logging_config_file = default_global_logging_config_file
    else:
        logging_config_file = ''
    if logging_config_file != '':
        progname = os.path.basename(instance).replace('.pyc','').replace('.py','')
        with open(logging_config_file, 'rt') as fn:
            logging.info(fn)
            logging_config = yaml.safe_load(fn.read())
            logging_config["handlers"]["file"]["filename"]=logging_config["handlers"]["file"]["filename"].replace('{progname}',progname)
            logging_config["handlers"]["file_error"]["filename"]=logging_config["handlers"]["file_error"]["filename"].replace('{progname}',progname) 
            for h in logging_config.get('handlers', []):
                logging.info(h)
                log_file = logging_config['handlers'].get(h, {}).get('filename')
                if log_file:
                    logging.info(log_file)
                    log_file = os.path.dirname(os.path.realpath(log_file))
                    if not os.path.isdir(log_file):
                        os.makedirs(log_file, exist_ok=False)
            logging.config.dictConfig(logging_config)
    else:
        logging.basicConfig(level=default_logging_level)
        logging.warning("File not found: %s.", os.path.abspath(default_logging_config_file))
        logging.info("Logging config file not found! Using default logging.")
except Exception as ex:
    print(ex)
    sys.exit(11)

#===============================================================================
# XdmLib Ver. Control
# Using logging anyway
#===============================================================================
ver_control(0)

#===============================================================================
# Quick edit mode
#===============================================================================
try:
    quickedit(0) # Disable quick edit in terminal
except Exception as ex:
    pass

#===============================================================================
# Load configure file
#===============================================================================
try:
    args = argv[1:]
    import importlib
    if len(args) > 0 and os.path.isabs(args[0]):
        import importlib.machinery
        loader = importlib.machinery.SourceFileLoader(os.path.splitext(args[0])[0], args[0])
        import types
        config = types.ModuleType(loader.name)
        loader.exec_module(config)
    else:
        if len(args) > 0:
            if os.path.isfile(args[0]):
                default_config_name = args[0].rstrip(".py")
            elif os.path.isfile(args[0] + ".py"):
                default_config_name = args[0]
        config = importlib.import_module(default_config_name)
    config_dir = dir(config)
except Exception as ex:
    logging.error(ex, exc_info=True, stack_info=True)
    sys.exit(12)

#===============================================================================
# Check config information
#===============================================================================
hostname = ''
try:
    if 'HOSTNAME' in config_dir:
        hostname = config.HOSTNAME
    if not hostname:
        import socket
        hostname = socket.gethostname() if socket.gethostname().find('.') >= 0 else socket.gethostbyaddr(socket.gethostname())[0]       
except Exception:
    pass
logging.info("Server/Host Name: %s", hostname)

etl_group = None
try:
    if 'GROUP' in config_dir:
        etl_group = config.GROUP if config.GROUP else None
except Exception:
    pass

comment_text = None
try:
    if 'COMMENT' in config_dir:
        comment_text = config.COMMENT if config.COMMENT else None
except Exception:
    pass

oracle_resolve_synonyms = False
try:
    if 'ORACLE_DB_LINK' in dir(GlobalSetting):
        oracle_resolve_synonyms = config.ORACLE_DB_LINK if config.ORACLE_DB_LINK else False
except Exception:
    pass

#===============================================================================
# Timed rotating archive file
#===============================================================================
timed_rotating_file(XdmLib.Base.FileType.Archive.value)

timed_rotating_file(XdmLib.Base.FileType.Error.value)
#===============================================================================
# A0.08 Keep log to DB
#===============================================================================
log_db_path = None
log_db_handler = None
try:
    log_db_handler = config.LOG_DB_HANDLER
    for l in logging.getLoggerClass().root.handlers:
        if l.name == log_db_handler:
            log_db_path = l.baseFilename
            logging.debug("log to DB file: %s", log_db_path)
            break
    if not log_db_path:
        logging.error("Can't get log file by name: '%s'.", log_db_handler)
except AttributeError as ex:
    pass

#===============================================================================
# enable debug mode flag
#===============================================================================
enable_debug = False
try:
    enable_debug = config.DEBUG
except AttributeError as ex:
    pass

#===============================================================================
# XDM control table
#===============================================================================
status = 0
last_data_time = None
last_start_time=None
is_error=False
db = None
meta = None
stmt = None
try:
    db = XdmLib.Database.DbClient(config.CTRL_TABLE_CONN, logging_obj=logging, enable_debug=enable_debug, encoding="utf-8")
    meta = MetaData(db._dbEngine)
    xdm_prog_control = table_access('xdm_prog_control', meta,oracle_resolve_synonyms)
    stmt = select(
            [
                xdm_prog_control.c.server,
                xdm_prog_control.c.status,
                xdm_prog_control.c.data_time_flg,
                xdm_prog_control.c.last_start_time
            ]
        ).where(
            and_(
                xdm_prog_control.c.program == program,
                xdm_prog_control.c.instance == instance
                )
        ).order_by(
            case(
                [
                    (xdm_prog_control.c.server == hostname, 1)
                ], else_=0
            )
        )
    result = db._connObj.execute(stmt)
    is_current_server = False
    for row in result:
        server = row[0]
        status = row[1]
        last_data_time = row[2]
        last_start_time = row[3]
        if server == hostname:
            is_current_server = True
            logging.info("Server: %s, Status: %s, data_time_flg: %s.", server, status, last_data_time)
            if status == -9:
                logging.error("Loader set status 'DISABLE'! sys.exit(%d).", status)
                sys.exit(status)
        allow_duplicate_execute = True
        try:
            allow_duplicate_execute = config.ALLOW_DUPLICATE_EXECUTE
        except AttributeError as ex:
            if status == 0:
                if not server:
                    pass
                elif server == hostname:
                    logging.warning("The loader duplicate execute(Set attribute: 'ALLOW_DUPLICATE_EXECUTE = False' disable it).")
                else:
                    logging.warning("The loader duplicate execute on different server: %s(Set attribute: 'ALLOW_DUPLICATE_EXECUTE = False' disable it).", server)
        if not allow_duplicate_execute and status == 0:
            status = 13
            logging.error("The loader duplicate execute! Please check status. sys.exit(%d).", status)
            sys.exit(status)
        if is_current_server: 
            break
    db.begin()
    if is_current_server:
        xdm_prog_control_his = None
        try:
            xdm_prog_control_his = table_access('xdm_prog_control_his', meta,oracle_resolve_synonyms)
            stmt = xdm_prog_control_his.insert().from_select(
                    [
                        xdm_prog_control_his.c.server,
                        xdm_prog_control_his.c.program,
                        xdm_prog_control_his.c.instance,
                        xdm_prog_control_his.c.etl_group,
                        xdm_prog_control_his.c.etl_comment,
                        xdm_prog_control_his.c.status,
                        xdm_prog_control_his.c.message,
                        xdm_prog_control_his.c.last_start_time,
                        xdm_prog_control_his.c.last_end_time,
                        xdm_prog_control_his.c.data_time_flg,
                        xdm_prog_control_his.c.update_time
                    ],
                    select(
                    [
                        xdm_prog_control.c.server,
                        xdm_prog_control.c.program,
                        xdm_prog_control.c.instance,
                        xdm_prog_control.c.etl_group,
                        xdm_prog_control.c.etl_comment,
                        xdm_prog_control.c.status,
                        xdm_prog_control.c.message,
                        xdm_prog_control.c.last_start_time,
                        xdm_prog_control.c.last_end_time,
                        xdm_prog_control.c.data_time_flg,
                        func.now().label('update_time')
                    ]
                ).where(
                    and_(
                        xdm_prog_control.c.server == server,
                        xdm_prog_control.c.program == program,
                        xdm_prog_control.c.instance == instance
                    )
                )
            )
            db._connObj.execute(stmt)
        except Exception as ex:  
            is_error=True
            logging.error("Add 'xdm_prog_control_his' fail.")
            logging.error(ex, exc_info=True, stack_info=True)
            if db: logging.debug(stmt)
        finally:
            if xdm_prog_control_his != None: xdm_prog_control_his = None
    else:
        logging.info("First run, initialize db table: 'xdm_prog_control'.")
    
    

    status = 0
    message = "Starting"
    if enable_debug:
        status = 2
        message = "Testing"
        logging.info("Debug mode enable!")
    stmt = select(
            [
                1
            ]
        ).where(
            and_(
                xdm_prog_control.c.server == hostname,
                xdm_prog_control.c.program == program,
                xdm_prog_control.c.instance == instance
                )
        )
    result = db._connObj.execute(stmt)
    
    if result.fetchall():
        stmt = select(
                       [
                           func.count()
                       ]
                ).where(
                    and_(
                        xdm_prog_control.c.server == server,
                        xdm_prog_control.c.program == program,
                        xdm_prog_control.c.instance == instance,
                        xdm_prog_control.c.status == 9
                        )
                )
        count = db._connObj.execute(stmt).fetchall()[0][0]        
        if count == 0:
            stmt = xdm_prog_control.update().where(
                and_(
                    xdm_prog_control.c.server == server,
                    xdm_prog_control.c.program == program,
                    xdm_prog_control.c.instance == instance
                )
            ).values(
                {
                    'etl_group': etl_group,
                    'etl_comment': comment_text,
                    'status': status,
                    'message': message,
                    'last_start_time': start_time,
                    'last_end_time': None
                }
            )
    else:
        stmt = xdm_prog_control.insert().values(
            {
                'server': hostname,
                'program': program,
                'instance': instance,
                'etl_group': etl_group,
                'etl_comment': comment_text,
                'status': status,
                'message': message,
                'last_start_time': start_time
            }
        )
    db._connObj.execute(stmt)
    db.commit()
except Exception as ex:
    is_error=True
    xdm_prog_control_his = None
    xdm_prog_control = None
    
    if meta:
        meta.clear()
        meta = None
    if db:
        logging.debug(stmt)
        db.rollback()
        db.disconnect()
        db = None
    logging.debug(ex, exc_info=True, stack_info=True)
    logging.error("Access XDM Loader control table fail.")
    sys.exit(20)

#===============================================================================
# A0.01
# watchdog: pip install watchdog
# Loader control
#===============================================================================

run_flag = True

class MyHandler(watchdog.events.PatternMatchingEventHandler):
    def on_created(self, event):
        global run_flag
        try:
            os.remove(event.src_path)
        except IOError:
            pass
        if os.path.basename(event.src_path) == program + '.stop':
            run_flag = False
            logging.info("Get stop command! Loader stop next task!")
        elif os.path.basename(event.src_path) == program + '.exit':
            run_flag = False
            logging.info("Get exit command! Loader stop right now!")
            os._exit(99)
    pass
pass

ctrl_path = cwd
try:
    if config.CONTROL_FILE_PATH:
        ctrl_path = config.CONTROL_FILE_PATH
    if not ctrl_path:
        ctrl_path = cwd
except AttributeError:
    pass

patterns = [os.path.join(ctrl_path, program + '.stop'), os.path.join(ctrl_path, program + '.exit')]
observer = watchdog.observers.Observer()
observer.schedule(MyHandler(patterns=patterns, ignore_directories=False, case_sensitive=True), path=cwd, recursive=False)
observer.start()
logging.info("Watching control file: %s", os.path.join(ctrl_path , program + '.*'))
observer.stop()






