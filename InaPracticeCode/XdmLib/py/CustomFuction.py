# -*- coding: UTF-8 -*-
'''
 (c) Copyright 2017, XDMTECH All rights reserved.

 System name: XdmLib
 Source name: XdmLib/CustomFuction.py
 Description: XDM Common Libs package
'''
'''
 Modification history:
 Date        Ver.   Author           Comment
 ----------  -----  ---------------  -----------------------------------------
 2021/01/01  I0.00  Paul Yin         Initial Release
 2021/07/13  M0.01  Jie Dai          Modity datetime log show mode
 2021/07/19  M0.02  Jie Dai          Add file2db_init Function
 2021/07/23  M0.03  Jie Dai          Modify logic_main only run when starttime<endtime
 2021/08/02  M0.04  Jie Dai          Add CheckStrNoneOrEmpty,error_dic
 2021/08/04  A0.05  Jie Dai          Modify function db2db_monthly_init last_update_time to xdm_control_prog, Add function db2db_weekly_init
 2021/08/25  M0.06  Jie Dai          Weekly/Monthly function Standard
 2021/09/27  M0.07  Evelyn Sun       Add get redund index:0 not repeat;1 repeat
 2021/10/18  M1.00  Jie Dai          CustomFuction 2.0
 2021/10/21  M1.01  Vincent          Add Monthly/Weekly increment function
 2021/11/25  M1.02  Vincent          Add function simple_file2db_init
 2021/11/25  M1.03  Jie Dai          Add function mout_sharefolder for file2db_init
 2021/12/02  M1.04  Evelyn Sun       Modify json LastUpdateTime->DataTimeFlg
'''

import datetime
import json
import os
import re
import shutil
import sys

import numpy as np
from dateutil import parser
from dateutil.relativedelta import *

import XdmLib
import XdmLib.Database

sys.path.append('..')


def send_result(flag):
    if type(flag) is bool:
        pass
    else:
        flag = True if flag == 1 else False
    if flag is True:
        print('[XDMResult]:true')
    else:
        print('[XDMResult]:false')


def write_latest_json_single(latest, program_start_time, last_data_time, latest_item):
    latest['DataTimeFlg'] = datetime.datetime.strftime(
        program_start_time, '%Y-%m-%d %H:%M:%S')
    latest[latest_item] = datetime.datetime.strftime(
        last_data_time, '%Y-%m-%d %H:%M:%S')
    if latest[latest_item]:
        file_wjson = open('speciallatest.json', 'w')
        json.dump(latest, file_wjson, indent=4)
        file_wjson.close()


def db2db_his_init_fullUpdate(main_logic, log, config, config_sql):
    run_code = 0
    XdmLib.SetProcessStatus(run_code)
    program = os.path.basename(sys.argv[0])
    # 去除毫秒
    program_start_time = datetime.datetime.strptime(
        datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')
    latest = None

    db_src = None
    db_src_sec = None
    db_tgt = None

    try:
        run = True

        if 'DB_SUPPORT_SRC_AND_TGT_SPLIT' in dir(config):
            if config.DB_SUPPORT_SRC_AND_TGT_SPLIT == False and not is_strip_equals(config.SOURCE_CONNECT_STRING,config.TARGET_CONNECT_STRING):
                run = False
        if run:
            if not db_src:
                db_src = XdmLib.Database.DbClient(config.SOURCE_CONNECT_STRING, logging_obj=log,
                                                  enable_debug=config.DEBUG, encoding="utf8")
            db_src.begin()
            if not db_tgt:
                db_tgt = XdmLib.Database.DbClient(config.TARGET_CONNECT_STRING, logging_obj=log,
                                                  enable_debug=config.DEBUG, encoding="utf8")
            db_tgt.begin()
            if 'MUTABLE_PARAMETER_FLAG' in dir(config) and config.MUTABLE_PARAMETER_FLAG:
                db_src_sec = None
                if "SOURCE_CONNECT_STRING_SEC" in dir(config):
                    db_src_sec = XdmLib.Database.DbClient(config.SOURCE_CONNECT_STRING_SEC, logging_obj=log,
                                                          enable_debug=config.DEBUG, encoding="utf8")
                    db_src_sec.begin()
                    main_logic(db_src, db_tgt, db_src_sec=db_src_sec)
            else:
                main_logic(db_src, db_tgt)
            if db_tgt:
                db_tgt.commit()
                log.info("commit data")
                if latest:
                    write_latest_json(latest, program_start_time, '')
                run_code = 1
                send_result(run_code)

        else:
            log.error("DBConnection Error: 本ETL不支持源表目标表分数据库！")
            run_code = 5
            send_result(run_code)

    except Exception as ex:
        log.error(ex, exc_info=True, stack_info=True)
        if db_src:
            log.debug(db_src.getstatement())

        if db_src_sec:
            log.debug(db_src_sec.getstatement())

        if db_tgt:
            log.debug(db_tgt.getstatement())
            db_tgt.rollback()

        XdmLib.WriteToErrorLog('E001', program, '程序运行错误,错误见明细', str(ex), True)
        run_code = 5
        send_result(run_code)
    finally:
        if db_src:
            db_src.disconnect()
            db_src = None
        del db_src

        if db_src_sec:
            db_src_sec.disconnect()
            db_src_sec = None
        del db_src_sec

        if db_tgt:
            db_tgt.disconnect()
            db_tgt = None
        del db_tgt

    XdmLib.SetProcessStatus(run_code)
    log.info("========== Finally ==========")


def db2db_his_init(main_logic, log, config, config_sql):
    run_code = 0
    XdmLib.SetProcessStatus(run_code)
    program = os.path.basename(sys.argv[0])
    # 去除毫秒
    program_start_time = datetime.datetime.strptime(
        datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')
    max_query_day = datetime.timedelta(days=config.MAX_QUERY_DAY)
    time_frequency = datetime.timedelta(minutes=int(config.TIME_FREQUENCY))
    tolerance_minute = datetime.timedelta(minutes=int(config.TOLERANCE_MINUTE))
    latest = None

    db_src = None
    db_tgt = None

    query_start_time = None
    query_end_time = None

    skip_minutes = 0
    skip_start_time = None
    skip_end_time = None

    main_logic_begin = False  # 标记是否执行到main_logic

    try:
        run = True

        if 'DB_SUPPORT_SRC_AND_TGT_SPLIT' in dir(config):
            if config.DB_SUPPORT_SRC_AND_TGT_SPLIT == False and not is_strip_equals(config.SOURCE_CONNECT_STRING,
                                                                                    config.TARGET_CONNECT_STRING):
                run = False

        if run:
            last_data_time_json = None  # Json中获取的上次处理的数据结束时间点（非程序执行时间）
            last_data_time_db = None  # db中获取的上次处理的数据结束时间点（非程序执行时间）

            query_end_time_json = None  # Json模式时，可指定本次处理的数据结束时间点

            # 读取Json文件
            latest = json.load(open('latest.json'))
            try:
                # 为避免时间区间临界点毫秒影响，现舍弃毫秒位
                if len(latest['DataTimeFlg']) > 0:
                    last_data_time_json = datetime.datetime.strptime(
                        latest['DataTimeFlg'][0:19] if len(latest['DataTimeFlg']) > 19 else latest[
                            'DataTimeFlg'], "%Y-%m-%d %H:%M:%S")

                if len(latest['QueryEndTime']) > 0:
                    query_end_time_json = datetime.datetime.strptime(
                        latest['QueryEndTime'][0:19] if len(
                            latest['QueryEndTime']) > 19 else latest['QueryEndTime'],
                        "%Y-%m-%d %H:%M:%S")
            except Exception as exparsetime:
                XdmLib.WriteToErrorLog('E001', program, 'Read latest.json Error',
                                       "Parsing TimeType in latest.json Error: {0}".format(exparsetime), True)

            # 读取xdm_prog_control
            last_data_time_db = XdmLib.last_data_time
            if last_data_time_db is None:
                # 从xdm_prog_control中读不到上次处理的数据结束时间点（程序第一次运行），就用Json时间代替
                # 但还是理解为db中获取的时间点
                last_data_time_db = last_data_time_json
            try:
                # 去除毫秒
                last_data_time_db = datetime.datetime.strptime(
                    datetime.datetime.strftime(last_data_time_db, '%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')
            except Exception as exparsetime:
                XdmLib.WriteToErrorLog('E001', program, 'Read data_time_flg Error',
                                       "Can't correctly get last_data_time_db, Error: {0}".format(exparsetime), True)

            # 判断db模式，json模式
            if config.IS_USING_LATESTJSON is False:
                query_start_time = last_data_time_db
            else:
                query_start_time = last_data_time_json

            # 连接数据库
            if not db_src:
                db_src = XdmLib.Database.DbClient(config.SOURCE_CONNECT_STRING, logging_obj=log,
                                                  enable_debug=config.DEBUG, encoding="utf8")
            db_src.begin()
            if not db_tgt:
                db_tgt = XdmLib.Database.DbClient(config.TARGET_CONNECT_STRING, logging_obj=log,
                                                  enable_debug=config.DEBUG, encoding="utf8")
            db_tgt.begin()

            # 计算query时间区间
            max_data_time_db = db_src.execute(
                config_sql.SQL_GET_MAX_RUN_CONTEXT_TIME).fetchall()

            max_data_time_db = max_data_time_db[0][0]
            if type(max_data_time_db) == datetime.datetime:
                # 去除毫秒
                max_data_time_db = datetime.datetime.strptime(
                    datetime.datetime.strftime(max_data_time_db, '%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')
            elif type(max_data_time_db) == str:
                max_data_time_db = parser.parse(max_data_time_db)
                # 去除毫秒
                max_data_time_db = datetime.datetime.strptime(
                    datetime.datetime.strftime(max_data_time_db, '%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')
            else:
                max_data_time_db = None

            # 计算query_end_time前，判断是否已经从源表正确获取到最大时间，判断是否已经取到query_start_time
            if query_start_time is not None and max_data_time_db is not None:

                query_end_time = max_data_time_db  # 源表最大时间暂时作为query_end_time

                # 计算query_end_time
                if time_frequency > program_start_time - query_end_time:
                    query_end_time = program_start_time - time_frequency

                # 捞取数据时，时间临界点的数据，可能未及时塞入数据库，
                # 所以用config.TOLERANCE_MINUTE无脑多捞段时间；
                # query_end_time和query_start_time才是程序通过逻辑计算出来的query time
                # query_end_time > query_start_time时，main_logic才有执行意义
                if query_end_time > query_start_time:
                    if query_end_time - query_start_time > max_query_day:
                        query_end_time = query_start_time + max_query_day

                    # Json模式下才可绕过config.MAX_QUERY_DAY，直接设定query_end_time
                    # Json模式下QueryEndTime空值或设定格式错误，也依然绕不开config.MAX_QUERY_DAY
                    if config.IS_USING_LATESTJSON is True and type(query_end_time_json) == datetime.datetime:
                        query_end_time = query_end_time_json

                    main_logic_begin = True  # 经过时间判断，能执行到main_logic
                    main_logic(db_src, db_tgt, query_start_time -
                               tolerance_minute, query_end_time)

                elif query_end_time == query_start_time:  # 警告，不执行main_logic
                    run_code = 3
                    log.warning('query_end_time = query_start_time: actual query_end_time = {0}, current query_start_time = {1}'.format(
                            query_end_time, query_start_time))
                else:  # 报错，不执行main_logic
                    XdmLib.WriteToErrorLog('T001', program, 'Parse query time Error',
                                           'query_end_time < query_start_time: actual query_end_time = {0}, current query_start_time = {1}'.format(
                                               query_end_time, query_start_time), True)
            else:  # 报错,不执行main_logic
                XdmLib.WriteToErrorLog('T001', program, 'Parse query time Error',
                                       "Can't get max_data_time_db for operating query_end_time, program fail! Please check config_sql.SQL_GET_MAX_RUN_CONTEXT_TIME.\r\n    Or    Can't get query_start_time, program fail! Please check latest.json[DataTimeFlg] or check xdm_prog_control[data_time_flg].",
                                       True)

            # 能执行到main_logic，说明query time没有异常
            # main_logic中的错误会被try捕获到,不会触发commit及记录query_end_time
            if main_logic_begin is True:
                if db_tgt:
                    db_tgt.commit()
                    log.info("commit data")

                    XdmLib.last_data_time = query_end_time
                    if latest:
                        write_latest_json(
                            latest, program_start_time, query_end_time)
                    run_code = 1
                    send_result(run_code)
            else:
                # 计算query time并没有db操作,所以query time有异常,并不需要db.rollback()
                # 也不需要跳时间
                # 单纯记录并显示错误
                run_code = 5 if run_code != 3 else run_code
                send_result(run_code)

        else:
            log.error("DBConnection Error: 本ETL不支持源表目标表分数据库！")
            run_code = 5
            send_result(run_code)
    except Exception as ex:
        log.error(ex, exc_info=True, stack_info=True)
        if db_src:
            log.debug(db_src.getstatement())

        if db_tgt:
            log.debug(db_tgt.getstatement())
            db_tgt.rollback()
        try:
            if 'SKIP_MINUTE' in dir(config):
                skip_minutes = config.SKIP_MINUTE
                skip_start_time = query_start_time + \
                    datetime.timedelta(minutes=skip_minutes)
                XdmLib.last_data_time = skip_start_time
                XdmLib.WriteToErrorLog('E001', program, 'Error Time: From {0} To {1},Next Execute: From {2} '.format(
                    query_start_time,
                    query_end_time,
                    skip_start_time), str(ex), True)
                pass
            XdmLib.last_data_time = skip_end_time
            if latest:
                write_latest_json(latest, program_start_time, skip_start_time)
        except Exception as ex:
            pass
        run_code = 5
        send_result(run_code)
    finally:
        if db_src:
            db_src.disconnect()
            db_src = None
        del db_src

        if db_tgt:
            db_tgt.disconnect()
            db_tgt = None
        del db_tgt

        XdmLib.SetProcessStatus(run_code)
        log.info("========== Finally ==========")


def write_latest_json(latest, program_start_time, last_data_time):
    latest['DataTimeFlg'] = datetime.datetime.strftime(
        program_start_time, '%Y-%m-%d %H:%M:%S')
    latest['DataTimeFlg'] = datetime.datetime.strftime(
        last_data_time, '%Y-%m-%d %H:%M:%S')
    latest['QueryEndTime'] = ''
    if latest['DataTimeFlg']:
        file_wjson = open('latest.json', 'w')
        json.dump(latest, file_wjson, indent=4)
        file_wjson.close()


def check_configTime(string):
    return True if (re.match("^(([0-1]\d)|(2[0-4])):[0-5]\d:[0-5]\d$", string)) is not None else False

# 获取Monthly, Weekly配置


def getConfigTime(db_src, sql):
    '''
    sql: item_value Output 'MONTH','WEEK','TIME'
    '''
    monthlyDay = None
    weeklyDay = None
    configTime = None

    dicWeekDay = {"MON": 0, "TUE": 1, "WED": 2,
                  "THU": 3, "FRI": 4, "SAT": 5, "SUN": 6}
    sqlResult = db_src.execute(sql).fetchall()

    for i in sqlResult:
        if i[0] == 'MONTH':
            monthlyDay = i[1]
        elif i[0] == 'TIME':
            configTime = i[1]
        elif i[0] == 'DAY':
            weeklyDay = i[1]

    if weeklyDay is not None and weeklyDay.upper() in dicWeekDay.keys():
        weeklyDay = dicWeekDay[weeklyDay.upper()]
    if configTime is not None:
        configTime = datetime.datetime.strftime(
            datetime.datetime.strptime(configTime, "%H:%M:%S"), "%H:%M:%S")
    else:
        raise Exception("MFG Time definition ERROR")

    return monthlyDay, weeklyDay, configTime


# 判断来源表连接字符串是否等于目标表连接字符串
def is_strip_equals(str1, str2) -> bool:
    if str1.strip() == str2.strip():
        return True
    else:
        return False


def convert_from_nmpy(value):
    if value is not None:
        return str(value)
    else:
        return None


def get_redund_data(list):
    count_map = {}
    error_data = []
    for insdis in list:
        count_map[insdis] = count_map.get(insdis, 0) + 1
        if count_map[insdis] > 1:
            error_data.append(insdis)
    set_error_data = set(error_data)
    return set_error_data


def get_redund_index(uk_list):  # 0:not repeat;1:repeat
    count_map = {}
    return_data = {}
    for insdis in uk_list:
        count_map[insdis] = count_map.get(insdis, 0) + 1
        if count_map[insdis] > 1:
            return_data[insdis] = 1
        else:
            return_data[insdis] = 0
    return return_data


# 字典的集合转集合的集合
def dictionaryToValueList(oldList: "[{},{}]") -> "[[],[]]":
    newlist_Sum = []
    for item in oldList:
        newlist = []
        for item1 in item:
            i = item[item1]
            newlist.append(i)
        newlist_Sum.append(newlist)
    return newlist_Sum


# 元组的集合转集合的集合
def TupleToList(tplresult: "[(),()]") -> "[[],[]]":
    lstresult = tplresult
    for i in range(0, len(lstresult)):
        lstresult[i] = list(lstresult[i])
    return lstresult


# 数值是否空值，返回None
def PostProcessNumeric(dval):
    if np.isnan(dval):
        return None
    else:
        return dval


# 判断字符串是否为空或None
def CheckStrNoneOrEmpty(text: str) -> bool:
    if text is None or text.strip() == '':
        return True
    else:
        return False


def mount_sharefolder(log, config):
    mount_disk_good = True
    if not os.path.isdir(config.TO_LOCAL_DISK_NAME):
        cmd_str = 'net use' + ' ' + config.TO_LOCAL_DISK_NAME + ' ' + \
            config.FOLDER_PATH + ' ' + r'/user:' + config.ACCOUNT + ' ' + config.PASSWORD
        print(cmd_str)
        mount_result = os.system(cmd_str)
        if mount_result == 0:
            log.info('mount {0} success.'.format(config.TO_LOCAL_DISK_NAME))
        else:
            mount_disk_good = False
            log.error('mount {0} fail.'.format(config.TO_LOCAL_DISK_NAME))
    else:
        log.info('{0} is already mounted.'.format(config.TO_LOCAL_DISK_NAME))
    return mount_disk_good


def file2db_init(do_file, before_do_file, after_do_file, skip_file, move_file_self, log, config, config_sql):
    running = True
    run_code = 0
    if config.IS_NEED_MOUNT is True:
        if mount_sharefolder(log, config) is False:
            running = False
    if running is True:
        XdmLib.SetProcessStatus(run_code)
        db_src = None
        db_tgt = None
        db_log = None
        config_source = config.SOURCE_FOLDER
        config_skip = config.SKIP_FOLDER
        config_archive = config.ARCHIVE_FOLDER
        config_error = config.ERROR_FOLDER

        program = os.path.basename(sys.argv[0])
        error_dic = {}
        analysis_res = []
        # Task*
        try:

            if not db_src:
                db_src = XdmLib.Database.DbClient(config.SOURCE_CONNECT_STRING, logging_obj=log, enable_debug=config.DEBUG,
                                                  encoding="utf8")
            db_src.begin()

            # Get target db connection
            if not db_tgt:
                db_tgt = XdmLib.Database.DbClient(config.TARGET_CONNECT_STRING, logging_obj=log, enable_debug=config.DEBUG,
                                                  encoding="utf8")
            db_tgt.begin()

            if not db_log:
                db_log = XdmLib.Database.DbClient(
                    config.CTRL_TABLE_CONN, logging_obj=log, enable_debug=config.DEBUG, encoding="utf-8")
            db_log.begin()

            pre_params = before_do_file()

            if move_file_self is None:
                move_file_self = move_file
            for response in os.walk(config_source):
                # Current Path
                file_root_source_path = response[0]
                # Current Path folder list
                folder_list = response[1]
                # Current Path file list
                file_list = response[2]

                # Create Path
                file_root_target_path = config_archive
                # file_root_source_path.replace(config_source, config_archive)
                file_root_skip_path = config_skip
                # file_root_source_path.replace(config_source, config_skip)
                file_root_error_path = config_error
                # file_root_source_path.replace(config_source, config_error)

                if len(file_list) > 0:
                    for file in file_list:
                        move_file_info = {
                            'type': 'Type',
                            'source_org': config_source,
                            'source_path': file_root_source_path,
                            'target_path': file_root_target_path,
                            'file': file,
                            'log': log,
                            'config': config,
                            'detail': None,
                            'other_info': None
                        }

                        # Skip file by file name or folder path
                        if skip_file(move_file_info):
                            move_file_info['type'] = 'SKIP'
                            move_file_self(move_file_info)
                        # Moving large file to Pending folder
                        elif pass_large_file(file_root_source_path, file, config, log):
                            config_pending = config.PENDING_FOLDER
                            move_file_info['type'] = 'Pending'
                            move_file_info['target_path'] = file_root_source_path.replace(
                                config_source, config_pending)
                            move_file_self(move_file_info)
                        else:
                            # Do File
                            info = {
                                'log': log,
                                'db': db_log,
                                'config': config,
                                'file_info': {
                                    'file_name': file,
                                    'file_path': file_root_source_path,
                                    'start_time': datetime.datetime.now()
                                }
                            }
                            res = None
                            try:
                                res = do_file(file_root_source_path,
                                              file, pre_params, db_src, db_tgt)
                                analysis_res.append(res['data'])
                                move_file_info['detail'] = res.get(
                                    'detail', None)
                                move_file_info['other_info'] = res.get(
                                    'other_info', None)
                                # For DB Commot or rollback
                                if res['status'] == 'archive':
                                    db_tgt.commit()
                                else:
                                    db_tgt.rollback()
                                # For move file
                                if res['status'] == 'archive':
                                    move_file_info['type'] = 'Archive'
                                    move_file_info['target_path'] = file_root_target_path
                                elif res['status'] == 'error':
                                    move_file_info['type'] = 'ERROR'
                                    move_file_info['target_path'] = file_root_error_path
                                    XdmLib.WriteToErrorLog('E001', program, 'File:{0} Move To Error Folder'.format(
                                        file), str(res['data']), True)
                                elif res['status'] == 'skip':
                                    move_file_info['type'] = 'SKIP'
                                    move_file_info['target_path'] = file_root_skip_path
                                elif res['status'] == 'pass':
                                    # This file is just by pass
                                    continue
                                else:
                                    move_file_info['type'] = 'ERROR'
                                    move_file_info['target_path'] = file_root_error_path
                                    move_file_info['detail'] = 'Unknow Status'
                                    XdmLib.WriteToErrorLog('0001', program, 'File:{0} Move To Error Folder'.format(
                                        file), str('Unknow Status'), True)
                                move_file_self(move_file_info)
                            except Exception as ex:
                                log.error(ex, exc_info=True, stack_info=True)
                                if file not in error_dic.keys():
                                    error_dic[file] = ex
                                move_file_info = {
                                    'type': 'ERROR',
                                    'source_org': config_source,
                                    'source_path': file_root_source_path,
                                    'target_path': file_root_error_path,
                                    'file': file,
                                    'log': log,
                                    'config': config,
                                    'detail': None,
                                    'other_info': None
                                }
                                db_tgt.rollback()
                                move_file_self(move_file_info)
                                XdmLib.WriteToErrorLog(
                                    'E001', program, 'File:{0} Move To Error Folder'.format(file), str(ex), False)
                            finally:
                                if ('ETL_LOG_RECORD' in dir(config) and config.ETL_LOG_RECORD):
                                    info['result'] = res
                                    record_etl_status(info)
                            pass
                        pass
                    pass
                elif len(folder_list) == 0:
                    log.info("[Empty] Empty Folder : " + file_root_source_path)
                    pass
            log.info("========[ File End ]========")
            # ===== Delete Empty Folder ===== #
            log.info("========[ Delete Empty Folder ]========")
            delete_empty_folder(config_source, config_source, log)

            after_do_file(analysis_res=analysis_res)

        except Exception as ex:
            log.error(ex, exc_info=True, stack_info=True)
            run_code = 5
            send_result(run_code)
            if db_src:
                log.debug(db_src.getstatement())
            if db_tgt:
                log.debug(db_tgt.getstatement())
                db_tgt.rollback()

            XdmLib.WriteToErrorLog('E001', program, 'File Error',
                                   str(ex), False)

        finally:
            
            if len(error_dic.keys()) > 0:
                run_code = 5
                send_result(run_code)
            else:
                run_code = 1
                send_result(run_code)
            if db_src:
                db_src.disconnect()
                db_src = None
            del db_src
            if db_tgt:
                db_tgt.disconnect()
                db_tgt = None
            del db_tgt
        log.info("========== Finally ==========")
        XdmLib.SetProcessStatus(run_code)
    else:
        run_code = 5
        send_result(run_code)


def batch_file2db_init(do_file, before_do_file, after_do_file, skip_file, move_file_self, log, config, config_sql):
    running = True
    run_code = 0
    send_result(run_code)
    if config.IS_NEED_MOUNT is True:
        if mount_sharefolder(log, config) is False:
            running = False
    if running is True:
        XdmLib.SetProcessStatus(run_code)
        db_src = None
        db_tgt = None
        db_log = None
        config_source = config.SOURCE_FOLDER
        config_skip = config.SKIP_FOLDER
        config_archive = config.ARCHIVE_FOLDER
        config_error = config.ERROR_FOLDER

        program = os.path.basename(sys.argv[0])
        error_dic = {}
        analysis_res = []
        # Task*
        try:

            if not db_src:
                db_src = XdmLib.Database.DbClient(config.SOURCE_CONNECT_STRING, logging_obj=log, enable_debug=config.DEBUG,
                                                  encoding="utf8")
            db_src.begin()

            # Get target db connection
            if not db_tgt:
                db_tgt = XdmLib.Database.DbClient(config.TARGET_CONNECT_STRING, logging_obj=log, enable_debug=config.DEBUG,
                                                  encoding="utf8")
            db_tgt.begin()

            if not db_log:
                db_log = XdmLib.Database.DbClient(
                    config.CTRL_TABLE_CONN, logging_obj=log, enable_debug=config.DEBUG, encoding="utf-8")
            db_log.begin()

            pre_params = before_do_file()

            if move_file_self is None:
                move_file_self = batch_move_file
            for response in os.walk(config_source):
                # Current Path
                file_root_source_path = response[0]
                # Current Path folder list
                folder_list = response[1]
                # Current Path file list
                file_list = response[2]

                # Create Path
                file_root_target_path = config_archive
                # file_root_source_path.replace(config_source, config_archive)
                file_root_skip_path = config_skip
                # file_root_source_path.replace(config_source, config_skip)
                file_root_error_path = config_error
                # file_root_source_path.replace(config_source, config_error)

                if len(file_list) > 0:
                    for file in file_list:
                        # for batch move file
                        batch_info = {}
                        if ('FILE_EXT' in dir(config)):
                            batch_info['ext'] = config.FILE_EXT
                        if ('FILE_PREFIX' in dir(config)):
                            batch_info['prefix'] = config.FILE_PREFIX
                        if ('FILE_SUFFIX' in dir(config)):
                            batch_info['suffix'] = config.FILE_SUFFIX

                        move_file_info = {
                            'type': 'Type',
                            'source_org': config_source,
                            'source_path': file_root_source_path,
                            'target_path': file_root_target_path,
                            'file': file,
                            'log': log,
                            'config': config,
                            'detail': None,
                            'batch_info': batch_info,
                            'other_info': None
                        }

                        # Skip file by file name or folder path
                        if skip_file(move_file_info):
                            move_file_info['type'] = 'SKIP'
                            move_file_info['target_path'] = file_root_skip_path
                            move_file_self(move_file_info)
                        # Moving large file to Pending folder
                        elif ('BATCH_LARGE_EXT' in dir(config)) and (file.split('.')[-1].upper() in [x.upper() for x in config.BATCH_LARGE_EXT]) and (pass_large_file(file_root_source_path, file, config, log)):
                            config_pending = config.PENDING_FOLDER
                            move_file_info['type'] = 'Pending'
                            move_file_info['target_path'] = file_root_source_path.replace(
                                config_source, config_pending)
                            move_file_self(move_file_info)
                            pass
                        # Moving large file to Pending folder
                        elif pass_large_file(file_root_source_path, file, config, log):
                            config_pending = config.PENDING_FOLDER
                            move_file_info['type'] = 'Pending'
                            move_file_info['target_path'] = file_root_source_path.replace(
                                config_source, config_pending)
                            move_file_self(move_file_info)
                        else:
                            # Do File
                            info = {
                                'log': log,
                                'db': db_log,
                                'config': config,
                                'file_info': {
                                    'file_name': file,
                                    'file_path': file_root_source_path,
                                    'start_time': datetime.datetime.now()
                                }
                            }
                            res = None
                            try:
                                res = do_file(file_root_source_path,
                                              file, pre_params, db_src, db_tgt)
                                analysis_res.append(res['data'])
                                batch_info = {
                                    'name': res.get('name', None),
                                    'ext': res.get('ext', None),
                                    'prefix': res.get('prefix', ['']),
                                    'suffix': res.get('suffix', [''])
                                }
                                move_file_info['batch_info'] = batch_info
                                move_file_info['detail'] = res.get(
                                    'detail', None)
                                move_file_info['other_info'] = res.get(
                                    'other_info', None)

                                # For DB Commot or rollback
                                if res['status'] == 'archive':
                                    db_tgt.commit()
                                else:
                                    db_tgt.rollback()
                                # For move file
                                if res['status'] == 'archive':
                                    move_file_info['type'] = 'Archive'
                                    move_file_info['target_path'] = file_root_target_path
                                elif res['status'] == 'error':
                                    move_file_info['type'] = 'ERROR'
                                    move_file_info['target_path'] = file_root_error_path
                                    XdmLib.WriteToErrorLog('E001', program, 'File:{0} Move To Error Folder'.format(
                                        file), str(res['data']), True)
                                elif res['status'] == 'skip':
                                    move_file_info['type'] = 'SKIP'
                                    move_file_info['target_path'] = file_root_skip_path
                                elif res['status'] == 'pass':
                                    # This file is just by pass
                                    continue
                                else:
                                    detail = 'Unknow Status'
                                    move_file_info['type'] = 'ERROR'
                                    move_file_info['target_path'] = file_root_error_path
                                    move_file_info['detail'] = detail
                                    XdmLib.WriteToErrorLog('0001', program, 'File:{0} Move To Error Folder'.format(
                                        file), str('Unknow Status'), True)
                                move_file_self(move_file_info)
                            except Exception as ex:
                                log.error(ex, exc_info=True, stack_info=True)
                                if file not in error_dic.keys():
                                    error_dic[file] = ex
                                batch_info = {}
                                if ('FILE_EXT' in dir(config)):
                                    batch_info['ext'] = config.FILE_EXT
                                if ('FILE_PREFIX' in dir(config)):
                                    batch_info['prefix'] = config.FILE_PREFIX
                                if ('FILE_SUFFIX' in dir(config)):
                                    batch_info['suffix'] = config.FILE_SUFFIX
                                move_file_info = {
                                    'type': 'ERROR',
                                    'source_org': config_source,
                                    'source_path': file_root_source_path,
                                    'target_path': file_root_error_path,
                                    'file': file,
                                    'log': log,
                                    'config': config,
                                    'detail': None,
                                    'batch_info': batch_info,
                                    'other_info': None
                                }
                                move_file_self(move_file_info)
                                db_tgt.rollback()
                                XdmLib.WriteToErrorLog(
                                    'E001', program, 'File:{0} Move To Error Folder'.format(file), str(ex), False)
                            finally:
                                if ('ETL_LOG_RECORD' in dir(config) and config.ETL_LOG_RECORD):
                                    info['result'] = res
                                    record_etl_status(info)
                            pass
                        pass
                    pass
                elif len(folder_list) == 0:
                    log.info("[Empty] Empty Folder : " + file_root_source_path)
                    pass
            log.info("========[ File End ]========")
            # ===== Delete Empty Folder ===== #
            log.info("========[ Delete Empty Folder ]========")
            delete_empty_folder(config_source, config_source, log)

            after_do_file(analysis_res=analysis_res)

        except Exception as ex:
            log.error(ex, exc_info=True, stack_info=True)
            run_code = 5
            send_result(run_code)
            if db_src:
                log.debug(db_src.getstatement())
            if db_tgt:
                log.debug(db_tgt.getstatement())
                db_tgt.rollback()

            XdmLib.WriteToErrorLog('E001', program, 'File Error',
                                   str(ex), False)

        finally:
            if len(error_dic.keys()) > 0:
                run_code = 5
                send_result(run_code)
            else:
                run_code = 1
                send_result(run_code)
            if db_src:
                db_src.disconnect()
                db_src = None
            del db_src

            if db_tgt:
                db_tgt.disconnect()
                db_tgt = None
            del db_tgt

            if db_log:
                db_log.disconnect()
                db_log = None
            del db_log

        log.info("========== Finally ==========")
        XdmLib.SetProcessStatus(run_code)
    else:
        run_code = 5
        send_result(run_code)


def simple_file2db_init(do_file, log, config, config_sql):
    run_code = 0
    XdmLib.SetProcessStatus(run_code)
    db_src = None
    db_tgt = None
    program = os.path.basename(sys.argv[0])
    try:
        if not db_src:
            db_src = XdmLib.Database.DbClient(config.SOURCE_CONNECT_STRING, logging_obj=log, enable_debug=config.DEBUG,
                                              encoding="utf8")
        db_src.begin()

        # Get target db connection
        if not db_tgt:
            db_tgt = XdmLib.Database.DbClient(config.TARGET_CONNECT_STRING, logging_obj=log, enable_debug=config.DEBUG,
                                              encoding="utf8")
        db_tgt.begin()
        res = do_file(db_src, db_tgt)
    except Exception as ex:
        log.error(ex, exc_info=True, stack_info=True)
        run_code = 5
        send_result(run_code)
        if db_src:
            log.debug(db_src.getstatement())
        if db_tgt:
            log.debug(db_tgt.getstatement())
            db_tgt.rollback()

        XdmLib.WriteToErrorLog('E001', program, 'File Error',
                               str(ex), False)
    finally:
        if res is False:
            run_code = 5
            send_result(run_code)
        else:
            run_code = 1
            send_result(run_code)

        if db_src:
            db_src.disconnect()
            db_src = None
        del db_src

        if db_tgt:
            db_tgt.disconnect()
            db_tgt = None
        del db_tgt

    log.info("========== Finally ==========")
    XdmLib.SetProcessStatus(run_code)


# ===== Delete Empty Folder ===== #
def delete_empty_folder(org, path, log):
    if os.path.isdir(path):
        for i in os.listdir(path):
            path_full = os.path.join(path, i)
            if (os.path.isdir(path_full) == True):
                delete_empty_folder(org, path_full, log)
    if (not os.listdir(path)) & (org != path):
        path = os.path.normpath(path)
        os.rmdir(path)
        log.info("[Delete] Delete Empty Folder : " + path)

# ===== Default Move File ===== #


def move_file(move_file_info):
    try:
        move_type = move_file_info.get('type', None)
        source_path = move_file_info.get('source_path', None)
        target_path = move_file_info.get('target_path', None)
        fileName = move_file_info.get('file', None)
        log = move_file_info.get('log', None)

        # If You want to modify by yourself,you can return key with "other_info" in main function.
        # It would be here to get what you return.
        # other_info = move_file_info.get('other_info',None)

        target_path = classification_target_path(move_file_info, target_path)

        log.info("[%s] %s", move_type, fileName)

        # New folder
        if not os.path.isdir(target_path):
            os.makedirs(target_path, mode=0o777)
        # Move file to folder
        msg = shutil.move(os.path.join(source_path, fileName),
                          os.path.join(target_path, fileName))
        if target_path:
            log.info("Move file: %s to %s folder: %s.",
                     fileName, move_type, msg)
        else:
            log.info("Delete file: %s.", fileName)
    except Exception as ex:
        log.error('Move File Error')
        log.error(ex, exc_info=True, stack_info=True)

# ===== Default Batch Move File ===== #


def batch_move_file(move_file_info):
    try:
        move_type = move_file_info.get('type', None)
        source_path = move_file_info.get('source_path', None)
        target_path = move_file_info.get('target_path', None)
        fileName = move_file_info.get('file', None)
        log = move_file_info.get('log', None)
        batch_info = move_file_info.get('batch_info', {})

        # If You want to modify by yourself,you can return key with "other_info" in main function.
        # It would be here to get what you return.
        # other_info = move_file_info.get('other_info',None)

        target_path = classification_target_path(move_file_info, target_path)

        log.info("[%s]", move_type)

        # New folder
        if not os.path.isdir(target_path):
            os.makedirs(target_path, mode=0o777)

        # Move file to folder
        if len(batch_info) == 0:
            msg = shutil.move(os.path.join(source_path, fileName),
                              os.path.join(target_path, fileName))
            if target_path:
                log.info("Move file: %s to %s folder: %s.",
                         fileName, move_type, msg)
            else:
                log.info("Delete file: %s.", fileName)
        else:
            name = batch_info.get('name', None)
            ext = batch_info.get('ext', None)
            prefix = batch_info.get('prefix', [''])
            suffix = batch_info.get('suffix', [''])
            if name is None:
                # if no basic file name
                name = '.'.join(fileName.split('.')[:-1])
                if len(prefix) > 0:
                    for p in prefix:
                        if name.upper().startswith(p.upper()):
                            name = name[len(p):]
                            break
                if len(suffix) > 0:
                    for s in suffix:
                        if name.upper().endswith(s.upper()):
                            name = name[:-len(s)]
                            break
            if ext is None:
                # if no ext
                ext = [fileName.split('.')[-1]]
                pass
            for p in prefix:
                for s in suffix:
                    for e in ext:
                        f = p + name + s + '.' + e
                        # check for file is exist
                        if os.path.isfile(os.path.join(source_path, f)):
                            msg = shutil.move(os.path.join(
                                source_path, f), os.path.join(target_path, f))
                            if target_path:
                                log.info(
                                    "Move file: %s to %s folder: %s.", f, move_type, msg)
                            else:
                                log.info("Delete file: %s.", f)
    except Exception as ex:
        log.error('Batch Move File Error')
        log.error(ex, exc_info=True, stack_info=True)

# ===== Passing Large File ===== #


def pass_large_file(file_root_source_path, file, config, log):
    result = False
    unit_list = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB']
    try:
        if ('LARGE_FILE' in dir(config)) & ('PENDING_FOLDER' in dir(config)):
            if type(config.LARGE_FILE) is not list:
                log.error('LARGE_FILE setting is illegal.')
            elif len(config.LARGE_FILE) != 3:
                log.error('LARGE_FILE setting is illegal.')
            else:
                if type(config.LARGE_FILE[0]) != int and type(config.LARGE_FILE[0]) != float:
                    log.error('Size must be a Number.')
                elif config.LARGE_FILE[0] <= 0:
                    log.error('Size must bigger than 0.')
                elif type(config.LARGE_FILE[1]) != str:
                    log.error('Unit must be a String.')
                elif config.LARGE_FILE[1].upper() not in unit_list:
                    log.error(
                        'Unit is illegal.\nLegal units : {0}'.format(unit_list))
                else:
                    index = unit_list.index(config.LARGE_FILE[1])
                    file_path = os.path.join(file_root_source_path, file)
                    if os.path.isfile(file_path):
                        file_size = os.path.getsize(file_path)
                        if file_size >= 2**(index*10)*config.LARGE_FILE[0]:
                            if config.LARGE_FILE[2] == 'Warning':
                                log.warning('File size is bigger than {0} {1}'.format(
                                    config.LARGE_FILE[0], config.LARGE_FILE[1]))
                            elif config.LARGE_FILE[2] == 'Error':
                                log.error('File size is bigger than {0} {1}'.format(
                                    config.LARGE_FILE[0], config.LARGE_FILE[1]))
                            else:
                                log.error('File size is bigger than {0} {1}'.format(
                                    config.LARGE_FILE[0], config.LARGE_FILE[1]))
                            result = True
    except Exception as ex:
        result = False
        log.error(ex, exc_info=True, stack_info=True)
    finally:
        return result

# ===== Get Folder List ===== #


def get_folder_list(path):
    result = []
    try:
        import platform
        p = os.path.abspath(path)
        if platform.system() == 'Windows':
            result = p.split('\\')
        else:
            result = p.split('/')
    except Exception as ex:
        result = []
    finally:
        result.reverse()
        return result

# ===== Classification Target Path ===== #


def classification_target_path(move_file_info, target_path):

    log = move_file_info.get('log', None)
    config_setting = move_file_info.get('config', None)
    result = target_path

    # If want to classification folder according config
    if config_setting is not None:
        # If want to classification folder by time
        if 'TIME_FOLDER' in dir(config_setting):
            time_folder = config_setting.TIME_FOLDER
            try:
                now = datetime.datetime.now()
                time_string = now.strftime(time_folder)
                result = os.path.join(result, time_string)
            except Exception as ex:
                log.info("Time format error : {0}".format(time_folder))
        # If want to classification something else
        ## ===== ##

    # If want to classification reason by folder
    detail = move_file_info.get('detail', None)
    if detail is not None:
        try:
            result = os.path.join(result, detail)
        except Exception as ex:
            print('Detail setting is failed.')

    source_org = move_file_info.get('source_org', None)
    source_path = move_file_info.get('source_path', None)
    result = source_path.replace(source_org, result)
    result = os.path.normpath(result)
    return result

# ===== Record ETL Status===== #


def record_etl_status(info):
    log = info.get('log', None)
    config = info.get('config', None)
    db = info.get('db', None)
    result = info.get('result', None)
    file_info = info.get('file_info', None)
    try:
        from sqlalchemy import Table, MetaData, func, case
        from sqlalchemy.sql import select, and_
        import sys
        argv = sys.argv
        meta = MetaData(db._dbEngine)
        if ('ETL_LOG_TABLE' in dir(config)):
            table = 'etl_log_' + config.ETL_LOG_TABLE.lower()
            xdm_log = XdmLib.table_access(table,meta,XdmLib.oracle_resolve_synonyms)
            stmt = None
            if result is None:
                result = {
                    'status': 'ERROR',
                    'log_text': '[Unknow Exception]'
                }
            status = None
            result_status = result.get('status', 'ERROR')
            record = False
            if result_status != 'pass':
                if result_status == 'archive':
                    status = 'PASS'
                else:
                    status = 'FAIL'
            log_text = result.get('log_text', None)
            if log_text is not None:
                if len(log_text) >= 4000:
                    log_text = log_text[0:3999]
            stmt = xdm_log.insert().values(
                {
                    'loader_name': '.'.join(os.path.basename(argv[0]).split('.')[:-1]),
                    'pid': str(os.getpid()),
                    'load_file_name': file_info.get('file_name', None),
                    'load_file_path': os.path.abspath(file_info.get('file_path', '.')),
                    'status': status,
                    'log_text': log_text,
                    'start_time': file_info.get('start_time', None),
                    'end_time': datetime.datetime.now()
                }
            )
            if ('PASS_LOG_RECORD' in dir(config) and config.PASS_LOG_RECORD and status == 'PASS'):
                record = True
            if ('FAIL_LOG_RECORD' in dir(config) and config.FAIL_LOG_RECORD and status == 'FAIL'):
                record = True
            if record:
                db._connObj.execute(stmt)
                db.commit()
    except Exception as ex:
        try:
            db.rollback()
        except Exception as ex2:
            pass
        log.info('Record to {0} Failed'.format(table))
