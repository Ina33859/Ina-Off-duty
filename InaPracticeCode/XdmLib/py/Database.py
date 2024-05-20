# -*- coding: UTF-8 -*-
'''
 (c) Copyright 2017, XDMTECH All rights reserved.

 System name: XdmLib
 Source name: XdmLib/Database.py
 Description: XDM Common Libs package
'''
'''
 Modification history:
 Date        Ver.   Author           Comment
 ----------  -----  ---------------  -----------------------------------------
 2017/12/07  I0.00  Even Chen        Initial Release
 2018/06/13  A0.01  Even Chen        Add dynamic parameters auto matching(count), fixed some issue
 2018/08/06  A0.02  Even Chen        Add mssql+pyodbc support
 2019/03/15  A0.03  Even Chen        Add sqlite support(Python3)
 2019/03/15  A0.04  Even Chen        Add postgresql+psycopg2 support
 2020/08/06  A0.05  Owen Wang        Add executemany support
 2020/09/23  A0.06  Owen Wang        Add executemanyfordataframe support
 2020/12/08  A0.07  Evelyn Sun       Set Postgres TimeOu:3600000ms
 2021/01/18  A0.07  Owen Wang        Add db2 support
 2021/07/12  M0.08  Jie Dai          Modity DBConnection only hide pwd
 2021/01/28  A0.09  Owen Wang        Add executemanybycopy & executemanybycopyexpert support
 2021/09/18  M0.10  Jie Dai          Modify debug module
 2021/10/18  M0.11  Jie Dai          executemanybycopyexpert to_csv remove quoting
 2021/10/22  M0.12  Jie Dai          optimize DBConnetion only hide pwd
'''


#===============================================================================
# DB Object
#===============================================================================
import re


class DbClient(object):
    '''
    classdocs
    '''
    imp_module = "sqlite3"
    connection = None
    cursor = None
    conn_str = ''
    exec_params = {}
    enable_debug = False
    _statement = ''
    _parameters = {}
    _keywordParameters = None
    _columns = []
    _isdisconnect = True
    _log = None
    _tuple_in_list = []
    
    _dbEngine = None
    _connObj = None

    def __init__(self, conn_str, autocommit=False, logging_obj=None, enable_debug=False, **params):
        '''
        Constructor
        '''
        self.enable_debug = enable_debug
        import logging
        if logging_obj and type(logging_obj) is type(logging):
            self._log = logging_obj
        else:
            logging.basicConfig(level=logging.DEBUG)
            self._log = logging
            self._log.info("Non set logging! Using default logging.")
        
        try:
            pwd_after_constr='@'
            pwd_str='******'
            if conn_str.count('@') != 1:
                pwd_after_constr = re.findall(r"\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b", conn_str)[0]
                pwd_str = '******@'
            # A0.03*
            if conn_str == ':memory:' or conn_str.startswith('sqlite://'):
                # https://docs.python.org/3/library/sqlite3.html
                self.imp_module = "sqlite3"
                #self._log.debug("Import module: sqlalchemy")
                try:
                    import sqlalchemy
                except ImportError as e:
                    raise e
                
                self.conn_str = conn_str if conn_str != ':memory:' else 'sqlite://'
                self._log.debug(self.conn_str.replace(self.conn_str[self.conn_str.find(":",self.conn_str.find(":")+1)+1:self.conn_str.find(pwd_after_constr)],pwd_str))
                self._dbEngine = sqlalchemy.create_engine(self.conn_str, connect_args=params)
                self._dbEngine.connect()
                self._connObj = self._dbEngine.connect()
                self.connection = self._connObj.connection
                self._isdisconnect = False
                self.cursor = self.connection.cursor()
            # A0.03&
            elif conn_str.startswith('oracle+cx_oracle://'):
                self.imp_module = "cx_Oracle"
                #self._log.debug("Import module: sqlalchemy")
                try:
                    import sqlalchemy
                except ImportError as e:
                    raise e
                self.conn_str = conn_str
                self._log.debug(self.conn_str.replace(self.conn_str[self.conn_str.find(":",self.conn_str.find(":")+1)+1:self.conn_str.find(pwd_after_constr)],pwd_str))
                self._dbEngine = sqlalchemy.create_engine(self.conn_str, connect_args=params)
                self._dbEngine.connect()
                self._connObj = self._dbEngine.connect()
                self.connection = self._connObj.connection
                self._isdisconnect = False
                if autocommit: self.connection.autocommit = True
                self.cursor = self.connection.cursor()
            elif conn_str.startswith('mssql+pyodbc://'):
                self.imp_module = "pyodbc"
                #self._log.debug("Import module: sqlalchemy")
                try:
                    import sqlalchemy
                    import pyodbc
                except ImportError as e:
                    raise e
                driver_list = []
                fast_execute = True
                if '?driver=' not in str(conn_str).lower():
                    driver_list = list(filter(lambda x:str(x).endswith('SQL Server'),pyodbc.drivers()))
                    driver_list.sort(reverse=True)
                    if 'SQL Server' in driver_list:
                        driver_list.remove('SQL Server')
                        driver_list.append('SQL Server')
                    if len(driver_list) > 0:
                        conn_str += '?driver='+driver_list[0]
                if str(conn_str).endswith('?driver=SQL Server') or str(conn_str).endswith('?driver=SQL+Server'):
                    fast_execute = False
                conn_str += '&TrustServerCertificate=yes'
                self.conn_str = conn_str
                self._log.debug(self.conn_str.replace(self.conn_str[self.conn_str.find(":",self.conn_str.find(":")+1)+1:self.conn_str.find(pwd_after_constr)],pwd_str))
                self._dbEngine = sqlalchemy.create_engine(self.conn_str)
                self._dbEngine.connect()
                self._connObj = self._dbEngine.connect()
                self.connection = self._connObj.connection
                self._isdisconnect = False
                self.cursor = self.connection.cursor()
                self.cursor.fast_executemany = fast_execute
            # A0.04*
            elif conn_str.startswith('postgresql+psycopg2://'):
                # https://docs.sqlalchemy.org/en/latest/dialects/postgresql.html#module-sqlalchemy.dialects.postgresql.psycopg2
                self.imp_module = "psycopg2"
                #self._log.debug("Import module: sqlalchemy")
                try:
                    import sqlalchemy
                except ImportError as e:
                    raise e
                self.conn_str = conn_str
                self._log.debug(self.conn_str.replace(self.conn_str[self.conn_str.find(":",self.conn_str.find(":")+1)+1:self.conn_str.find(pwd_after_constr)],pwd_str))
                self._dbEngine = sqlalchemy.create_engine(self.conn_str,echo=False,client_encoding='utf8',connect_args={"options":"-c statement_timeout=3600000"})
                #self._dbEngine = sqlalchemy.create_engine(self.conn_str, connect_args=params)
                self._dbEngine.connect()
                self._connObj = self._dbEngine.connect()
                self.connection = self._connObj.connection
                self._isdisconnect = False
                self.cursor = self.connection.cursor()
            # A0.04&
            # A0.07*
            elif conn_str.startswith('ibm_db_sa://'):
                # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.5.0/com.ibm.swg.im.dbclient.python.doc/doc/t0060891.html
                # ibm_db_sa://user:password@host.name.com:50000/database
                self.imp_module = "ibm_db_dbi"
                self._log.debug("Import module: sqlalchemy")
                try:
                    import sqlalchemy
                    import ibm_db_sa
                except ImportError as e:
                    raise e
                self.conn_str = conn_str
                self._log.debug(self.conn_str.replace(self.conn_str[self.conn_str.find(":",self.conn_str.find(":")+1)+1:self.conn_str.find(pwd_after_constr)],pwd_str))
                self._dbEngine = sqlalchemy.create_engine(self.conn_str, connect_args=params)
                self._dbEngine.connect()
                self._connObj = self._dbEngine.connect()
                self.connection = self._connObj.connection
                self._isdisconnect = False
                self.cursor = self.connection.cursor()
            # A0.07&
            else:
                self._log.debug("Import module: %s", self.imp_module)
                try:
                    import importlib
                    Database = importlib.import_module(self.imp_module)
                except ImportError as e:
                    raise e
                
#                 if db_type != conn_str:
#                     conn_str = conn_str.lstrip(db_type + "://")
#                     import re
#                     conn_str = re.sub(r"(?P<id>[^:]+):(?P<pw>[^@]+)@", r"\g<id>/\g<pw>@", conn_str)
                self.conn_str = conn_str
                self._log.debug(self.conn_str.replace(self.conn_str[self.conn_str.find(":",self.conn_str.find(":")+1)+1:self.conn_str.find(pwd_after_constr)],pwd_str))
                self.connection = Database.connect(self.conn_str, **params)
                self._isdisconnect = False
                self.cursor = self.connection.cursor()
        except Exception as e:
            self._log.warn(e)
            raise e
    
    def execute(self, statement, *parameters, **keywordParameters):
        # A0.01 self._statement = statement
        self._parameters = parameters
        self._keywordParameters = keywordParameters
        # A0.01*
        if self._statement != statement:
            self._statement = statement
            self._columns.clear()
            if parameters:
                if parameters[0] and type(parameters[0]) is dict:
                    import re
                    # Dynamic parameters naming: https://docs.oracle.com/cd/E11882_01/timesten.112/e21642/names.htm#TTSQL191
                    pattern = re.compile(r':([\w|_|#|$|@]+)[\s|,]')
                    self._columns = pattern.findall(statement)
                    # self._columns = sorted(pattern.findall(statement), key=len, reverse=True)
                    if self.enable_debug: self._log.debug("SQL include columns: %s", self._columns)
#                 else:
#                     self._log.debug("The object '*parameters' is not a dict{}: %s", parameters)
                # pass
            # pass
        # pass
        if self._columns:
            self.exec_params.clear()
            if parameters[0] and type(parameters[0]) is dict:
                for paramter in self._columns:
                    try:
                        self.exec_params[paramter] = parameters[0][paramter]
                    except KeyError:
                        self._log.debug("The 'parameters' lose key: %s.", paramter)
                    # pass
                # pass
#             else:
#                 self._log.debug("The object '*parameters' is not a dict{}: %s", parameters)
            # pass
            return self.cursor.execute(statement, self.exec_params, **keywordParameters)
        # A0.01&
        if (self.imp_module in ("ibm_db_dbi","psycopg2")):
            if len(keywordParameters)==0:
                self.cursor.execute(statement , *parameters)
            else:
                self.cursor.execute(statement, *parameters, **keywordParameters)
            if self.enable_debug:
                self._log.debug(self.getstatement())
            return self.cursor

        if self.enable_debug:
            self._log.debug(self.getstatement())
        return self.cursor.execute(statement , *parameters,**keywordParameters)
    # pass
    
    # A0.05
    def executemany(self, statement, tuple_in_list):
        # sqlite3 statement   - "INSERT INTO samples (COL1,COL2) VALUES (?, ?)"
        # oracle statement    - "INSERT INTO samples (COL1,COL2) VALUES (:1, :2)"
        # pyodbc statement    - "INSERT INTO samples (COL1,COL2) VALUES (?, ?)"
        # psycopg2 statement  - "INSERT INTO samples (COL1,COL2) VALUES (%s, %s)"
        # db2 statement       - "INSERT INTO samples (COL1,COL2) VALUES (?, ?)"
        # tuple_in_list samples - [ ('A', 1), ('B', 2) ] 
        self._tuple_in_list = list(tuple_in_list)
        if self._statement != statement:
            self._statement = statement
        
        if(self.imp_module == "psycopg2"):
            # statement contains only one %s placeholder
            ##INSERT INTO {table_name} (column1, column2) VALUES %s
            ##update {table_name} 
            ##set column = {temp_table}.column1 FROM (VALUES %s) AS {temp_table} (column1, column2)
            ##where {table_name}.column = {temp_table}.column2
            ##delete from {table_name} 
            ##using (VALUES %s) AS {temp_table} (column1, column2)
            ##where {table_name}.column = {temp_table}.column2
            from psycopg2 import extras
            from psycopg2 import sql
            query = sql.SQL(statement)
            if self.enable_debug:
                self._log.debug(self.getstatement())
            return extras.execute_values(self.cursor,query.as_string(self.cursor), tuple_in_list)
        if self.enable_debug:
            self._log.debug(self.getstatement())
        return self.cursor.executemany(statement, tuple_in_list)

    # A0.06
    def executemanyfordataframe(self, statement, df):
        # sqlite3 statement   - "INSERT INTO samples (COL1,COL2) VALUES (?, ?)"
        # oracle statement    - "INSERT INTO samples (COL1,COL2) VALUES (:1, :2)"
        # pyodbc statement    - "INSERT INTO samples (COL1,COL2) VALUES (?, ?)"
        # psycopg2 statement  - "INSERT INTO samples (COL1,COL2) VALUES (%s, %s)"
        # db2 statement       - "INSERT INTO samples (COL1,COL2) VALUES (?, ?)"
        # tuple_in_list samples - [ ('A', 1), ('B', 2) ] 
        tuple_in_list = df.values.tolist()
        self._tuple_in_list = tuple_in_list
        if self._statement != statement:
            self._statement = statement

        return self.executemany(statement, tuple_in_list)

    # A0.08
    def executemanybycopy(self, table_name, column_names, df, delimiter, nullStr = ''):
        # this function only for psycopg2
        # psycopg2 statement  - column_names=('col1','col2')
        from io import StringIO
        buffer = StringIO()
        df.to_csv(buffer, header=False, index=False, sep=delimiter)
        buffer.seek(0)
        if self.enable_debug:
            debug_str="copy_from: Table: {0}, \r\ncolumns_names: {1}".format(table_name,column_names)
            for show in df[0:10].values.tolist():
                debug_str += "{0}\r\n".format(show)
            self._log.debug(debug_str)
        return self.cursor.copy_from(buffer, table_name, sep=delimiter, null=nullStr, columns=column_names)

    # A0.08
    def executemanybycopyexpert(self, table_name, df):
        # this function only for psycopg2
        from io import StringIO
        buffer = StringIO()
        import csv
        df.to_csv(buffer, header=False, index=False)
        buffer.seek(0)
        sql = "COPY " + table_name + " FROM STDIN WITH (FORMAT CSV)"
        if self.enable_debug:
            debug_str = "copy_expert: Table: {0}, \r\ncolumns_names: {1}".format(table_name, list(df.columns.values))
            for show in df[0:10].values.tolist():
                debug_str += "{0}\r\n".format(show)
            self._log.debug(debug_str)
        return self.cursor.copy_expert(sql, buffer)

    def getstatement(self):
        if self._statement and (self._parameters or self._keywordParameters):
            import datetime
            for key, value in {k: self._keywordParameters[k] for k in sorted(self._keywordParameters, key=len, reverse=True)}.items():  # self._keywordParameters.items():
#                 if raw_type(value) is str:
#                     self._statement = self._statement.replace(":{0}".format(key), "'{0}'".format(value))
                if value is None:
                    self._statement = self._statement.replace(":{0}".format(key), "NULL")
                elif type(value) is int or type(value) is float:
                    self._statement = self._statement.replace(":{0}".format(key), "TO_NUMBER('{0}')".format(value))
                elif type(value) is datetime:
                    self._statement = self._statement.replace(":{0}".format(key), "TO_DATE('{:%Y-%m-%d %H:%M:%S}','yyyy/mm/dd hh24:mi:ss')".format(value))
                else:
                    self._statement = self._statement.replace(":{0}".format(key), "'{0}'".format(value))
                # pass
            # pass
            
            if self._parameters and self._parameters[0] and type(self._parameters[0]) is dict:
                for key, value in {k: self._parameters[0][k] for k in sorted(self._parameters[0], key=len, reverse=True)}.items():  # self._parameters[0].items():
#                     if raw_type(value) is str:
#                         self._statement = self._statement.replace(":{0}".format(key), "'{0}'".format(value))
                    if value is None:
                        self._statement = self._statement.replace(":{0}".format(key), "NULL")
                    elif type(value) is int or type(value) is float:
                        self._statement = self._statement.replace(":{0}".format(key), "TO_NUMBER('{0}')".format(value))
                    elif type(value) is datetime:
                        self._statement = self._statement.replace(":{0}".format(key), "TO_DATE('{:%Y-%m-%d %H:%M:%S}','yyyy/mm/dd hh24:mi:ss')".format(value))
                    else:
                        self._statement = self._statement.replace(":{0}".format(key), "'{0}'".format(value))
                    # pass
                # pass
            # pass

            if self._parameters and (type(self._parameters[0]) is tuple or type(self._parameters[0]) is list):
                self._statement += "{0}\r\n".format(self._parameters[0])

        if self._statement and self._tuple_in_list:
            tuple_in_show=self._tuple_in_list[0:10]
            for show in tuple_in_show:
                self._statement += "{0}\r\n".format(show)

        self._tuple_in_list=[]

        return self._statement
    # pass
    
    def begin(self):
        if self.imp_module == 'cx_Oracle' and self.connection:
            if self.connection.autocommit:
                self.connection.autocommit = False 
                self.cursor.close()
                self.cursor = self.connection.cursor()
            return self.connection.begin()
        else:
            return None 
    
    def commit(self):
        if self.connection:
            return self.connection.commit()
        else:
            return None
    
    def rollback(self):
        if self.connection:
            return self.connection.rollback()
        else:
            return None
    
    def close(self):
        try:
            self.cursor.close()
        except:
            pass
        finally:
            self.cursor = None
            del self.cursor
            
    def disconnect(self):
        self._statement = None
        del self._statement
        
        self._tuple_in_list = None
        del self._tuple_in_list
        
        if self._keywordParameters:
            self._keywordParameters.clear()
        self._keywordParameters = None
        del self._keywordParameters
        
        self._parameters = None
        del self._parameters
        
        self.conn_str = None
        del self.conn_str
        
        self.db_type = None
        del self.db_type
        
        self.close()
        
        try:
            self.connection.close()
        except:
            pass
        finally:
            self.connection = None
            del self.connection
            self._isdisconnect = True
            
            self._dbEngine.dispose()
            self._dbEngine = None
            del self._dbEngine
            self._connObj = None
            del self._connObj
    
    def __del__(self):
        if not self._isdisconnect:
            self.disconnect()
        pass


#===============================================================================
# Main(test method)
#===============================================================================
if __name__ == "__main__":
    "postgresql+psycopg2://User:Password@IP:Port/db_name"
    "oracle+cx_oracle://User:Password@IP:Port/db_name"
    db = DbClient("")
    print(db.conn_str)
