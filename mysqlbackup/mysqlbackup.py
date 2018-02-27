#!/usr/bin/python3
#-------------------------------------------------------------------------------
# Name:        mysqlbackup.py
# Purpose:     MySQL backup script.
#
# Author:      shikano.takeki
#
# Created:     22/02/2018
# Copyright:   (c) shikano.takeki 2018
# Licence:     <your licence>
#-------------------------------------------------------------------------------
# -*- coding: utf-8 -*-
## import modules.
# mysql operation
from py_mysql.mysql_custom import MySQLDB
# date operation
from datetime_skt.datetime_orig import dateArithmetic
# file/dir operation
from osfile import fileope
# logger
from mylogger import logger
from mylogger.factory import StdoutLoggerFactory, FileLoggerFactory
# read/write operation to file
from iomod import rwfile
# python standard modules
from os.path import split
import subprocess
import time

# base log file name.
LOGFILE = 'mariadb_backup.log'
# config file name.
# default config file path is <python lib>/dist|site-packages/mysqlbackup/config
CONFIG_FILE = 'backup.json'
INST_DIR = ''
CONFIG_PATH = ''

class MySQLBackup(object):
    """
    """

    def __new__(cls, loglevel=None, handler=None):
        self = super().__new__(cls)
        # JSONファイルから各種データの読み込み、インスタンス変数にセット.
        self.parsed_json = {}
        self._get_pylibdir()
        self.rwfile = rwfile.RWFile()
        self.pj = rwfile.ParseJSON()
        self._load_json()
        self._set_data()

        # set up logger.
        # loglevel 20 = info level.
        if loglevel is None:
            loglevel = 20
        if handler is None:
            handler = 'console'
        self._handler = handler
        # create file logger.
        if self._handler == 'file':
            filelogger_fac = FileLoggerFactory(loglevel=loglevel)
            self._logger = filelogger_fac.create(file=LOGFILE)
        # create stdout logger.
        elif self._handler == 'console':
            stdlogger_fac = StdoutLoggerFactory(loglevel=loglevel)
            self._logger = stdlogger_fac.create()
        # create rotation logger.

        self.date_arith = dateArithmetic()
        self.year = self.date_arith.get_year()
        self.month = self.date_arith.get_month()
        self.day = self.date_arith.get_day()
        self.ym = "{0}{1}".format(self.year, self.month)
        self.md = "{0}{1}".format(self.month, self.day)
        self.ymd = "{0}{1}{2}".format(self.year, self.month, self.day)
        self.ymdhm = self.date_arith.get_now_full()
        self.bk_dir = "{0}mysqlbackup_{1}".format(self.bk_root, self.ymdhm)

        return self

    def _get_pylibdir(self):
            import mysqlbackup

            global INST_DIR, CONFIG_PATH
            INST_DIR = split(mysqlbackup.__file__)[0]
            CONFIG_PATH = "{0}/config/{1}".format(INST_DIR, CONFIG_FILE)

    def _load_json(self):
        """jsonファイルをパースする."""
        self.parsed_json = self.pj.load_json(file=r"{}".format(CONFIG_PATH))

    def _set_data(self):
        """パースしたJSONオブジェクトから必要なデータを変数にセットする."""
        # PATH
        self.bk_root = self.parsed_json['default_path']['BK_ROOT']

        # MYSQL
        self.myuser = self.parsed_json['mysql']['MYSQL_USER']
        self.mypass = self.parsed_json['mysql']['MYSQL_PASSWORD']
        self.mydb = self.parsed_json['mysql']['MYSQL_DB']
        self.myhost = self.parsed_json['mysql']['MYSQL_HOST']
        self.myport = self.parsed_json['mysql']['MYSQL_PORT']

    def _decrypt_string(self, line: str):
        import codecs

        decrypted = codecs.decode(line, 'rot_13')
        return decrypted

    def _remove_old_backup(self, preserved_day=None):
        """旧バックアップデータを削除する.

        Args:
            param1 preserved_day: バックアップを保存しておく日数. デフォルトは3日
                type: int
        """
        if preserved_day is None:
            preserved_day = 3
        # バックアップルートにあるディレクトリ名一覧を取得する.
        dir_names = fileope.get_dir_names(dir_path=self.bk_root)
        if len(dir_names) == 0:
            return
        for dir_name in dir_names:
            # バックアップ用ディレクトリ以外は除外.
            if not self.rwfile.is_matched(line=dir_name, search_objs=['^[0-9]{6}$']):
                continue
            # 日毎のバックアップディレクトリ名一覧の取得.
            monthly_bkdir = "{0}{1}".format(self.bk_root, dir_name)
            daily_bkdirs = fileope.get_dir_names(dir_path=monthly_bkdir)
            # 日毎のバックアップディレクトリがひとつも存在しない場合は
            # 月毎のバックアップディレクトリ自体を削除する.
            if len(daily_bkdirs) == 0:
                fileope.remove_dir(monthly_bkdir)
                continue
            for daily_bkdir in daily_bkdirs:
                # 現在の日付と対象となるディレクトリのタイムスタンプの日数差を計算する.
                backup_dir = "{0}/{1}".format(monthly_bkdir, daily_bkdir)
                sub_days = self.date_arith.subtract_target_from_now(backup_dir)
                self._logger.debug("sub_days = {}".format(sub_days))
                    # 作成されてから3日以上経過しているバックアップディレクトリを削除する.
                if sub_days >= preserved_day:
                    try:
                        fileope.f_remove_dirs(path=backup_dir)
                    except OSError as e:
                        error = "raise error! failed to trying remove {}".format(backup_dir)
                        self._logger.error(error)
                        raise e
                    else:
                        stdout = "remove old dump files: {}".format(backup_dir)
                        self._logger.info(stdout)

    ''' ログローテーション機能はLoggerモジュールで実装したためこれはさようなら

    def _remove_old_log(self, type, elapsed_days=None):
        """一定日数経過したログファイルを削除する.

        Args:
            param1 type: 削除対象のログを選択する.
                指定可能な値 ... 1 | 2
                1 ... 標準ログ
                2 ... エラーログ
            param1 elapsed_days: ログファイルを削除する規定経過日数. デフォルトは5日.
        """
        if type == 1:
            path = self.log_root
        elif type == 2:
            path = self.errlog_root
        else:
            raise TypeError("引数 'type' は 1 又は 2 を入力してください。")

        if elapsed_days is None:
            elapsed_days = 5

        # ログファイル格納ディレクトリからログファイル名一覧を取得する.
        log_files = fileope.get_file_names(dir_path=path)
        for log_file in log_files:
            target = "{0}{1}".format(path, log_file)
            # 現在の日付とログファイルのタイムスタンプを比較する.
            days = self.date_arith.subtract_target_from_now(target)
            # 5日以上経過しているログファイルは削除する.
            if days >= elapsed_days:
                try:
                    fileope.rm_filedir(path=target)
                except OSError as e:
                    error = "raise error! failed to trying remove file {}".format(target)
                    self.output_errlog(error)
                    raise e
                else:
                    stdout = "remove a old log file. {}".format(target)
                    self.output_logfile(stdout)
    '''

    def _mk_backupdir(self):
        """バックアップ用ディレクトリを作成する.
        """
        # make a directory for db backup.
        dbs = self.get_dbs_and_tables()
        for db in dbs.keys():
            db_bkdir = fileope.join_path(self.bk_dir, db)
            if not fileope.dir_exists(path=r"{}".format(db_bkdir)):
                try:
                    fileope.make_dirs(path=r"{}".format(db_bkdir))
                except OSError as e:
                    error = "raise error! failed to trying create a backup directory."
                    self._logger.error(error)
                    raise e
                else:
                    self._logger.info("create a backup directory: {}".format(db_bkdir))

    def get_dbs_and_tables(self):
        """MYSQLに接続してデータベース名とテーブル名を取得する.

            Returns:
                データベース名とテーブル名を対応させた辞書.
                {'db1': (db1_table1, db1_table2, ...), 'db2': (db2_table1, ...)}
        """
        results = {}
        # MySQLに接続する.
        with MySQLDB(host=self.myhost,
                     dst_db=self.mydb,
                     myuser=self.myuser,
                     mypass=self._decrypt_string(self.mypass),
                     port=self.myport) as mysqldb:
            # SHOW DATABASES;
            self._logger.info("Database names now acquireing...")
            sql = mysqldb.escape_statement("SHOW DATABASES;")
            cur_showdb = mysqldb.execute_sql(sql)
            for db_name in cur_showdb.fetchall():
                for db_str in db_name:
                    # information_schema と peformance_schema DBはバックアップ対象から除外.
                    if db_str.lower() in {'information_schema', 'performance_schema'}:
                        continue
                    # DBに接続する.
                    mysqldb.change_database(db_str)
                    # SHOW TABLES;
                    sql = mysqldb.escape_statement("SHOW TABLES;")
                    cur_showtb = mysqldb.execute_sql(sql)
                    for table_name in cur_showtb.fetchall():
                        for table_str in table_name:
                            # 辞書にキーとバリューの追加.
                            results.setdefault(db_str, []).append(table_str)
        self._logger.info("succeeded acquireing database names.")
        return results

    def mk_cmd(self, params):
        """実行するLinuxコマンドを成形する.

        Args:
            param1 params: パラメータ.

        Return.
            tupple command.
        """
        self._logger.info("creating mysql dump command...")
        cmds = tuple()
        for db, tables in params.items():
            for table in tables:
                self._logger.debug(table)
                output_path = "{0}/{1}/{2}_{3}.sql".format(self.bk_dir,
                                                           db,
                                                           self.ymd,
                                                           table)
                # -R オプションははずして、ループの外でSPのみを出力するmysqldumpを実行する.
                # mysqqldump -u{} -p{} --routines --no-data --no-create-info {db} > {dump}
                mysqldump_cmd = (
                                "mysqldump -u{0} -p{1} -q --skip-opt -R {2} {3} > "
                                "{4}".format(self.myuser,
                                             self._decrypt_string(self.mypass),
                                             db,
                                             table,
                                             output_path)
                                )
                split_cmd = mysqldump_cmd.split()
                cmds += (split_cmd,)

        return cmds

    def do_backup(self, exc_cmds: tuple):
        """mysqldumpコマンドをサーバで実行することによりバックアップを取得する.

            Args:
                param1 exc_cmd: 実行するコマンド タプル.

            Returns:

        """
        statement = "backup start. Date: {}".format(self.ymd)
        self._logger.info(statement)
        print(statement)
        for exc_cmd in exc_cmds:
            try:
                subprocess.check_call(args=' '.join(exc_cmd), shell=True)
            except subprocess.CalledProcessError as e:
                error = "an error occured during execution of following command.\n{}".format(e.cmd)
                self._logger.error(error)
            else:
                stdout = "mysqldump succeeded. dumpfile is saved {}".format(exc_cmd[len(exc_cmd) - 1])
                self._logger.info(stdout)
        self._logger.info("complete backup process.")

    def compress_backup(self, del_flag=None):
        """取得したバックアップファイルを圧縮処理にかける.

        Args:
            param1 del_flag: 圧縮後、元ファイルを削除するかどうかのフラグ.
                             デフォルトでは削除する.
        """
        self._logger.info("start compression.")
        if del_flag is None:
            del_flag = True
        # DBのディレクトリ名を取得.
        dir_list = fileope.get_dir_names(self.bk_dir)
        # gzip圧縮処理
        for dir_name in dir_list:
            target_dir = fileope.join_path(self.bk_dir, dir_name)
            file_list = fileope.get_file_names(r'{}'.format(target_dir))
            for file_name in file_list:
                target_file = fileope.join_path(target_dir, file_name)
                try:
                    fileope.compress_gz(r'{}'.format(target_file))
                except OSError as oserr:
                    error = oserr.strerror
                    # output error line... "Error: {} failed to compress.".format(target_file))
                    self._logger.error(error)
                except ValueError as valerr:
                    error = valerr
                    # "Error: {} failed to compress.".format(target_file))
                    self._logger.error(error)
                else:
                    if del_flag:
                        fileope.rm_filedir(target_file)
        self._logger.info("complete compressing dump files.")

    def main(self):
        """main.
        """
        start = time.time()
        # バックアップ用ディレクトリの作成.
        self._mk_backupdir()
        # 旧バックアップデータの削除.
        self._remove_old_backup()
        # ログファイルの削除.
        #self._remove_old_log(type=1)
        #self._remove_old_log(type=2)
        # DB名とテーブル名一覧の取得.
        dbs_tables = self.get_dbs_and_tables()
        # 実行するLinuxコマンドを生成.
        commands = self.mk_cmd(params=dbs_tables)
        # mysqldumpの実行.
        self.do_backup(commands)
        # 圧縮処理
        self.compress_backup()

        elapsed_time = time.time() - start
        line = "elapsed time is {0} sec. {1} finished.".format(elapsed_time, __file__)
        self._logger.info(line)
        print(line)
        # close
        #self._logger.close()


if __name__ == '__main__':
    import argparse
    import mysqlbackup

    lib_dir = split(mysqlbackup.__file__)[0]
    with open(fileope.join_path(lib_dir, 'README')) as f:
        description = f.read()

    with open(fileope.join_path(lib_dir, 'EPILOG')) as file:
        epilog = file.read()

    argparser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                        description=description,
                                        epilog=epilog)
    argparser.add_argument('-l', '--loglevel', type=int, required=False,
                           default=20,
                           help='log level. need to set int value 10 ~ 50.\n' \
                           'default is INFO. 10:DEBUG, 20:INFO, 30:WARNING, 40:ERROR, 50:CRITICAL')
    argparser.add_argument('-H', '--handler', type=str, required=False,
                           default='console',
                           help="settings the handler of log outputed.\n" \
                                "default handler is 'console'. log is outputed in standard out.\n" \
                                 "available value is 'file' | 'console'")
    args = argparser.parse_args()

    db_backup = MySQLBackup(loglevel=args.loglevel, handler=args.handler)
    db_backup.main()
    # logger close
    db_backup._logger.close()



