"""
    Author: WANGXINYU
	Email:	wangxy518@chinaunicom.cn
"""

import argparse
import cx_Oracle
import inspect
import json
import re


class Monitor(object):
    def monitor_alive(self):
        """monitor a instance  alive or not"""
        sql = "select INSTANCE_NAME from v$instance where status = 'OPEN' and database_status = 'ACTIVE'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print i[0]

    def dsksortratio(self):
        """Disk sorts ratio"""
        sql = "SELECT nvl(to_char(disk_sort.value / (disk_sort.value + memory_sort.value) * 100, 'FM99999990.9999'),'0') retvalue \
               FROM v$sysstat memory_sort, v$sysstat disk_sort \
               WHERE memory_sort.name = 'sorts (memory)' \
               AND disk_sort.name = 'sorts (disk)'"
        self.cur.execute(sql)
        res = self.cur.fetchone()
        print res

    def rcachehit(self):
        """Calculate Cache hit ratio"""
        sql = "SELECT nvl(to_char((1 -(physical_reads.value - physical_reads_direct_lob.value - physical_reads_direct.value) / logical_reads.value) * 100,'FM99999990.9999'), '0') retvalue \
               FROM v$sysstat logical_reads,v$sysstat physical_reads_direct_lob,v$sysstat physical_reads_direct,v$sysstat physical_reads \
               WHERE logical_reads.name = 'session logical reads' \
               AND physical_reads_direct.name = 'physical reads direct' \
              AND physical_reads_direct_lob.name = 'physical reads direct (lob)' \
              AND physical_reads.name = 'physical reads'"
        self.cur.execute(sql)
        res = self.cur.fetchone()
        print res

    def activesessioncount(self):
        """Number of active sessions"""
        sql = "select to_char(count(*))  from v$session t where username is not null   and status = 'ACTIVE'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        print res

    def tablespace_free_size(self):
        """free space of each tablespace"""
        sql = "select tablespace_name, to_char(trunc(sum(bytes) / (1024 * 1024 * 1024)))  SPACE(G) from dba_free_space group by tablespace_name"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print i[0]

    def dbfilesize(self):
        """Size of each datafiles"""
        sql = "select file_name,trunc(sum(bytes)/1024/1024/1024) GB   from dba_data_files group by file_name"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print i[0]

    def version(self):
        """Oracle version"""
        sql = "select banner from v$version"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print i[0]

    def running_time(self):
        """Instance running time (days)"""
        sql = "select trunc((sysdate-startup_time)) running_days from v$instance"
        self.cur.execute(sql)
        res = self.cur.fetchone()
        print res

    def commits_count(self):
        """sum of user commits"""
        sql = "select value   from v$sysstat  where name = 'user commits'"
        self.cur.execute(sql)
        res = self.cur.fetchone()
        print res

    def rollbacks_count(self):
        """count of rollbacks"""
        sql = "select value   from v$sysstat  where name = 'user rollbacks'"
        self.cur.execute(sql)
        res = self.cur.fetchone()
        print res

    def deadlocks_count(self):
        """count of deadlocks"""
        sql = "select value from v$sysstat where name = 'enqueue deadlocks'"
        self.cur.execute(sql)
        res = self.cur.fetchone()
        print res

    def redowrites(self):
        """count of redo writes"""
        sql = "select name,value from v$sysstat where name = 'redo writes'"
        self.cur.execute(sql)
        res = self.cur.fetchone()
        print res

    def table_scans(self):
        """Table scans"""
        sql = "select name,value from v$sysstat where name like  'table scans%'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print i[0]

    def table_scan_gotten(self):
        """Table scan rows gotten"""
        sql = "select name,value from v$sysstat where name like  '%gotten%'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print i[0]

    def index_fast_full_scans(self):
        """Index fast full scans (full)"""
        sql = "select name,value from v$sysstat where name like  'index fast full%'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print i[0]

    def hard_parse_ratio(self):
        """Hard parse ratio"""
        sql = "SELECT nvl(to_char(hard.value / total.value * 100, 'FM99999990.9999'), '0')  hard_parse   FROM v$sysstat hard, v$sysstat total WHERE hard.name = 'parse count (hard)'  AND total.name = 'parse count (total)'"
        self.cur.execute(sql)
        res = self.cur.fetchone()
        print res

    def netsent(self):
        """Bytes sent via SQL*Net to client"""
        sql = "select value from  v$sysstat where name = 'bytes sent via SQL*Net to client'"
        self.cur.execute(sql)
        res = self.cur.fetchone()
        print res

    def netresv(self):
        """Bytes received via SQL*Net from client"""
        sql = "select value v$sysstat where name = 'bytes received via SQL*Net from client'"
        self.cur.execute(sql)
        res = self.cur.fetchone()
        print res

    def netroundtrips(self):
        """SQL*Net roundtrips to/from client"""
        sql = "select name,value from v$sysstat where name = 'SQL*Net roundtrips to/from client'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print i[0]

    def logonscurrent(self):
        """Logons current"""
        sql = "select value from v$sysstat  where name like  'logons current'"
        self.cur.execute(sql)
        res = self.cur.fetchone()
        print res

    def lastarclog(self):
        """Last archived log sequence"""
        sql = "select group#,thread#  from v$log where archived = 'YES'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print i[0]

    def lastapplarclog(self):
        """Last applied archive log (at standby).Next items requires
        [timed_statistics = true]"""
        sql = "select lh.SEQUENCE#) from v$loghist lh, v$archived_log al  where lh.SEQUENCE# = al.SEQUENCE# and applied='YES'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print i[0]

    def freebufwaits(self):
        """Free buffer waits"""
        sql = "select se.time_waited from v$system_event se, v$event_name en where se.event(+) = en.name and en.name = 'free buffer waits'  "
        self.cur.execute(sql)
        res = self.cur.fetchone()
        print res

    def bufbusywaits(self):
        """Buffer busy waits"""
        sql = "select se.time_waited from v$system_event se, v$event_name en where se.event(+) = en.name and en.name = 'buffer busy waits'"
        self.cur.execute(sql)
        res = self.cur.fetchone()
        print res

    def logswcompletion(self):
        """log file switch completion"""
        sql = "select se.time_waited from v$system_event se, v$event_name en where se.event(+) = en.name and  en.name = 'log file switch completion'"
        self.cur.execute(sql)
        res = self.cur.fetchone()
        print res

    def logfilesync(self):
        """Log file sync"""
        sql = "select se.time_waited from v$system_event se, v$event_name en where se.event(+) = en.name and  en.name = 'log file sync'"
        self.cur.execute(sql)
        res = self.cur.fetchone()
        print res

    def logprllwrite(self):
        """Log file parallel write"""
        sql = "select se.time_waited from v$system_event se, v$event_name en where se.event(+) = en.name and  en.name = 'log file parallel write'"
        self.cur.execute(sql)
        res = self.cur.fetchone()
        print res

    def enqueue(self):
        """Enqueue waits"""
        sql = "select se.time_waited from v$system_event se, v$event_name en where se.event(+) = en.name and  en.name = 'enqueue'"
        self.cur.execute(sql)
        res = self.cur.fetchone()
        print res

    def dbseqread(self):
        """DB file sequential read waits"""
        sql = "select se.time_waited from v$system_event se, v$event_name en where se.event(+) = en.name and  en.name = 'db file sequential read'"
        self.cur.execute(sql)
        res = self.cur.fetchone()
        print res

    def dbscattread(self):
        """DB file scattered read"""
        sql = "select se.time_waited from v$system_event se, v$event_name en where se.event(+) = en.name and  en.name = 'db file scattered read'"
        self.cur.execute(sql)
        res = self.cur.fetchone()
        print res

    def dbsnglwrite(self):
        """DB file single write"""
        sql = "select se.time_waited from v$system_event se, v$event_name en where se.event(+) = en.name and en.name = 'db file single write'"
        self.cur.execute(sql)
        res = self.cur.fetchone()
        print res

    def dbprllwrite(self):
        """DB file parallel write"""
        sql = "select se.time_waited from v$system_event se, v$event_name en where se.event(+) = en.name and en.name = 'db file parallel write'"
        self.cur.execute(sql)
        res = self.cur.fetchone()
        print res

    def directread(self):
        """Direct path read"""
        sql = "select se.time_waited from v$system_event se, v$event_name en where se.event(+) = en.name and en.name = 'direct path read'"
        self.cur.execute(sql)
        res = self.cur.fetchone()
        print res

    def directwrite(self):
        """Direct path write"""
        sql = "select se.time_waited from v$system_event se, v$event_name en where se.event(+) = en.name and en.name = 'direct path write'"
        self.cur.execute(sql)
        res = self.cur.fetchone()
        print res

    def latchfree(self):
        """latch free"""
        sql = "select se.time_waited from v$system_event se, v$event_name en where se.event(+) = en.name and en.name = 'latch free'"
        self.cur.execute(sql)
        res = self.cur.fetchone()
        print res

    def tablespace(self, name):
        """Get tablespace usage"""
        sql = "SELECT tablespace_name, 100 - (TRUNC((max_free_mb / max_size_mb) * 100)) AS USED \
               FROM (SELECT a.tablespace_name, b.size_mb, a.free_mb, b.max_size_mb, a.free_mb + (b.max_size_mb - b.size_mb) AS max_free_mb \
               FROM (SELECT tablespace_name,  TRUNC(SUM(bytes) / 1024 / 1024) AS free_mb FROM dba_free_space GROUP BY tablespace_name) a, \
               (SELECT tablespace_name, TRUNC(SUM(bytes) / 1024 / 1024) AS size_mb, TRUNC(SUM(GREATEST(bytes, maxbytes)) / 1024 / 1024) AS max_size_mb \
                FROM dba_data_files GROUP BY tablespace_name) b  WHERE a.tablespace_name = b.tablespace_name)  order by 1"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print i[0]

    def tablespace_abs(self, name):
        """Get tablespace in use"""
        sql = '''SELECT df.tablespace_name "TABLESPACE", (df.totalspace - \
              tu.totalusedspace) "FREEMB" from (select tablespace_name, \
              sum(bytes) TotalSpace from dba_data_files group by tablespace_name) \
              df ,(select sum(bytes) totalusedspace,tablespace_name from dba_segments \
              group by tablespace_name) tu WHERE tu.tablespace_name = \
              df.tablespace_name '''
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print i[0]

    def show_tablespaces(self):
        """List tablespace """
        sql = "SELECT tablespace_name FROM dba_tablespaces ORDER BY 1"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print i[0]

    def show_tablespaces_temp(self):
        """List temporary tablespace names"""
        sql = "SELECT TABLESPACE_NAME FROM DBA_TABLESPACES WHERE   CONTENTS='TEMPORARY'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print i[0]

    def monitor_archive(self, archive):
        """diskgroup stats without disk discovery"""
        sql = "select trunc((total_mb-free_mb)*100/(total_mb)) PCT from  v$asm_diskgroup_stat "
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print i[0]

    def show_asm_volumes(self):
        """List als ASM volumes"""
        sql = "select NAME from v$asm_diskgroup_stat ORDER BY 1"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print i[0]

    def asm_volume_use(self, name):
        """Get ASM volume usage"""
        sql = "select round(((TOTAL_MB-FREE_MB)/TOTAL_MB*100),2) from  v$asm_diskgroup_stat"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print i[0]

    def query_lock(self):
        """Query lock in rac"""
        sql = "SELECT count(*) FROM gv$lock l WHERE  block=1"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print i[0]

    def query_redologs(self):
        """Redo logs"""
        sql = "select COUNT(*) from v$LOG WHERE STATUS='ACTIVE'"
        self.cur.execute(sql)
        res = self.cur.fetchone()
        print res

    def query_rollbacks(self):
        """Query Rollback in rac"""
        sql = "select nvl(trunc(sum(used_ublk*4096)/1024/1024),0) from  gv$transaction t,gv$session s where ses_addr = saddr"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print i[0]

    def query_sessions(self):
        """Query Sessions in rac"""
        sql = "select count(*) from gv$session where username is not null and status='ACTIVE'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        print res

    def tablespace_temp(self, name):
        """Query temporary tablespaces"""
        sql = "SELECT round(((TABLESPACE_SIZE-FREE_SPACE)/TABLESPACE_SIZE)*100,2)  PERCENTUAL FROM dba_temp_free_space"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print i[0]

    def query_sysmetrics(self, name):
        """Query v$sysmetric parameters"""
        sql = "select value from v$sysmetric where METRIC_NAME ='{0}' and \
              rownum <=1 order by INTSIZE_CSEC".format(name.replace('_', ' '))
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print i[0]

    def fra_use(self):
        """Query the Fast Recovery Area usage"""
        sql = "select round((SPACE_LIMIT-(SPACE_LIMIT-SPACE_USED))/  SPACE_LIMIT*100,2) FROM V$RECOVERY_FILE_DEST"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print i[0]

    def show_users(self):
        """Query the list of users on the instance"""
        sql = "SELECT username FROM dba_users ORDER BY 1"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        key = ['{#DBUSER}']
        lst = []
        for i in res:
            d = dict(zip(key, i))
            lst.append(d)
        print json.dumps({'data': lst})

    def user_status(self, dbuser):
        """Determines whether a user is locked or not"""
        sql = "SELECT account_status FROM dba_users WHERE username='{0}'" \
            .format(dbuser)
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print i[0]


class Main(Monitor):
    def __init__(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--username')
        parser.add_argument('--password')
        parser.add_argument('--address')
        parser.add_argument('--database')
        parser.add_argument('--port')

        subparsers = parser.add_subparsers()

        for name in dir(self):
            if not name.startswith("_"):
                p = subparsers.add_parser(name)
                method = getattr(self, name)
                argnames = inspect.getargspec(method).args[1:]
                for argname in argnames:
                    p.add_argument(argname)
                p.set_defaults(func=method, argnames=argnames)
        self.args = parser.parse_args()

    def db_connect(self):
        a = self.args
        username = a.username
        password = a.password
        address = a.address
        database = a.database
        port = a.port
        self.db = cx_Oracle.connect("{0}/{1}@{2}:{3}/{4}".format(
            username, password, address, port, database))
        self.cur = self.db.cursor()

    def db_close(self):
        self.cur.close()
        self.db.close()

    def __call__(self):
        try:
            a = self.args
            callargs = [getattr(a, name) for name in a.argnames]
            self.db_connect()
            try:
                return self.args.func(*callargs)
            finally:
                self.db_close()
        except Exception, err:
            print 0
            print str(err)


if __name__ == "__main__":
    main = Main()
    main()
