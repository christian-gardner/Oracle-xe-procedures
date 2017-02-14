CREATE OR REPLACE PROCEDURE DEFINE_DUPLICATES
IS

   TYPE GenRefCursor  IS REF CURSOR;

   TYPE BILLING_LOAD_QUEUE_PIDS IS RECORD
   (
     CLIENT           DBMS_SQL.VARCHAR2_TABLE,
     FILE_NAME        DBMS_SQL.VARCHAR2_TABLE,
     COMPLETED        DBMS_SQL.NUMBER_TABLE,
     FILE_TYPE        DBMS_SQL.VARCHAR2_TABLE,
     PID              DBMS_SQL.NUMBER_TABLE
    );


    GC          GenRefCursor;
    LQ          BILLING_LOAD_QUEUE_PIDS;
    SQL_STMT    VARCHAR2(32000);

BEGIN


     OPEN GC FOR   SELECT Q.CLIENT, Q.FILE_NAME, Q.COMPLETED, Q.FILE_TYPE, Q.PID
                FROM ( SELECT  A.CLIENT, A.FILE_NAME, A.COMPLETED, A.FILE_TYPE, A.PID
                       FROM ( SELECT  LTRIM(RTRIM( CLIENT)) AS CLIENT,
                                      LTRIM(RTRIM( FILE_NAME)) AS FILE_NAME,
                                      COMPLETED,
                                      FILE_TYPE, pid, rank() over ( partition by LTRIM(RTRIM( CLIENT)), LTRIM(RTRIM( FILE_NAME)) order by  ENTRYDTE , rownum) rk
                               FROM BOA_BILLING_LOAD_QUEUE
                              WHERE COMPLETED = 0) a
                WHERE A.RK > 1 ) Q ;

    FETCH GC BULK COLLECT INTO LQ.CLIENT, LQ.FILE_NAME, LQ.COMPLETED, LQ.FILE_TYPE, LQ.PID;

    CLOSE GC;

    for k in 1..LQ.PID.count loop

        SQL_STMT  := 'UPDATE BOA_BILLING_LOAD_QUEUE SET COMPLETED = 3 WHERE  PID = :1';
        EXECUTE IMMEDIATE SQL_STMT USING LQ.PID(k);

    end loop;


    COMMIT;



/*
left join ( select CLIENT, FILE_NAME
              from BOFA_FILES_PROCESSED ) b
       on ( b.client   = Q.client  and
            b.file_name = Q.file_name)
WHERE  b.file_name is NOT null
ORDER BY Q.PID DESC
*/


END;

/
