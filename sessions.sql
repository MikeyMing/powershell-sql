SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SELECT 
                des.session_id AS spid
				,@@SERVERNAME AS Instance
                ,ISNULL(lb.lead_blocker,0) AS [lead]
                ,COUNT(*) OVER (PARTITION BY des.session_id) AS Cnt
                ,CONVERT(varchar,DATEPART(DAY, des.last_request_start_time ) ) + ':' + CONVERT(varchar,DATEPART(MONTH, des.last_request_start_time ) ) + '-' + CONVERT(varchar,des.last_request_start_time,24) AS StTm
                --,des.last_request_start_time
                --,des.last_request_end_time
                --,DATEDIFF(s,des.last_request_start_time, des.last_request_end_time) S
                ,der.blocking_session_id AS Blk
                ,host_name
                ,DB_NAME(s.dbid) AS DatabaseName
                --,LEFT(des.program_name,15) AS ShortProg
                --,des.program_name AS [Full Program Name]
                ,case when login_name = 'ZEBRA\admmikfle' THEN '**ME**' else login_name END AS Login
                --,nt_user_name
                ,des.status
				,(ssu.user_objects_alloc_page_count - ssu.user_objects_dealloc_page_count) * 8 / 1024 AS UserTempDBMB
				,(ssu.internal_objects_alloc_page_count - ssu.internal_objects_dealloc_page_count) * 8 / 1024 AS SysTempDBMB
				--,ssu.internal_objects_alloc_page_count
                ,des.session_id AS spid
                ,des.cpu_time AS SessCPU
                --,des.reads AS SessR
                --,des.writes AS SessW
                ,des.logical_reads AS SessLR
                ,der.logical_reads AS ReqLR
				,der.cpu_time AS ReqCPU
                ,case des.transaction_isolation_level WHEN 0 THEN '?' WHEN 1 THEN 'RUC' WHEN 2 THEN 'RC' WHEN 3 THEN 'RR' WHEN 4 THEN 'S' WHEN 5 THEN 'SS' ELSE CAST(deS.TRANSACTION_ISOLATION_LEVEL AS VARCHAR(32)) END AS IsolationLevel
                ,der.wait_resource, der.last_wait_type
                ,der.percent_complete AS '%cmplt'
                ,dest.text AS CurrentCommand
                ,destc.text AS MostRecentText
                ,CONVERT(varchar,DATEPART(DAY, GETDATE() ) ) + ':' + CONVERT(varchar,DATEPART(MONTH, GETDATE() ) ) + ' ' + CONVERT(varchar,GETDATE(),24) AS SnapShtTm
                ,des.session_id AS spid
                ,SUBSTRING(dest.text, (der.statement_start_offset/2)+1,           ((CASE der.statement_end_offset            WHEN -1 THEN DATALENGTH(dest.text)           ELSE der.statement_end_offset           END - der.statement_start_offset)/2) + 1) AS statement_text 
FROM sys.dm_exec_sessions  DES LEFT OUTER JOIN  sys.dm_exec_requests DER ON DER.session_id = DES.session_id OUTER APPLY sys.dm_exec_sql_text(der.plan_handle) dest LEFT OUTER JOIN sys.dm_exec_connections DEC ON DEC.session_id = des.session_id OUTER APPLY sys.dm_exec_sql_text(dec.most_recent_sql_handle) destc JOIN sys.sysprocesses S ON s.[spid] = des.session_id OUTER APPLY ( SELECT lead_blocker = 1 FROM master.dbo.sysprocesses sp WHERE sp.spid IN (SELECT blocked FROM master.dbo.sysprocesses) AND sp.blocked = 0 AND sp.spid = der.session_id ) lb
LEFT OUTER JOIN sys.dm_db_session_space_usage ssu ON DES.session_id = ssu.session_id
LEFT OUTER JOIN sys.dm_db_task_space_usage tsu ON DES.session_id = tsu.session_id
--LEFT OUTER JOIN  sys.dm_tran_locks tl ON tl.request_session_id = des.session_id
WHERE (des.session_id > 50 OR des.session_id IS NULL) AND des.session_id <> @@SPID 
                                AND                                       DB_NAME(s.dbid)                           LIKE '%%'                                                                             --DATABASE NAME
                                AND                                       login_name                                        LIKE '%%'                                                                             --LOGIN NAME
                                AND                                       des.session_id                  LIKE '%%'                                                                             --SESSION ID
                                AND                                       des.host_name                                LIKE '%%'                                                                             --HOST NAME
                                AND                                       DB_NAME(s.dbid)                           NOT IN ('master', 'msdb')             --EXCLUDE SYSTEM DATABASES (not tempdb)
ORDER BY der.blocking_session_id DESC, des.logical_reads DESC
--ORDER BY DB_NAME(s.dbid)


----select * from PROD_Archive.dbo.archivelog
--SELECT *
--FROM sys.dm_tran_locks
--WHERE resource_type <> 'DATABASE'


