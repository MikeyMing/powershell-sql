$DBAInstance = "."
$DBADatabase = "DBA"
$smtpserver = "10.202.170.15"
[int]$ProgressInterval = 2


if ((test-path variable:SCRIPT:executionid) -eq $false ) { $ExecutionID = $([GUID]::newGuid()).Guid }

##Enable logging if the log table connection details are correct
$loggingEnabled = $false
$LoggingTestSQL = "select top 1 Logid from log"
Write-Host "Testing the log table."
Try
    {
    $LoggingTest = Invoke-Sqlcmd -ServerInstance $DBAInstance -Database $DBADatabase -Query $LoggingTestSQL -ConnectionTimeout 2 -ErrorAction SilentlyContinue
    Write-Host "The log table is accessible so enabling logging."
    $loggingEnabled = $true
    }
Catch
    {
    Write-Warning "The log table is not accessible so disbling logging to SQL Database table.  `nIf you require database logging, please furnish the '`$DBAInstance' and '`$DBADatabase' variables in the script. `nThe script can be found in the following location:`n`n$($myInvocation.MyCommand.Definition) "
    $loggingEnabled = $false
    }

##Check the SMTP Settings.
If ($(Test-NetConnection -ComputerName $smtpserver -Port 25 -InformationLevel Quiet) -eq $false)
    {
    If ($smtpserver -eq "")
        {
        Write-Warning "The SMTP server settings have not been specified. Emails will not be sent. Please furnish the '`$smtpserver' variable.  The script can be found in the following location:`n`n$($myInvocation.MyCommand.Definition)"
        }
    Else
        {
        Write-Warning "The SMTP server setting specified is not accessible.  Emails will not be sent.  The value given is '$($smtpserver)'.  "
        }
    }

Function Get-AUthSchemeAndServiceAccount([string]$Server)
{
    $sname = $Server
    Log -message "Server name = $sname"
   Try
      {
      $port = Get-PortForSQLInstance -Instance $sname
      $auth = Get-AuthenticationScheme -Instance $sname
      Log -message "auth for $sname = $auth"
      $serviceaccount = get-ServiceAccountForSQLServer -Instance $sname
      Log -message "serviceaccount for $sname = $serviceaccount"
      $new = New-Object psobject
      $new | Add-Member -Name "Server_Name" -Value $sname -MemberType NoteProperty
      $new | Add-Member -Name "auth_scheme" -Value $auth -MemberType NoteProperty
      $new | Add-Member -Name "ServiceAccount" -Value $serviceaccount -MemberType NoteProperty
      $new | Add-Member -Name "Port" -Value $port -MemberType NoteProperty

      $adUser = Get-ADObject -Filter "samAccountName -eq '$($serviceaccount.Split("\")[1])'" -Properties samAccountName,servicePrincipalName,userAccountControl 
      If ($adUser.UserAccountControl -band 0x80000)
        {
        $new | Add-Member -Name "SvcAccountTrustedForDelegation" -Value "Yes" -MemberType NoteProperty
        }
    Else
        {
        $new | Add-Member -Name "SvcAccountTrustedForDelegation" -Value "No" -MemberType NoteProperty
        }
    $new | Add-Member -Name "SPN" -Value $adUser.servicePrincipalName.value -MemberType NoteProperty
    $ADComnputer = Get-ADObject -Filter {cn -eq $sname} -Properties  samAccountName,servicePrincipalName,userAccountControl 
          If ($ADComnputer.UserAccountControl -band 0x80000)
        {
        $new | Add-Member -Name "ComputerAccountTrustedForDelegation" -Value "Yes" -MemberType NoteProperty
        }
    Else
        {
        $new | Add-Member -Name "ComputerAccountTrustedForDelegation" -Value "No" -MemberType NoteProperty
        }
      $new | Add-Member -Name "Error" -Value "" -MemberType NoteProperty
      $S = $S + $new
       }
   Catch
       {
       $auth = ""
       $serviceaccount = ""
       $new = New-Object psobject
       $new | Add-Member -Name "Server_Name" -Value $sname -MemberType NoteProperty
       $new | Add-Member -Name "auth_scheme" -Value $auth -MemberType NoteProperty
       $new | Add-Member -Name "ServiceAccount" -Value $serviceaccount -MemberType NoteProperty
       $new | Add-Member -Name "Port" -Value $port -MemberType NoteProperty
       $new | Add-Member -Name "Error" -Value $Error[0] -MemberType NoteProperty
       $S = $S + $new
       }
$S
}

function get-ServiceAccountForSQLServer ([string]$Instance)
{
Try
    {
    $arrInstance = $Instance.Split("\")
    $hostname = $arrInstance[0]
    if ($arrInstance.Count -eq 1)
        {
        $InstanceName = "MSSQLSERVER"
        }
    Else
        {
        $InstanceName = $arrInstance[1]
        }
log -message "Instance = $Instance InstanceName = $InstanceName"
    $uri ="ManagedComputer[@Name='$hostname']/ ServerInstance[@Name='$instanceName']/ServerProtocol[@Name='Tcp']"
    $mc1 = New-Object -TypeName Microsoft.SqlServer.Management.Smo.Wmi.ManagedComputer $hostname
    $port = $mc1.GetSmoObject($uri).Properties
    log -message $port -Level Info 
    $mc1.Services | Where-Object {$_.Type -eq "SqlServer" -and $_.DisplayName -eq $("SQL Server ($InstanceName)")  } | Select-Object -ExpandProperty ServiceAccount
    }
Catch
    {
    log -message "Error" -Level Error
    Throw
    }
}

function Get-AuthenticationScheme ([string]$Instance)
{
Try
    {
    $AuthCheckSQL = "select auth_scheme from sys.dm_exec_connections where session_id = @@SPID"
    Write-Host $AuthCheckSQL
    $AuthCheck = Invoke-Sqlcmd -ServerInstance $Instance -Query $AuthCheckSQL -AbortOnError
    $AuthCheck | Select-Object -ExpandProperty auth_scheme
    }
Catch
    {
    Throw
    }
}

function Get-PortForSQLInstance ([string]$Instance)
{
Try
    {
    $arrInstance = $Instance.Split("\")
    $hostname = $arrInstance[0]
    Write-Host $hostname
    if ($arrInstance.Count -eq 1)
        {
        write-host "count 1"
        $InstanceName = "MSSQLSERVER"
        }
    Else
        {
        $InstanceName = $arrInstance[1]
        }
    log -message "Instance = $Instance InstanceName = $InstanceName"
    $uri ="ManagedComputer[@Name='$hostname']/ ServerInstance[@Name='$instanceName']/ServerProtocol[@Name='Tcp']"
    $mc1 = New-Object -TypeName Microsoft.SqlServer.Management.Smo.Wmi.ManagedComputer $hostname
    $IPAddressPropertiesforIPAll = ($mc1.GetSmoObject($uri).ipaddresses | Where-Object {$_.name -eq "IPAll" } ).IPAddressProperties | Where-Object {$_.Name -eq "TcpPort"}
    $port = $IPAddressPropertiesforIPAll.Value
    $port
    }
Catch
    {log -message "Error" -Level Error ; throw }
}

function log ([string]$message, [ValidateSet("Info","Error","Warning")][string]$Level = "Info", [switch]$WriteToHost, [ConsoleColor]$ForegroundColour = "White", [string]$Note)
{
Try
    {
    If ($WriteToHost)
        {
        Write-Host $message -ForegroundColor $ForegroundColour
        }
    $message = $message -replace "`'" , ""
    If ($Level-eq "Error") { $errorDetails = $($Error[0] | Out-String) -replace "`'", ""}
    $errordetails = $errorDetails -replace "`[$]", "X"
    $errorLine = $($Error[0].InvocationInfo.Line | Out-String) -replace "`'", ""
    $errorLine = $errorLine -replace "`[$]", "X"
    $ErrorLineNumber = $($Error[0].InvocationInfo.ScriptLineNumber)
    $FunctionName = $($(Get-Variable MyInvocation -Scope 1).Value.MyCommand.Name)
    $scriptName = $MyInvocation.ScriptName
    if ($Level -eq "Error")
        {
        [string]$logQuery = "EXEC sp_log @Message = '" + $message + "', @ScriptName = '" + $scriptName + "', @FunctionName = '" + $FunctionName + "', @Level = '" + "Error" + "', @ExecutionId = '" + $ExecutionID + "', @Errordetails = '" + $errorDetails + "', @ErrorLineNumber = '" + $ErrorLineNumber + "', @ErrorLine = '" + $errorLine + "', @ScriptLineNumber ='" + $MyInvocation.ScriptLineNumber + "' , @Note = '" + $note + "'"
        }
    else 
        {
        [string]$logQuery = "EXEC sp_log @Message = '" + $message + "', @ScriptName = '" + $MyInvocation.ScriptName + "', @FunctionName = '" + $($(Get-Variable MyInvocation -Scope 1).Value.MyCommand.Name) + "', @Level = '" + "Info" + "', @ExecutionId = '" + $ExecutionID + "', @ScriptLineNumber ='" + $MyInvocation.ScriptLineNumber + "' , @Note = '" + $Note + "'"
        }
    If ($loggingEnabled)
        {
        Invoke-Sqlcmd -ServerInstance $dbainstance -Database $dbadatabase -Query $logQuery 
        }
    } Catch {Write-Host "Error in log" -ForegroundColor Red ; Throw }
}

function Get-SQLInstances ([string]$SQLInstance)
{
$instancesSQL = @"
SELECT si.InstanceId, si.InstanceName, si.Online, si.ConnectTo, s.Site FROM SQLInstances si
LEFT OUTER JOIN Clusters c on si.ClusterId = c.ClusterId
LEFT OUTER JOIN ClusterHosts ch on ch.ClusterID = c.ClusterId
LEFT OUTER JOIN Hosts h on h.HostId = ch.HostID
LEFT OUTER JOIN Sites s on s.SiteId = h.SiteId
WHERE InstanceName LIKE '%$($SQLInstance)%' AND Online = 1
"@
$Instances = Invoke-Sqlcmd -ServerInstance $DBAInstance -Database $DBADatabase -Query $instancesSQL
$instances
}

function Get-SQLDatabases ([string]$SQLInstance, [string]$Database)
{
$DatabasesSQL = "select si.InstanceName, d.DatabaseName, d.state_desc, si.instanceId, d.Databaseid from SQLInstances SI JOIN Databases D ON d.InstanceId = si.InstanceId where d.DatabaseName LIKE '%$($database)%' AND si.InstanceName LIKE '%$($SQLInstance)%' AND d.DatabaseName != 'DBAdmin' AND d.Active = 1 AND d.State_desc = 'ONLINE'"
Write-Host $DatabasesSQL
$Databases = Invoke-Sqlcmd -ServerInstance $DBAInstance -Database $DBADatabase -Query $DatabasesSQL
$Databases
}

function Get-LastdattimeValues ($instance, $Database, [switch]$GetLatest)
{
$q = @"
IF OBJECT_ID('tempdb..#Temp') IS NOT NULL DROP TABLE #Temp
CREATE TABLE #Temp (LastDate datetime, SchemaName sysname, TableName sysname, ColName sysname)

DECLARE @Statement nvarchar(500)
declare @SchemaName SYSNAME
DECLARE @TableName sysname
DECLARE @ColName SYSNAME
DECLARE Columns CURSOR FOR SELECT 'INSERT INTO #Temp SELECT TOP 1 [' + c.name + '] , ''' + SCHEMA_NAME(t.schema_id) + ''' AS SchemaName, ''' + t.name + ''' AS TableName, ''' + c.name + ''' AS ColName FROM [' + SCHEMA_NAME(t.schema_id) + '].[' + OBJECT_NAME(t.Object_id)  + '] WHERE [' + c.name + '] IS NOT NULL and [' +  c.name + '] < getdate() ORDER BY [' + c.name + '] DESC' AS Statement,SCHEMA_NAME(t.schema_id) as SchemaName, t.name, c.name FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE c.user_type_id = 61 AND t.is_ms_shipped = 0
OPEN Columns
FETCH NEXT FROM Columns INTO @STatement, @SchemaName, @TableName, @ColName
WHILE @@FETCH_STATUS = 0
BEGIN
	PRINT @statement
	EXEC sp_executesql @statement
	FETCH NEXT FROM Columns INTO @STatement, @SchemaName, @TableName, @ColName
END
DEALLOCATE Columns
"@
if ($GetLatest)
    {
    $q = $q + " select top 1 LastDate from #temp order by lastdate DESC"
    }
Else
    {
    $q = $q + " select  * from #temp order by lastdate desc"
    }
Invoke-Sqlcmd -ServerInstance $instance -Database $Database -Query $Q
}

function Get-SQLTraces
{
$Traces = @()
$SQLInstances = Get-SQLInstances | ? {$_.instanceName -notlike "*AZ-*" }
foreach ($Instance in $SQLInstances)
    {
    Write-Host "looking at $($instance.InstanceName)"
    $TraceSQL = "select $($Instance.InstanceId) AS InstanceId, '$($Instance.InstanceName)' AS InstanceName, GETDATE() AS ScanDate, id, status, path, max_size, file_position, start_time, last_event_time, event_count FROM sys.traces WHERE is_default = 0"
    $Trace = Invoke-Sqlcmd -ServerInstance $Instance.ConnectTo -Query $TraceSQL
    $Traces = $traces + $trace
    }
$Traces
}

function Get-LastIndexUpdate ($Instance, $database)
{
$LastSQL = @"
SELECT MAX(lastUpdate) AS LastUpdate, SUM(user_updates) AS Updates, SUM(userSeeks) AS userSeeks, SUM(UserScans) AS UserScans, SUM(UserLookups) AS UserLookups,   SUM(userSeeks) + SUM(UserScans) + SUM(UserLookups) AS AllReads, (SELECT sqlserver_start_time FROM sys.dm_os_sys_info) AS StartUpTime
FROM(
SELECT 
t.name AS TableName 
,SUM(User_updates) OVER (PARTITION BY i.object_id, i.index_id) AS user_updates
,SUM(ius.user_seeks) OVER(PARTITION BY i.object_id, i.index_id) AS userSeeks
,SUM(ius.user_scans)  OVER(PARTITION By i.object_id, i.index_id) AS UserScans
,SUM(ius.user_lookups)  OVER(PARTITION BY i.object_id, i.index_id) AS UserLookups
,MAX(ius.last_user_update)  OVER(PARTITION BY i.object_id, i.index_id) AS LastUpdate
FROM sys.indexes i 
LEFT OUTER JOIN sys.dm_db_index_usage_stats ius ON ius.index_id = i.index_id AND i.object_id = ius.object_id
JOIN sys.tables t ON t.object_id = i.object_id
WHERE t.is_ms_shipped = 0-- AND i.type = 0 
AND database_id = DB_ID()
) d
"@
Invoke-Sqlcmd -ServerInstance $Instance -Database $database -Query $LastSQL


}

function Get-LastIndexUpdateForDatabses ($instance)
{
$dbs = Get-SQLDatabases -SQLInstance $instance
foreach ($db in $dbs)
{
$ius = $(Get-LastIndexUpdate -Instance $db.InstanceName -database $db.Databasename)
$LastDatetimeUpdate = $(Get-LastdattimeValues -instance $db.InstanceName -Database $db.DatabaseName -GetLatest).LastDate 
$UpdateSQL = @"

IF EXISTS (SELECT databaseid FROM DatabaseLastUpdates WHERE databaseId = $($db.DatabaseId))
BEGIN
UPDATE DatabaseLastUpdates SET LastIndexUpdate = '$($LastIndexUpdate)' , LastDatetimeValue = '$($LastDatetimeUpdate)', AllReads = '$($ius.AllReads)', Updates = '$($ius.Updates)', StartupTime = '$($ius.StartupTime)' WHERE databaseid = $($db.DatabaseId)
END
ELSE
BEGIN
INSERT INTO DatabaseLastUpdates (DatabaseId, LastIndexUpdate, LastDatetimeValue, AllReads, Updates, StartupTime) VALUES ($($db.DatabaseId), '$($LastIndexUpdate)', '$($LastDatetimeUpdate)', '$($ius.AllReads)', '$($ius.Updates)', '$($ius.StartupTime)')
END
"@

Write-Host $UpdateSQL
Invoke-Sqlcmd -ServerInstance $DBAInstance  -Database $DBADatabase -Query $UpdateSQL 
}
}

Function Get-LoginCreationCommandsForDatabase ($instance, $database)
{
Try
    {
    CheckAndCreatesp_hexidecimalProc -instance $instance
    $loginsSQL = @"
       IF OBJECT_ID('tempdb..#Logins') IS NOT NULL DROP TABLE #Logins
DECLARE @login_name sysname --= NULL

DECLARE @name sysname
DECLARE @type varchar (1)
DECLARE @hasaccess int
DECLARE @denylogin int
DECLARE @is_disabled int
DECLARE @PWD_varbinary  varbinary (256)
DECLARE @PWD_string  varchar (514)
DECLARE @SID_varbinary varbinary (85)
DECLARE @SID_string varchar (514)
DECLARE @tmpstr  varchar (1024)
DECLARE @is_policy_checked varchar (3)
DECLARE @is_expiration_checked varchar (3)

DECLARE @defaultdb sysname

CREATE TABLE #Logins
(
Login sysname,
CreateString nvarchar(max),
T nvarchar(1)
)

IF (@login_name IS NULL)
  DECLARE login_curs CURSOR FOR

        SELECT p.sid, p.name, p.type, p.is_disabled, p.default_database_name, l.hasaccess, l.denylogin FROM 
sys.server_principals p LEFT JOIN sys.syslogins l
      ON ( l.name = p.name ) 
JOIN sys.database_principals dp ON dp.sid = p.sid
WHERE p.type IN ( 'S', 'G', 'U' ) AND p.name <> 'sa'

ELSE
  DECLARE login_curs CURSOR FOR

      SELECT p.sid, p.name, p.type, p.is_disabled, p.default_database_name, l.hasaccess, l.denylogin FROM 
sys.server_principals p LEFT JOIN sys.syslogins l
      ON ( l.name = p.name ) WHERE p.type IN ( 'S', 'G', 'U' ) AND p.name = @login_name
OPEN login_curs

FETCH NEXT FROM login_curs INTO @SID_varbinary, @name, @type, @is_disabled, @defaultdb, @hasaccess, @denylogin
IF (@@fetch_status = -1)
BEGIN
  PRINT 'No login(s) found.'
  CLOSE login_curs
  DEALLOCATE login_curs
  --RETURN -1
END
SET @tmpstr = '/* sp_help_revlogin script '
PRINT @tmpstr
SET @tmpstr = '** Generated ' + CONVERT (varchar, GETDATE()) + ' on ' + @@SERVERNAME + ' */'
PRINT @tmpstr
PRINT ''
WHILE (@@fetch_status <> -1)
BEGIN
  IF (@@fetch_status <> -2)
  BEGIN
    PRINT ''
    SET @tmpstr = '-- Login: ' + @name
    PRINT @tmpstr
    IF (@type IN ( 'G', 'U'))
    BEGIN -- NT authenticated account/group

      SET @tmpstr = 'CREATE LOGIN ' + QUOTENAME( @name ) + ' FROM WINDOWS WITH DEFAULT_DATABASE = [' + @defaultdb + ']'
    END
    ELSE BEGIN -- SQL Server authentication
        -- obtain password and sid
            SET @PWD_varbinary = CAST( LOGINPROPERTY( @name, 'PasswordHash' ) AS varbinary (256) )
        EXEC sp_hexadecimal @PWD_varbinary, @PWD_string OUT
        EXEC sp_hexadecimal @SID_varbinary,@SID_string OUT

        -- obtain password policy state
        SELECT @is_policy_checked = CASE is_policy_checked WHEN 1 THEN 'ON' WHEN 0 THEN 'OFF' ELSE NULL END FROM sys.sql_logins WHERE name = @name
        SELECT @is_expiration_checked = CASE is_expiration_checked WHEN 1 THEN 'ON' WHEN 0 THEN 'OFF' ELSE NULL END FROM sys.sql_logins WHERE name = @name

            SET @tmpstr = 'CREATE LOGIN ' + QUOTENAME( @name ) + ' WITH PASSWORD = ' + @PWD_string + ' HASHED, SID = ' + @SID_string + ', DEFAULT_DATABASE = [' + @defaultdb + ']'

        IF ( @is_policy_checked IS NOT NULL )
        BEGIN
          SET @tmpstr = @tmpstr + ', CHECK_POLICY = ' + @is_policy_checked
        END
        IF ( @is_expiration_checked IS NOT NULL )
        BEGIN
          SET @tmpstr = @tmpstr + ', CHECK_EXPIRATION = ' + @is_expiration_checked
        END
    END
    IF (@denylogin = 1)
    BEGIN -- login is denied access
      SET @tmpstr = @tmpstr + '; DENY CONNECT SQL TO ' + QUOTENAME( @name )
    END
    ELSE IF (@hasaccess = 0)
    BEGIN -- login exists but does not have access
      SET @tmpstr = @tmpstr + '; REVOKE CONNECT SQL TO ' + QUOTENAME( @name )
    END
    IF (@is_disabled = 1)
    BEGIN -- login is disabled
      SET @tmpstr = @tmpstr + '; ALTER LOGIN ' + QUOTENAME( @name ) + ' DISABLE'
    END
    PRINT @tmpstr
	INSERT INTO #Logins (Login, CreateString, T) VALUES (@name, @tmpstr, @type)
  END

  FETCH NEXT FROM login_curs INTO @SID_varbinary, @name, @type, @is_disabled, @defaultdb, @hasaccess, @denylogin
   END
CLOSE login_curs
DEALLOCATE login_curs
--RETURN 0

SELECT * from #Logins
"@
    Invoke-Sqlcmd -ServerInstance $instance -Database $database -Query $loginsSQL
    }
Catch { write-host "error" ; Throw }
}#Get-LoginCreationCommandsForDatabase

function CheckAndCreatesp_hexidecimalProc ([string]$instance){
Try
{
    $procexists = $(Invoke-Sqlcmd -ServerInstance $instance -Database master -Query "IF OBJECT_ID ('sp_hexadecimal') IS  NULL SELECT 0 AS P ELSE SELECT 1 AS P").P
    If ($procexists -eq "0")
            {
        $CreateProc = @"
        CREATE PROCEDURE sp_hexadecimal
            @binvalue varbinary(256),
            @hexvalue varchar (514) OUTPUT
        AS
        DECLARE @charvalue varchar (514)
        DECLARE @i int
        DECLARE @length int
        DECLARE @hexstring char(16)
        SELECT @charvalue = '0x'
        SELECT @i = 1
        SELECT @length = DATALENGTH (@binvalue)
        SELECT @hexstring = '0123456789ABCDEF'
        WHILE (@i <= @length)
        BEGIN
            DECLARE @tempint int
            DECLARE @firstint int
            DECLARE @secondint int
            SELECT @tempint = CONVERT(int, SUBSTRING(@binvalue,@i,1))
            SELECT @firstint = FLOOR(@tempint/16)
            SELECT @secondint = @tempint - (@firstint*16)
            SELECT @charvalue = @charvalue +
            SUBSTRING(@hexstring, @firstint+1, 1) +
            SUBSTRING(@hexstring, @secondint+1, 1)
            SELECT @i = @i + 1
        END

        SELECT @hexvalue = @charvalue
        GO
"@
        Invoke-Sqlcmd -ServerInstance $instance -Database master -Query $CreateProc
            }
} Catch { Write-Host "Error" ; Throw }
}

function Write-DataTable 
{ 
    [CmdletBinding()] 
    param( 
    [Parameter(Position=0, Mandatory=$true)] [string]$ServerInstance, 
    [Parameter(Position=1, Mandatory=$true)] [string]$Database, 
    [Parameter(Position=2, Mandatory=$true)] [string]$TableName, 
    [Parameter(Position=3, Mandatory=$true)] $Data, 
    [Parameter(Position=4, Mandatory=$false)] [string]$Username, 
    [Parameter(Position=5, Mandatory=$false)] [string]$Password, 
    [Parameter(Position=6, Mandatory=$false)] [Int32]$BatchSize=50000, 
    [Parameter(Position=7, Mandatory=$false)] [Int32]$QueryTimeout=0, 
    [Parameter(Position=8, Mandatory=$false)] [Int32]$ConnectionTimeout=15 
    ) 
     
    $conn=new-object System.Data.SqlClient.SQLConnection 
 
    if ($Username) 
    { $ConnectionString = "Server={0};Database={1};User ID={2};Password={3};Trusted_Connection=False;Connect Timeout={4}" -f $ServerInstance,$Database,$Username,$Password,$ConnectionTimeout } 
    else 
    { $ConnectionString = "Server={0};Database={1};Integrated Security=True;Connect Timeout={2}" -f $ServerInstance,$Database,$ConnectionTimeout } 
 
    $conn.ConnectionString=$ConnectionString 
 
    try 
    { 
        $conn.Open() 
        $bulkCopy = new-object ("Data.SqlClient.SqlBulkCopy") $connectionString 
        $bulkCopy.DestinationTableName = $TableName
        $bulkCopy.BatchSize = $BatchSize 
        $bulkCopy.BulkCopyTimeout = $QueryTimeOut 
        $bulkCopy.WriteToServer($Data) 
        $conn.Close() 
    } 
    catch 
    { 
        $ex = $_.Exception 
        Write-Error "$ex.Message" 
        continue 
    } 
 
} #Write-DataTable

function Restart-Trace ($instance)
{
$TraceSQL = "SELECT id, status, path from sys.traces"
$trace = Invoke-Sqlcmd -ServerInstance $Instance -Query $TraceSQL
$targetTrace = $trace | ? {$_.path -like "*d:\Trace*" }
$targetTrace
$stopTraceSQL = "EXEC sp_trace_setstatus $($targetTrace.id) , 0"
$stopTrace = Invoke-Sqlcmd -ServerInstance $Instance -Query $stopTraceSQL
#get trace into table
$TraceTableSQL = "SELECT * FROM fn_trace_gettable('$($targetTrace.path)',default)"
$TraceTable = Invoke-Sqlcmd -ServerInstance $Instance -Query $TraceTableSQL
$TraceTable | select Servername, DatabaseName, ApplicationName, StartTime, EndTime, HostName, LoginName, textData
#Write-DataTable -ServerInstance $DBAInstance -Database $DBADatabase -TableName Trace -Data $TraceTable | select Servername, DatabaseName, ApplicationName, StartTime, EndTime, HostName, LoginName, textData

foreach ($traceLine in $TraceTable | select Servername, DatabaseName, ApplicationName, StartTime, EndTime, HostName, LoginName, textData)
{
$traceLine.TextData.GetType().Name -eq "DBNull"
$textData = $null
if ($traceLine.TextData.GetType().Name -Ne "DBNull")
    {
    $textData = $traceLine.TextData.Replace("'","''")
    }
$insertSQL= "INSERT INTO Trace (Servername, DatabaseName, ApplicationName, StartTime, EndTime, HostName, LoginName, textData) VALUES ('$($traceLine.ServerName)','$($traceLine.DatabaseName)','$($traceLine.ApplicationName)','$($traceLine.StartTime)','$($traceLine.EndTime)','$($traceLine.HostName)','$($traceLine.LoginName)','$($textData)')"
Write-Host $insertSQL
$insert = Invoke-Sqlcmd -ServerInstance $DBAInstance -Database $DBADatabase -Query $insertSQL
}

$startTraceSQL = "EXEC sp_trace_setstatus $($targetTrace.id) , 1"

Write-Host $($targetTrace.id)
Write-Host $startTraceSQL
$startTrace = Invoke-Sqlcmd -ServerInstance $Instance -Query $startTraceSQL
}

function BackupAndRestore (
[string]$SourceInstance, 
[string]$SourceDatabase, 
[string]$TargetInstance, 
[string]$TargetDatabase, 
[bool]$TakeTargetOffline = $false,
[bool]$DontBackupTarget = $false,
[bool]$RetainOwnerName = $false,
[Parameter(Mandatory=$true)][ValidateSet('SIMPLE','FULL','Retain')][string]$RecoveryModel,
[bool]$OverwriteTarget = $false,
[Parameter(Mandatory=$true)][ValidateSet('SetLatest','Retain','100','110','120','130','140')][string]$CompatabilityLevel,
[bool]$ShrinkLog = $false,
[bool]$WaitforManualRestore = $false,
[bool]$CreateDatabase = $false,
[bool]$MarkAsRetain = $false,
[bool]$NoRecovery = $false,
[bool]$copyUserRoles = $false,
[Bool]$CreateLoginsIfTheyDontExist,
[string]$IntermediateInstance,
[bool]$Differential = $false,
[bool]$BatchMode,
[string]$EmailAddress,
[string]$FromAddress,
[string]$BackupPath,
[bool]$CopyOnly = $true,
[string]$ScriptToRunOnTarget,
[switch]$ChangeCOllation,
[ValidateSet("Latin1_General_CI_AS")][string]$Collation,
[switch]$DontCheckSpace,
[switch]$NoDBCC,
[switch]$UpdateStats
)
{
#log -message "before try TakeSourceOffline = $TakeSourceOffline" -Level Info -WriteToHost -ForegroundColour Yellow
<#
.Synopsis
   Takes a backup of a database and restores it elsewhere, on the same or remote instance.
.DESCRIPTION
   
.Parameter
SourceDatabase
The name of the database to be backed up.

.EXAMPLE
example
#>
Try
    {
    $BackupAndRestoreStartTime = Get-Date
    $ExecutionID = $([GUID]::newGuid()).Guid

    Log "Starting"
    Log "Differential is $Differential"
    $jobs = @() #used for the progress routine
    Try
        {
        If ($EmailAddress -EQ "")
            {
            $me = $env:USERNAME
            $EmailAddress = $(Get-ADUser -Filter {samaccountname -like $me} -Properties EmailAddress | Select-Object Emailaddress).EmailAddress
            }
        } Catch { Log -message "Error getting email address." -Level Error ; throw }
    #Validate params
    If ($WaitforManualRestore -and $Differential) 
        {
        Log "you cant specify WaitforManualRestore and Differential" "Error"
        Throw "Invalid parameters"
        }
    If ($IntermediateInstance -and $Differential)
        {
        Log "you cant specify IntermediateInstance and Differential" "Error"
        Throw "Invalid parameters"
        }
    
    If ($(Get-DatabaseState -instance $SourceInstance -database $SourceDatabase) -ne "ONLINE")
        {
        Log "The source database must be online" "Error"
        Throw "Source database not online"
        }

    If ($WaitforManualRestore -or $CreateDatabase) 
        {
        $DontBackupTarget = $true
        $CreateDatabase = $true
        }
    If ( ($CreateDatabase -eq $false) -and ($Differential -eq $false) )
        {
        If ( ($(Get-DatabaseState -instance $TargetInstance -database $TargetDatabase) -ne "ONLINE") -and ($CreateDatabase -eq $false) )
            {
            Log "The target database must be online" "Error"
            Throw "Target database not online"
            }
        }
    If ($Differential)
        {
        If ($(Get-DatabaseState -instance $TargetInstance -database $TargetDatabase) -ne "RESTORING")
            {
            Log "-Differential specified and database not in restoring state" "Error"
            Throw "-Differential specified and database not in restoring state"
            }
        $DontBackupTarget = $true
        }

    Log "WaitforManualRestore is $WaitforManualRestore , CreateDatabase is $CreateDatabase , Differential is $Differential"

    If (($WaitforManualRestore -eq $false) -and ( ($CreateDatabase -eq $false) -and ($Differential -eq $false) ) )
        {
        $SourceInternalVersion = Get-SQLDatabaseInternalVersionNumberFromDatabase -instance $SourceInstance -database $SourceDatabase
        $TargetInternalVersion = Get-SQLDatabaseInternalVersionNumberFromDatabase -instance $TargetInstance -database $TargetDatabase
        Log "SourceInternalVersion = $SourceInternalVersion , TargetInternalVersion = $TargetInternalVersion"
        If ($SourceInternalVersion  -gt $TargetInternalVersion)
            {
            Log "The SQL version of the target instance must the the same of greated than the source version." "Error"
            Throw "The SQL version of the target instance must the the same of greated than the source version."
            }
        Else
            {
            Log "Versions seem to be compatible (still need to check if the versions are not too far apart!"
            }
        }
    If ($TargetDatabase -eq "")
        {
        Get-Confirmation -Msg "Target Database not specified.  Will use name of source database." -BatchMode $BatchMode
        Log -message "Target Database not specified, assume source database name ($SourceDatabase)" -Level Info
        $TargetDatabase = $SourceDatabase
        }
    If ($ScriptToRunOnTarget)
        {
        Log -message "ScriptToRunOnTarget specified.  Checking if $ScriptToRunOnTarget exists." -Level Info
        if (Test-Path $ScriptToRunOnTarget)
            {
            Log -message "$ScriptToRunOnTarget exists"
            }
        Else
            {
            $msg = "$ScriptToRunOnTarget not accessible."
            log -message $msg -Level Error -WriteToHost -ForegroundColour Red
            Throw $msg
            }
        }

    If ($ChangeCOllation -and $Collation -eq "")
        {
        $msg = "You must supply the name of the collation to change to in the -Collation parameter"
        log -message $msg -Level Error -WriteToHost -ForegroundColour Red 
        Throw $msg
        }

    Log "Checking databases"
    #Check Databases
    Try { $ver = Get-SQLInstanceVersion -instanceName $TargetInstance } Catch { Log "Target Database $TargetDatabase on $TargetInstance is not available" "Error" ; Throw}
    If ($CreateDatabase -eq $false) 
        {
        Try { $ver = Get-SQLInstanceVersion -instanceName $SourceInstance } Catch { Log "Source Database $TargetDatabase on $TargetInstance is not available" "Error" ; Throw}
        }
    If ($CreateDatabase)
        {
        if (Check-DatabaseAccess -instance $TargetInstance -database $TargetDatabase)
            {
            Log "CreateDatabase was specified by the target exists" "error"
            Throw "CreateDatabase was specified by the target exists"
            }
        }

    Log "Checking connections"
    #Check connections
    If ($TakeTargetOffline -eq $false -and ($CreateDatabase -eq $false) -and ( $Differential -eq $false ) )
        {
        If ($(Get-DatabaseLocks -instancename $TargetInstance -databasename $TargetDatabase) -eq 1)
            {
            Write-Warning "Database is in use.  Specify TakeTargetOffline."
            Log "Database in use, exiting"
            Throw "Database in use"
            }
        }

    #Check AvailabilityGroup
    If (!$CreateDatabase -and !$Differential)
        {
       $ag = CheckIfDatabaseIsInAvailabilityGroup -Instance $TargetInstance -Database $TargetDatabase
        If ($ag)
            {
            $msg = "The database $TargetDatabase on $TargetInstance is part of the $($ag.AGName) availability group.  It cannot be restored"
            Log -message $msg -Level Error -WriteToHost -ForegroundColour Yellow 
            Throw $msg
            }
        }

    #Check encryption
    Try
        {
        If ($(Get-ProductName $SourceInstance) -ge "SQL 2008")
        {
            $sourceEncryption = Get-DatabaseEncryptionFromDatabase -instancename $SourceInstance -databasename $SourceDatabase
            If ($sourceEncryption.encryption_state -eq 3)
                {
                Log "Source is encrypted"
                Log $sourceEncryption.Key_algorithm
                Log $sourceEncryption.key_length
                Log $sourceEncryption.encryptor_thumbprint
                $certsql = "select len(thumbprint), CONVERT(varchar(100), thumbprint,2),* from sys.certificates where CONVERT(varchar(100), thumbprint,2) = '$($sourceEncryption.encryptor_thumbprint)'"
                $certResult = Invoke-Sqlcmd -ServerInstance $TargetInstance -Database master -Query $certsql -AbortOnError
                #$certResult
                If ($certResult -eq $null)
                    {
                    Log -message "No certificate found on target with thumbprint $($sourceEncryption.encryptor_thumbprint) " -Level Error -WriteToHost -ForegroundColour Yellow
                    Throw "No certificate found on target with thumbprint $($sourceEncryption.encryptor_thumbprint) "
                    }
                Else
                    {
                    Log -message "Certificate found ($($certResult.name)) on target ($TargetInstance)" -Level Info 
                    }

                If ( ($CreateDatabase -eq $false) -and ($Differential -eq $false) )
                    {
                    $targetEncryption = Get-DatabaseEncryptionFromDatabase -instancename $TargetInstance -databasename $TargetDatabase
                    If ($targetEncryption.encryption_state -eq 3)
                        {
                        Log "Target is encrypted"
                        Log $TargetEncryption.Key_algorithm
                        Log $TargetEncryption.key_length
                        Log $TargetEncryption.encryptor_thumbprint
                        }
                    }
                }
            }
        }
    Catch
        {
        Get-Confirmation -Msg "Cant check encryption." -BatchMode $BatchMode
        Throw
        }

    #Check Compression
    Log "Checking compression"
    If ($(Get-SQLInstanceCompression -instancename $SourceInstance) -and $(Get-SQLInstanceCompression -instancename $TargetInstance ))
        {
        $Compress = $true
        }
        Else
        {
        $Compress = $false
        }
    Log "Compression = $Compress"

    #Check Sizes
    
    If (!$DontCheckSpace -and !$Differential)
        {
        $s = Get-FileListFromDatabase -instance $SourceInstance -Database $SourceDatabase
        $s
        log -message $($s | Out-String) -Level Info
        $SpaceRequired = Get-space -sourcefilelist $s -Instance $TargetInstance -database $TargetDatabase -CreateDatabase $CreateDatabase
        $SpaceRequired
        log -message $($SpaceRequired | Out-String)
        $LowSpaceVolumes = $SpaceRequired | Where-Object {$_.PercentFreeAfter -lt 10}

        If ($LowSpaceVolumes)
            {
            $fullVolumes = $LowSpaceVolumes | Where-Object { $_.PercentFreeAfter -lt 0 }
            If ($fullVolumes)
                {
                Log -message "There is not enough space on the following drives $($LowSpaceVolumes | Format-Table | Out-String).  `nWill exit." -Level Error -WriteToHost
                Throw "There is not enough space on the following drives $($LowSpaceVolumes | Format-Table | Out-String).  Will exit."
                }
            Get-Confirmation -Msg "The following volumes may not have sufficient space. `n`n $($LowSpaceVolumes | Format-Table | Out-String)" -BatchMode $BatchMode
            }
        }

        #log -message "3 TakeSourceOffline = $TakeSourceOffline" -Level Info -WriteToHost -ForegroundColour Yellow

    #If ($TakeSourceOffline -and $($BatchMode -eq $false))
    #    {
   #     Get-Confirmation -Msg "TakeSourceOffline was specified." -BatchMode $BatchMode
  #      }
#

     #Get Source Properties
     Log "RetainOwnerName is $RetainOwnerName"
    If ($RetainOwnerName -eq $true)
        {
        Log "RetainOwner was specified"
        $SourceOwner = Get-SQLDatabaseOwner -instancename $SourceInstance -databasename $SourceDatabase
        Log "SourceOwner is $SourceOwner"
        If ($(LoginExists -instance $TargetInstance -login $SourceOwner) -eq $false)
            {
            Log "RetainOwner was specified, but $SourceOwner does not exist on the target instance" "Error"
            Throw "RetainOwner was specified, but $SourceOwner does not exist on the target instance"
            }
        Log "Source Owner = $SourceOwner"
        }
    If ($CompatabilityLevel -eq "Retain")
        {
        $SourceCompatibilityLevel = Get-SQLDatabaseCompatibilityLevel -instancename $SourceInstance -databasename $SourceDatabase
        Log "SourceCompatibilityLevel = $SourceCompatibilityLevel"
        Check-Compatibility -Instance $TargetInstance -CompatibilityLevel $SourceCompatibilityLevel
        }
    If ($RecoveryModel -eq "Retain")
        {
        $SourceRecoveryModel = Get-SQLDatabaseRecoveryModel -instancename $SourceInstance -databasename $SourceDatabase
        Log "SourceRecoveryModel = $SourceRecoveryModel"
        }
    $SourceTrustworthy = Get-SQLDatabaseTrustworthy -instancename $SourceInstance -databasename $SourceDatabase
    Log "SourceTrustworthy $SourceTrustworthy"

    #Check backup locations
    If ($CreateDatabase -eq $false)
        {
        #Get the targetbackup location and check accessible from target instance
        $TargetBackupLocation = Get-BackupLocation -instancename $TargetInstance -databasename $TargetDatabase -CreateIfNotExist $true -MarkAsRetain $MarkAsRetain -BackupLocation $BackupPath
        if ($(Test-PathOnSQLServer -instance $TargetInstance -path $TargetBackupLocation -TestDirectoryOnly $true) -eq $false)
            {
            $msg = "The  target location ($TargetBackupLocation) is not accesible from the target instance ($TargetInstance)"
            Write-Host $TargetBackupLocation
            Log -message $msg -Level Error
            Throw $msg
            }
        }
    #Check source backup location accessible from source and target instance
    $SourceBackupLocation = Get-BackupLocation -instancename $SourceInstance -databasename $SourceDatabase -CreateIfNotExist $true -MarkAsRetain $MarkAsRetain -differential $Differential -BackupLocation $BackupPath
    if ($(Test-PathOnSQLServer -instance $SourceInstance -path $SourceBackupLocation -TestDirectoryOnly $true) -eq $false)
        {
        $msg = "The source backup location ($SourceBackupLocation) is not accesible from the source instance ($SourceInstance)"
        Log -message $msg -Level Error
        Throw $msg
        }
    If ($IntermediateInstance)
        {
        #Get intermediate location
        $IntermedaiteBackupLocation = Get-BackupLocation -instancename $IntermediateInstance -databasename $TargetInstance -CreateIfNotExist -MarkAsRetain $false -BackupLocation $BackupPath
        #check intermediate location accessible from intermediate and source accesible from intermediate
        If ($(Test-PathOnSQLServer -instance $IntermediateInstance -path $IntermedaiteBackupLocation -TestDirectoryOnly $true) -eq $false)
            {
            $msg = "The Intermediate backup location ($IntermedaiteBackupLocation) is not accesible from the source instance ($IntermediateInstance)"
            Log -message $msg -Level Error
            Throw $msg
            }
        If ($(Test-PathOnSQLServer -instance $IntermediateInstance -path $SourceInstance -TestDirectoryOnly $true) -eq $false)
            {
            $msg = "The Source backup location ($SourceBackupLocation) is not accesible from the Intermediate instance ($IntermediateInstance)"
            Log -message $msg -Level Error
            Throw $msg
            }
        }
    If ( ($CreateDatabase -eq $false) -and ($Differential -eq $false) )
        {
        $originalTargetRoles = Get-SQLUserRoles -instancename $TargetInstance -databasename $TargetDatabase
        }

    $confirmmessage = @"
The destination database ($TargetDatabase on $TargetInstance) $(if ($CreateDatabase) {",which does not currently exist, will be restored "} else {"will be overwritten "})with a backup of the source database ($SourceDatabase on $SourceInstance)."
"@
    Get-Confirmation -Msg $confirmmessage  -BatchMode $BatchMode

    #Backup Databases
    #Source
    
    Log "Source Backup location = $SourceBackupLocation"
    If ($NoRecovery) {$copyonly = $false}
    $newjob = $(Backup-Database -instancename $SourceInstance -databasename $SourceDatabase -backuppath $SourceBackupLocation -ProgressID 1 -compress $Compress -JobName "SourceBackup" -Differential $Differential -CopyOnly $copyonly)
    $jobs = $jobs + $newjob
    
    #Intermediate
    If ($IntermediateInstance)
        {
        $messageBody = "IntermediateInstance was specified.  The backups on the databases are now complete and the intermediate instance now needs to be restored manually. `n`nRestore $TargetDatabase on $IntermediateInstance using file $SourceBackupLocation `n`nReturn to the script when complete and press a key to continue."
        SendEMail -Subject "Restore of $TargetDatabase on $TargetInstance .  Action required." -msg $messageBody -Address $EmailAddress -FromAddress $FromAddress -NoLog $true
        Write-Host $messageBody
        Log "WaitforManualRestore was specified.  Waiting for the the user to press a button."
        Read-Host -Prompt "Press any key to continue when manual restore onto the intermediate server is complete" 
        Log "Input recieved.  Continuing"
        Check-DatabaseAccess -instance $IntermediateInstance -database $TargetDatabase
        Backup-Database -instancename $IntermediateInstance -databasename $TargetDatabase -backuppath $IntermedaiteBackupLocation -compress -ProgressID 7 -JobName BackupIntermediate -CopyOnly
        Progress -Job BackupIntermediate -id 7 -path $IntermedaiteBackupLocation -instance $IntermediateInstance -database $TargetDatabase
        }
    #Target
    If ($DontBackupTarget)
        {
        Log "DontBackupTarget was specified"
        }
    ElseIf  ($Differential)
        {
        Log "Not backing up target as -Differential was specified"
        }
    Else
        {
        
        Log "TargetBackupLocation = $TargetBackupLocation"
        $jobs = $jobs + $(Backup-Database -instancename $TargetInstance -databasename $TargetDatabase -backuppath $TargetBackupLocation -ProgressID 2 -compress $Compress -JobName "TargetBackup" -Differential $Differential)
        }
    Progress2 -Jobdetailscollection $jobs

    If ($WaitforManualRestore)
        {
        Log "WaitforManualRestore was specified"
                    $messageTo = "$($env:USERNAME)@BOURNE-LEISURE.CO.UK"
            $messageTo
            $messageFrom = $messageTo = "$($env:USERNAME)@BOURNE-LEISURE.CO.UK"
            $messageBody = "WaitForManualRestore was specified.  The backups on the databases are now complete and the target now needs to be restored manually. Restore the database onto the target server using the backup file $SourceBackupLocation . `n`n The target server is $TargetInstance and the database name should be $TargetDatabase `n`nReturn to the script when complete and press a key to continue."
            Send-MailMessage -SmtpServer $smtpserver -From $messageFrom -To $messageTo -Subject "Restore of $TargetDatabase on $TargetInstance .  Action required." -Body $messageBody 
            Write-Host "WaitforManualRestore was specified."
            Write-Host "Restore the database onto the target server using the backup file $SourceBackupLocation"
            Write-Host "The target server is $TargetInstance and the database name should be $TargetDatabase"
            Log "WaitforManualRestore was specified.  Waiting for the the user to press a button."
            Read-Host -Prompt "Press any key to continue when manual restore is complete" 
            Log "Input recieved.  Continuing"
        }
    Else
        {
        $AmendedSourceBackupLocation = $(If ($IntermediateInstance) {$IntermedaiteBackupLocation} Else { $SourceBackupLocation})
        Log "Restoring target database $TargetDatabase on $TargetInstance from $SourceBackupLocation"
        Log "Differential = $Differential"
        $newjob = $(Restore-SQLDatabase -instancename $TargetInstance -databasename $TargetDatabase -TakeInstanceOffline $TakeTargetOffline  -backuppath $AmendedSourceBackupLocation -JobName "RestoreTarget" -NoRecovery $NoRecovery -CreateDatabase $CreateDatabase -sourceDatabase $SourceDatabase -differential $Differential)
        $restorejobs = @()
        $restorejobs = $restorejobs + $newjob
        Progress2 -Jobdetailscollection $restorejobs
        Log "Restore finished"
        }

    If ($NoRecovery -eq $false)
        {

        if ($ChangeCOllation)
            {
            CHange-Collation -Instance $TargetInstance -Database $TargetDatabase -Collation $Collation
            }

        If ($CreateLoginsIfTheyDontExist)
            {
            Log "Will create logins"
            Create-Logins -SourceInstance $SourceInstance -SourceDatabase $SourceDatabase -TargetInstance $TargetInstance -TargetDatabase $TargetDatabase
            }

        If ($ShrinkLog) 
            { Log "Shrinking transaction log on $TargetDatabase on $TargetInstance" 
            ShrinkLog -instancename $TargetInstance -databasename $TargetDatabase 
            } 
        Else 
            { Log "ShrinkLog not specified"}
        If ($copyUserRoles) 
            {Log "Writing user roles"
            Write-SQLUserRoles -instancename $TargetInstance -databasename $TargetDatabase -roles $originalTargetRoles 
            } 
        Else 
            { 
            Log "CopyUserRoles not specified" 
            }
        If ($SourceTrustworthy -eq 1) 
            {
            Write-SQLDatabaseTrustworthy -instancename $TargetInstance -databasename $TargetDatabase -trustworthy 1
            }
        If ($RetainOwnerName)
            {
            Write-SQLDatabaseOwner -instancename $TargetInstance -databasename $TargetDatabase -owner $SourceOwner
            }
        Else
            {
            Write-SQLDatabaseOwner -instancename $TargetInstance -databasename $TargetDatabase -owner "sa"
            }
        If ($CompatabilityLevel -eq "SetLatest")
            {
            Log "Setting compat = latest"
            $TargetLatestCompat = Get-SQLInstanceLatestSupportedCompatibilityLevel -instancename $TargetInstance 
            Log "Latest compat = $TargetLatestCompat"
            Write-SQLDatabaseCompatibilityLevel -instancename $TargetInstance -databasename $TargetDatabase -compatibilityLevel $TargetLatestCompat
            }
        Elseif ($CompatabilityLevel -eq "Retain")
            {
            Write-SQLDatabaseCompatibilityLevel -instancename $TargetInstance  -databasename $TargetDatabase -compatibilityLevel $SourceCompatibilityLevel
            }
        Else
            {
            Write-SQLDatabaseCompatibilityLevel -instancename $TargetInstance -databasename $TargetDatabase -compatibilityLevel $CompatabilityLevel
            }
        }    
    If (!$NoRecovery -and !$NoDBCC)
        {
        If (Run-DBCCCHECKDB -Instance $TargetInstance -Database $TargetDatabase)
            {
            Log "DBCC CHECKDB detected errors." -Level Error -WriteToHost -ForegroundColour Red -Note "DBCC"
            Throw "DBCC CHECKDB detected errors."
            }
        }
    If (!$NoRecovery -and $UpdateStats)
        {
        Log -message "Running update stats" -Level Info -WriteToHost 
        Invoke-Sqlcmd -ServerInstance $TargetInstance -Database $TargetDatabase -Query "EXEC sp_updatestats"
        }

    If (!$NoRecovery)
        {
        Log -message "Setting PAGE_VERIFY CHECKSUM" -Level Info -WriteToHost
        Invoke-Sqlcmd -ServerInstance $TargetInstance -Database $TargetDatabase -Query "ALTER DATABASE [$TargetDatabase] SET PAGE_VERIFY CHECKSUM"
        }

    #Check restored database
    If ($NoRecovery -eq $false)
    {
        #Check orphaned users
        $orph = ListOrphanedUsers -InstanceName $TargetInstance -DatabaseName $TargetDatabase
        If ($orph -eq $null)
            {
            Log -message "There are no orphaned users on $TargetDatabase on $TargetInstance" -Level Info
            }
        Else
            {
            Log -message "The following orphaned users were found on $TargetDatabase on $TargetInstance :`n`n$($orph | Format-Table | Out-String)" -Level Warning -WriteToHost -ForegroundColour Yellow
            }

        #Check users with no logins

        $userswithnologins = ListUsersWithNoLogins -InstanceName $TargetInstance -DatabaseName $TargetDatabase
        If ($userswithnologins -eq $null)
            {
            $NoLoginsSTring = "There are no  users without logins on $TargetDatabase on $TargetInstance"
            Log -message $NoLoginsSTring -Level Info
            }
        Else
            {
            $NoLoginsSTring = "The following  users without logins were found on $TargetDatabase on $TargetInstance :`n`n$($userswithnologins | Format-Table | Out-String)"
            Log -message $NoLoginsSTring -Level Warning -WriteToHost -ForegroundColour Yellow
            }
       }

    $mailsubject = "Restore of $TargetDatabase on $TargetInstance has completed successfully"
    If ($NoRecovery)
        {
        $recoveryMessage = "`n`nThe database remains in a RESTORING state to allow further differential backups to be applied.  Use the -Differential switch to apply."
        }
    Else 
        {
        $recoveryMessage = ""
        }

    If ($DontBackupTarget -eq $false) {
        $mailmessage = "Backup of target database is available at $TargetBackupLocation `n`n $recoveryMessage `n`n $($MyInvocation.BoundParameters | Out-String)"} 
    Else 
        {
        $mailmessage = "Parameters used: `n`n $($MyInvocation.BoundParameters | Out-String)"
        }

    If ($ScriptToRunOnTarget)
        {
        Log -message "Running $ScriptToRunOnTarget on $TargetDatabase on $TargetInstance" -Level Info
        Invoke-Sqlcmd -ServerInstance $TargetInstance -Database $TargetDatabase -InputFile $ScriptToRunOnTarget
        }

    #If ($TakeSourceOffline)
    #    {
    #    Log -message "Taking source offline." -Level Info -WriteToHost -ForegroundColour Yellow
    #    Invoke-Sqlcmd -ServerInstance $SourceInstance -Database master -Query "ALTER DATABASE $SourceDatabase SET OFFLINE WITH ROLLBACK IMMEDIATE"
    #    Log -message "Source has been set offline." -Level Info
    #    }
    
    $BackupAndRestoreEndTime = get-date
    $runtime = New-TimeSpan -Start $BackupAndRestoreStartTime -End $BackupAndRestoreEndTime
    $ElapsedString = $(“Elapsed Time: {0}:{1}:{2}” -f $RunTime.Hours,$Runtime.Minutes,$RunTime.Seconds)
    log -message $ElapsedString -Level Info -WriteToHost
    $mailmessage = "$ElapsedString`n" + $mailmessage
    $mailmessage = $mailmessage + $NoLoginsSTring

    }
Catch
    {
    Log "Error.  Final catch" "Error"
    $stacktrace = $Error[0].ScriptStackTrace
    $errorDetails = "ERROR: `n`n STACK TRACE: `n`n $stacktrace `n`n ERROR DETAILS: `n`n"
    $errorDetails = $errorDetails + $($Error[0] | Out-String) -replace "`'", ""
    $errordetails = $errorDetails -replace "`[$]", "X"
    $mailmessage =  "Parameters used: `n`n $($MyInvocation.BoundParameters | Out-String)`n`n" + $errorDetails
    $mailsubject = "ERROR in Backup and Restore of $TargetDatabase on $TargetInstance" 
    write-host $errorDetails -ForegroundColor Red
    }
Finally
    {
     #(Get-Job | ? {$_.State -eq "Failed"}).ChildJobs[0].JobStateInfo.Reason
    Get-Job | Remove-Job
    If ($mailsubject -ne $null)
        {
        SendEMail -Subject $mailsubject -msg $mailmessage -Address $EmailAddress -FromAddress $FromAddress
        }
    Log "Finished"
    }
}

function Get-DatabaseState ($instance, $database)
{
$DatabaseStateSQL = "SELECT state_desc from sys.databases where name = '$($database)'"
$DatabaseState = Invoke-Sqlcmd -ServerInstance $instance -Query $DatabaseStateSQL
$DatabaseState.state_desc
}

Function Get-SQLDatabaseInternalVersionNumberFromDatabase ($instance, $database)
{
Try
    {
    $InternalVersionNumberFromDatabaseSQL = "SELECT DATABASEPROPERTYEX('$database','Version') AS InternalVersion"
    $InternalVersionNumberFromDatabase = $(Invoke-Sqlcmd -ServerInstance $instance -Database $database -Query $InternalVersionNumberFromDatabaseSQL).InternalVersion
    $InternalVersionNumberFromDatabase
    } Catch { Log "Error getting internal version" -Level Error; Throw }
}

function Get-Confirmation ([string]$Msg, [bool]$BatchMode)
{
Log -message "USER PROMPT: $Msg" -Level Info
Write-Host $Msg -ForegroundColor Green
If ($BatchMode)
    {
    Write-Host "BatchMode specified, so continuing" -ForegroundColor Yellow
    }
Else
    {
    $Response = Read-Host "Do you wish to continue? (Y/N)"
    If ($Response -ne "Y")
        {
        Log -message "Exiting due to user response." -Level Info -WriteToHost
        exit
        }
    }
}

function Get-SQLInstanceVersion ($instanceName)
    {
    $ver = $(Invoke-Sqlcmd -ServerInstance $instanceName -Database master -Query "SELECT @@VERSION AS Version").Version
    $ver
    }

function Get-ProductName ([string]$Instance)
{
Try
    {
    $ProductNameSQL = @"
DECLARE @sqlVers numeric(4,2)
SELECT @sqlVers = left(cast(serverproperty('productversion') as varchar), 4)
SELECT SERVERPROPERTY('productversion') AS Version
,SERVERPROPERTY('productlevel') AS ServicePacklevel
,SERVERPROPERTY('edition') AS Edition
,CASE @sqlVers WHEN '8.00' THEN 'SQL 2000' WHEN '9.00' THEN 'SQL 2005' WHEN '10.00' THEN 'SQL 2008' WHEN '10.50' THEN 'SQL 2008 R2' WHEN '11.00' THEN 'SQL 2012' WHEN '12.00' THEN 'SQL 2014' WHEN '13.00' THEN 'SQL 2016'  WHEN '14.00' THEN 'SQL 2017' END AS	ProductName
"@
    $ProductNameResult = Invoke-Sqlcmd -ServerInstance $Instance -Query $ProductNameSQL -AbortOnError
    $ProductNameResult.ProductName
    } Catch { Log -message "Error getting product name" -Level Error -WriteToHost ; throw }
}

Function Get-SQLDatabaseInternalVersionNumberFromDatabase ($instance, $database)
{
Try
    {
    $InternalVersionNumberFromDatabaseSQL = "SELECT DATABASEPROPERTYEX('$database','Version') AS InternalVersion"
    $InternalVersionNumberFromDatabase = $(Invoke-Sqlcmd -ServerInstance $instance -Database $database -Query $InternalVersionNumberFromDatabaseSQL).InternalVersion
    $InternalVersionNumberFromDatabase
    } Catch { Log "Error getting internal version" -Level Error; Throw }
}

function Get-DatabaseLocks ($instancename, $databasename)
    {
Try
    {
    $InUseSQL = "IF EXISTS (SELECT * FROM sys.dm_tran_locks WHERE resource_database_id = DB_ID('$databasename')) SELECT 1 AS InUse ELSE SELECT 0 AS InUse"
    return $(Invoke-Sqlcmd -ServerInstance $instancename -Database master -Query $InUseSQL -AbortOnError ).InUse
    } Catch { Log -message "Error checking locks" -Level Error ;  Throw }
}
function CheckIfDatabaseIsInAvailabilityGroup ([string]$Instance, [string]$Database)
{
Try
    {
    $CheckAGSQL = @"
    SELECT DISTINCT d.name, ag.name AS AGName
    FROM sys.databases d 
    JOIN sys.dm_hadr_database_replica_states hadrdrs ON d.database_id = hadrdrs.database_id
    JOIN sys.availability_groups ag ON ag.group_id = hadrdrs.group_id
    WHERE d.name = '$Database'
"@
    Log -message $CheckAGSQL -Level Info 
    Log -message "Running on $Database on $Instance" -Level Info 
    Invoke-Sqlcmd -ServerInstance $Instance -Database $Database -Query $CheckAGSQL

    } Catch {log -message "Error checking availability group" -Level Error  ; Throw }
}

function Get-DatabaseEncryptionFromDatabase ($instancename, $databasename)
{
Try
    {
    $DatabaseEncryptionSQL = @"
SELECT d.name, key_algorithm, key_length, ISNULL(encryption_state,0) AS encryption_state, CONVERT(varchar(max),encryptor_thumbprint  ,2 ) AS  encryptor_thumbprint  
	FROM sys.databases d 
    LEFT OUTER JOIN sys.dm_database_encryption_keys dek ON dek.database_id = d.database_id
    WHERE DB_NAME(d.database_id) = '$databasename'
"@
    $DatabaseEncryptionResults = Invoke-Sqlcmd -ServerInstance $instancename -Database $databasename -Query $DatabaseEncryptionSQL -AbortOnError
    $DatabaseEncryptionResults
    } Catch {Log "Failed to get database encryption details for $databasename on $instancename" ;  Throw }
}

function Get-SQLInstanceCompression ($instancename)
    {
    If (($(Invoke-Sqlcmd -ServerInstance $instancename -Query "SELECT ISNULL((SELECT value FROM sys.configurations WHERE name = 'backup compression default'),-1) AS C").C) -eq -1)
        {
        $false
        }
    Else
        {
        $true
        }
    }

function Get-FileListFromDatabase ($instance, $Database)
{
Try
    {
    $FileListSQL = "SELECT CASE WHEN type = 0 THEN 'D' ELSE 'L' END AS Type, Size * 8 / 1024 AS SizeInMB, name AS LogicalName, physical_name AS PhysicalName FROM sys.master_files WHERE database_id = DB_ID()"
    Invoke-Sqlcmd -ServerInstance $instance -Database $Database -Query $FileListSQL
    } Catch  { Log -message "Error getting file list. instance = $instance , Database = $Database" -Level Error ; Throw }
}

function Get-SQLDatabaseOwner ($instancename, $databasename)
{
Try
    {
    $CurrentOwner = $(Invoke-Sqlcmd -ServerInstance $instancename -Database $databasename -Query "select suser_sname(owner_sid) AS Owner from sys.databases where database_id = DB_ID()").Owner
    $CurrentOwner
} Catch { Log "Error getting current owner." "Error"; Throw }
}

function Get-SQLDatabaseCompatibilityLevel  ($instancename, $databasename)
{
Try
    {
    $CurrentCompatabilityLevelSQL = "SELECT compatibility_level FROM sys.databases WHERE database_id = DB_ID()"
    $CurrentCompatabilityLevel = $(Invoke-Sqlcmd -ServerInstance $instancename -Database $databasename -Query $CurrentCompatabilityLevelSQL).compatibility_level
    $CurrentCompatabilityLevel
    } Catch { Log "Error getting current compatability Level." "Error"; Throw }
}

function Get-SQLDatabaseRecoveryModel ($instancename, $databasename)
{
Try
    {
    $CurrentRecoveryModel = $(Invoke-Sqlcmd -ServerInstance $instancename -Database $databasename -Query "SELECT recovery_model_desc FROM sys.databases WHERE name = db_name() ;").recovery_model_desc
    $CurrentRecoveryModel
    } Catch { Log "Error getting current recovery model." "Error"; Throw }
}

function Check-Compatibility ([string]$Instance, [string]$CompatibilityLevel)
{
Try
    {
    $ProdName = Get-ProductName -Instance $Instance
    [int]$c = $MinimumCompatibilityValues[$ProdName]
    
    Log -message "minimum compat for $ProdName is $c.  Compat $CompatibilityLevel was specified" -Level Info -WriteToHost
    If ( $c -gt [int]$CompatibilityLevel)
        {
        log -message "The minimum compatibility level for $Instance (which is version $ProdName ) is $($MinimumCompatibilityValues[ $ProdName]).  Specify a compatibility level greater than this." -Level Error  -WriteToHost
        Throw "The minimum compatibility level for $Instance (which is version $ProdName ) is $($MinimumCompatibilityValues[ $ProdName]).  Specify a compatibility level greater than this."
        }
    } Catch { log -message "Error checking compatibility" -Level Error ; Throw }
}

function Get-BackupLocation ([string]$instancename, [string]$databasename, [bool]$CreateIfNotExist, [bool]$MarkAsRetain, [bool]$differential = $false, [string]$BackupLocation)
    {
        If ($BackupLocation -eq "")
            {
            Try
                {
                Log "Differential is $Differential"
                Set-Location c:\
                $SourceSiteSQL =  @"
select CASE  s.SiteName WHEN 'Hemel Corporate' then 'HO' WHEN 'Slough Corporate' THEN 'PDC' END  AS SiteCode
from Tbl_Server_List SL JOIN Tbl_Stats_Hosts h ON sl.HostID = h.HostID JOIN tbl_stats_sites s ON h.SiteID = s.SiteID 
WHERE sl.Server_name = '$instancename' 
"@
                Log $SourceSiteSQL
                $SourceSite = $(Invoke-Sqlcmd -ServerInstance $dbainstance -Database $dbadatabase -Query $SourceSiteSQL).SiteCode
                }
            Catch
                {
                Log -message "Error querying for backup loction" -Level Error
                Throw 
                }
            If ($SourceSite -eq "PDC")
                {
                $dbabasebackuplocation = "\\pdc-dbastr-01vm\SQLBackups\"
                }
            Else
                {
                $dbabasebackuplocation = "\\ho-dbastr-01vm\SQLBackups\"
                }
            $BackupPath = "$dbabasebackuplocation$SourceSite\$($instancename -replace "\\", "$")\$databasename\"
            }
        Else
            {
            $BackupPath = "$BackupLocation\$($instancename -replace "\\", "$")\$databasename\"
            Log "BackupPath = $BackupPath"
            }
    If ($CreateIfNotExist)
        {
         If ((Test-Path $BackupPath) -eq $false )
            {
            try { mkdir $BackupPath | Out-Null } Catch {Throw "Error creating backup path" }
            }
        }
    $dateForFileName = Get-Date -Format "yyyyMMddhhmm"

    $fullPath = "$BackupPath$Databasename$("_adhoc_")$(if($MarkAsRetain) {"Retain_"})$(if($Differential) {"DIFF_"})$dateForFileName.BAK"
    Log $fullPath
    $fullPath
    }

function Get-SQLDatabaseRecoveryModel ($instancename, $databasename)
{
Try
    {
    $CurrentRecoveryModel = $(Invoke-Sqlcmd -ServerInstance $instancename -Database $databasename -Query "SELECT recovery_model_desc FROM sys.databases WHERE name = db_name() ;").recovery_model_desc
    $CurrentRecoveryModel
    } Catch { Log "Error getting current recovery model." "Error"; Throw }
}

function Get-SQLDatabaseTrustworthy ($instancename, $databasename)
{
Try

    {
    $TrustworthySQL = "SELECT is_trustworthy_on FROM sys.databases WHERE database_id = DB_ID()"
    $Trustworthyresults = Invoke-Sqlcmd -ServerInstance $instancename -Database $databasename -Query $TrustworthySQL
    $Trustworthy = $Trustworthyresults.is_trustworthy_on
    $Trustworthy
    } Catch { Log "Error getting is_trustworthy_on." "Error"; Throw }
}

function Write-SQLDatabaseTrustworthy ($instancename, $databasename, $trustworthy)
{
Try
    {
    if ($SourceTrustworthy -eq 1) 
        {
        $trustworthySQL = "ALTER DATABASE [$TargetDatabase] SET TRUSTWORTHY ON"
        } 
    else 
        {
        $trustworthySQL = "ALTER DATABASE [$databasename] SET TRUSTWORTHY OFF"
        }
    Invoke-Sqlcmd -ServerInstance $instancename -Database master -Query $trustworthySQL -AbortOnError
    } Catch  { Log "Error setting Trustworthy" "Error" ; Throw  }
}

function Test-PathOnSQLServer ($instance, $path, [bool]$TestDirectoryOnly = $false)
{
Try
    {
    If ($TestDirectoryOnly)
        {
        $path = Split-Path $path
        }
    $PathOnSQLServerSQL = "exec master.dbo.xp_fileexist '$path'"
    $PathOnSQLServerResults = $(Invoke-sqlcmd -ServerInstance $instance -Database master -Query $PathOnSQLServerSQL ).'File is a Directory'
    If ($PathOnSQLServerResults -eq 1)
        {
        $true
        }
    Else
        {
        $false
        }
    } Catch { Log -message "Error" -Level Error ; Throw }
}

function Get-SQLUserRoles ([string]$instancename, [string]$databasename)
{
Try
    {
    $existingPrincipalsSQL = @"
SELECT 
dpusers.name AS DatabaseUserName, sp.name AS ServerLoginName, dp.name AS RoleName, dpusers.type_desc
FROM sys.database_role_members drm
JOIN sys.database_principals dp ON dp.principal_id = drm.role_principal_id
JOIN sys.database_principals dpusers ON dpusers.principal_id = drm.member_principal_id
JOIN sys.server_principals sp ON dpusers.sid = sp.sid
WHERE dpusers.name <> 'dbo'
"@
    $existingPrincipals = Invoke-Sqlcmd -ServerInstance $instancename -Query $existingPrincipalsSQL -Database $databasename
    $existingPrincipals
    } Catch { Throw  }
}

function Backup-Database ([string]$instancename, [string]$databasename, [string]$backuppath, [bool]$compress = $false, [int]$ProgressID, [string]$JobName, [bool]$CopyOnly = $true, [bool]$Differential = $false)
    {
Try
    {
    Log "In Backup-Database"
    log "backuppath = $backuppath"
    $BackupSQL = "BACKUP DATABASE [$databasename] TO DISK = '$backuppath'"
    Try
        {
        $with = ""
        If  ($CompressIfPossible -and ($Compress.toString() -ne "-1")) {$with = " WITH COMPRESSION "}
        if($copyonly -and ($Differential -eq $false))
            {
            if ($with -eq "") {$with = "WITH COPY_ONLY"} else { $with = $with + ", COPY_ONLY " }
            }
        if($Differential )
            {
            Log "Adding DIFFERENTIAL"

            if ($with -eq "") {$with = "WITH DIFFERENTIAL"} else { $with = $with + ", DIFFERENTIAL " }
            }
        $BackupSQL = $BackupSQL + $with
        Log "$BackupSQL"
        Log $instancename
        } Catch {Log "Error adding with options" "Error" ; Throw }

    $job = Start-Job -Name $JobName -ScriptBlock {     Invoke-Sqlcmd -ServerInstance $args[0] -Query $args[1] -QueryTimeout 65535 -AbortOnError} -ArgumentList @( "$instancename", $BackupSQL ) 
    $jobdetails = @{job=$job ; Instance = $instancename ; Database = $databasename ; Path = $backuppath ; JobType = "BackupRestore" ; Id = Get-NextProgressId}
    Log -message $jobdetails.id -Level Info
    $jobdetails
    } Catch { Log "Error in Backup-Database" -Level Error ; Throw }
}

function Progress2 {param ($Jobdetailscollection)
Try
{
$i = 0
While ($Jobdetailscollection | Where-Object { $($_.job.State -eq "Running") })
    {
    $i++
    foreach ($job in $Jobdetailscollection )
        {
        Start-Sleep $ProgressInterval
       $state = $($job.job.State).ToString()
        if ($state -eq "Completed")
            {
            Write-Progress -Id $($job.id) -PercentComplete 100 -Activity "$($job.job.Name) on $($job.Database) on $($job.Instance) to $($job.path)" -Status "Complete" 
            }
        Elseif ($state -eq "Failed")
            {
            $er = $(($job).ChildJobs)[0].JobStateInfo.Reason
            Log -message $er -Level Error 
            Throw $er
            }
       Else
            {
            $BackupProgress = Get-BackupRestoreProgress -Instance $($Job.Instance) -CmdString $($job.path)
            $percentComplete = $BackupProgress.PercentComplete
            if ($percentComplete -eq $null) {$percentComplete = 0}
            If ($percentComplete -eq 0) {$NoBackupProgress = "  No information on progress yet, please wait.   "} else {$NoBackupProgress = ""}
            $ETA = $BackupProgress.ETACompletionTime
            if ($ETA -eq $null) {$ETA = "Unknown"}
            $ETAMin = $BackupProgress.ETAMin
            if ($ETAMin -eq $null) {$ETAMin = "Unknown"}
            $elapsedMin = $BackupProgress.ElapsedMin
            if ($elapsedMin -eq $null) {$elapsedMin = "Unknown"}
            If ($i % 4 -eq 0)
                {
                Log "Percent Complete = $percentComplete, ETA = $ETA (in $ETAMin minutes)"
                }
            $p = [int]$percentComplete
            Write-Progress -id $job.id -Activity "$($job.job.name) on $($job.database) on $($job.Instance) to $($job.path)" -percentComplete $p -Status "$NoBackupProgress Complete = $percentComplete, ETA = $ETA (in $ETAMin minutes). $elapsedMin minutes elapsed."
            }
        }
    }
} Catch 
    {
    $reason = (Get-Job | Where-Object {$_.State -eq "Failed"}).ChildJobs[0].JobStateInfo.Reason 
    Log -message "error $reason" -Level Error ; Throw $reason
    }
} #function Progress 

function Get-BackupRestoreProgress ([string]$Instance, [string]$CmdString, [int]$Interval)
{
$wait = $true
While ($wait -eq $true)
    {
    $ProgressSQL = @"
    SELECT 
    r.session_id
    ,r.command
    ,CONVERT(NUMERIC(6,2),r.percent_complete) AS [PercentComplete]
    ,CONVERT(VARCHAR(20),DATEADD(ms,r.estimated_completion_time,GetDate()),20) AS [ETACompletionTime]
    ,CONVERT(NUMERIC(10,2),r.total_elapsed_time/1000.0/60.0) AS [ElapsedMin]
    ,CONVERT(NUMERIC(10,2),r.estimated_completion_time/1000.0/60.0) AS [ETAMin]
    ,CONVERT(NUMERIC(10,2),r.estimated_completion_time/1000.0/60.0/60.0) AS [ETA Hours]
    ,CONVERT(VARCHAR(1000),SUBSTRING(text,r.statement_start_offset/2,CASE WHEN r.statement_end_offset = -1 THEN 1000 ELSE (r.statement_end_offset-r.statement_start_offset)/2 END)) AS Cmd
    FROM sys.dm_exec_requests r 
    CROSS APPLY sys.dm_exec_sql_text(sql_handle) st
    WHERE command IN ('BACKUP DATABASE','RESTORE HEADERON', 'RESTORE DATABASE','RESTORE VERIFYON') 
    AND session_id <> @@SPID 
    AND CONVERT(VARCHAR(1000),SUBSTRING(text,r.statement_start_offset/2,CASE WHEN r.statement_end_offset = -1 THEN 1000 ELSE (r.statement_end_offset-r.statement_start_offset)/2 END)) LIKE'%$cmdstring%'
"@
    Invoke-Sqlcmd -ServerInstance $Instance -Query $ProgressSQL
    if ($Interval -eq 0)
        {
        $wait = $false
        }
    Else
        {
        Start-Sleep -Seconds $Interval
        }
    }
}
[int]$Global:NextProgressId = 1
function Get-NextProgressId()
{
$Global:NextProgressId = $Global:NextProgressId + 1
log -message $Global:NextProgressId -Level Info 
$Global:NextProgressId
}

Function Check-DatabaseAccess ($instance, $database)
{
Try
    {
    $checkDatabaseNameInSysDatabasesSQL = "SELECT COUNT(*) AS cnt FROM sys.databases WHERE name = '$database'"
    $checkDatabaseNameInSysDatabasesResults = $(Invoke-Sqlcmd -ServerInstance $instance -Database master -Query $checkDatabaseNameInSysDatabasesSQL).cnt
    If ($checkDatabaseNameInSysDatabasesResults -eq 0)
        {
        $false
        }
    Else
        {
        $VersionSQL = "SELECT @@VERSION AS Version"
        $VersionResults = Invoke-Sqlcmd -ServerInstance $instance -Database $database -Query $VersionSQL -AbortOnError 
       # $Version = $Versionresults.Version 
        $true
        }
    } 
    Catch 
        { 
        $false
        }
}

Function SendEMail ([string]$Subject, [string]$msg, [string]$Address, [string]$FromAddress, [bool]$NoLog = $false)
{
Try
    {
    $Address = "mflemming@laingorourke.com"
    If ($Address -EQ "")
        {
        $me = $env:USERNAME
        $Address = $(Get-ADUser -Filter {samaccountname -like $me} -Properties EmailAddress | Select-Object Emailaddress).EmailAddress
        $messageTo = $Address 
        $messageFrom = $Address 
        }
    Else
        {
        $ArrAddress = $Address.Split(",")
        $messageTo = $ArrAddress 
        If ($FromAddress -eq "")
            {
            $messageFrom = $ArrAddress[0]
            }
        Else
            {
            $messageFrom = $FromAddress
            }
        }


    Log -message $Address -Level Info
    If ($ExecutionID -ne $null)
        {
        $logresultsSQL  = "SELECT  message, level, errordetails, errorline  FROM log WITH (NOLOCK) WHERE executionID = '$ExecutionID' ORDER BY logid DESC"
        $logResults = Invoke-Sqlcmd -ServerInstance $dbainstance -Database $dbadatabase -Query $logresultsSQL
        $logresultstext = $logResults | Format-Table -AutoSize| Out-String 
        $logresultstext = "`n`nNote that single quotes have been stripped from the log.  `n" + $logresultstext + " Run the following query for more details `n`n`n SELECT * FROM Log WHERE ExecutionID = '$ExecutionID' ORDER BY datetime DESC"
        }
    $messageBody = $msg + "`n`n" + $params + "`n`n" + $logresultstext 
    if ($Subject -eq $null) { $Subject = ""}
    if ($Subject -eq "") { $Subject = "" }
    Send-MailMessage -SmtpServer $smtpserver -Subject $Subject  -Body $messageBody -From $messageFrom -To $messageTo -ErrorAction Stop
    } Catch { Log "Failed to send email." "Error" ; Throw }
}

function Get-space ($sourcefilelist, [string]$Instance, [string]$database, [bool]$CreateDatabase)
{
Try
    {
    If ($CreateDatabase)
        {
        $SizeOfFilesToBeRestored = Get-SumOfFileSizes -sourcefilelist $sourcefilelist
        $Paths = Get-DefaultPathslegacy -instance $instance
        log -message "Paths = $($Paths | Out-String)"
        #$LocalPaths = Get-DriveSpace -Instance $instance
        #$TotalSizeForDrive = 0
        foreach ($drive in $(Get-DriveSpace -Instance $instance))
            {
            $LogSize = 0
            $dataSize = 0
            log -message "Checking $($drive.volume_mount_point)"
            If ($paths.defaultfile -like "$($drive.volume_mount_point)*") 
                {
                Log -message "defaultfile $($drive.volume_mount_point) $($paths.defaultfile) "
                $dataSize = $dataSize + $($SizeOfFilesToBeRestored | Where-Object {$_.Type -eq "D"} | Measure-Object SizeInMB -Sum).Sum
                }
            If ($paths.defaultLog -like "$($drive.volume_mount_point)*")
                {
                Log -message "defaultlog $($drive.volume_mount_point)"
                $LogSize = $LogSize +  $($SizeOfFilesToBeRestored | Where-Object {$_.Type -eq "L"} | Measure-Object SizeInMB -Sum).Sum
                }

            $Custom = New-Object psobject 
            $Custom | Add-Member -Name "Drive" -Value $drive.volume_mount_point -MemberType NoteProperty
            $Custom | Add-Member -Name DataFileRequired -Value $dataSize  -MemberType NoteProperty
            $Custom | Add-Member -Name LogFileRequired -Value $LogSize  -MemberType NoteProperty
            $Custom | Add-Member -Name TotalUsedMB -Value $drive.TotalUsedMB  -MemberType NoteProperty     
            $Custom | Add-Member -Name TotalFreeMB -Value $drive.TotalFreeMB  -MemberType NoteProperty
            $Custom | Add-Member -Name FreeAfterRestore -Value $($drive.TotalFreeMB - $dataSize - $LogSize) -MemberType NoteProperty
            $Custom | Add-Member -Name PercentFreeAfter -Value $($($drive.TotalFreeMB - $dataSize - $LogSize) /  $($drive.TotalFreeMB + $drive.TotalUsedMB) * 100) -MemberType NoteProperty
            $Custom | Where-Object {$_.Drive -ne "C:\"}
            }
        }
    Else
        {
        $result = @()
        
$trgfilelist = Get-FileListFromDatabase -instance $instance -Database $database
foreach ($trgfile in $trgfilelist)
    {
    foreach ($srcfile in $sourcefilelist)
        {
#        Write-Host "sourcefile logical name = $($srcfile.LogicalName) , target file logical name = $($trgfile.LogicalName)"
        If ($($srcfile.LogicalName) -eq $($trgfile.LogicalName))
            {
            $fileobj = New-Object psobject
            $fileobj | Add-Member -Name SourceLogicalName -Value $srcfile.LogicalName -MemberType NoteProperty
            $fileobj | Add-Member -Name SourcePhysicalName -Value $srcfile.PhysicalName -MemberType NoteProperty
            $fileobj | Add-Member -Name SourceSizeInMB -Value $srcfile.SizeInMB -MemberType NoteProperty
            $fileobj | Add-Member -Name SourceType -Value $srcfile.Type -MemberType NoteProperty
            $fileobj | Add-Member -Name TargetLogicalName -Value $trgfile.LogicalName -MemberType NoteProperty
            $fileobj | Add-Member -Name TargetPhysicalName -Value $trgfile.PhysicalName -MemberType NoteProperty
            $fileobj | Add-Member -Name TargetSizeInMB -Value $trgfile.SizeInMB -MemberType NoteProperty
            $fileobj | Add-Member -Name TargetType -Value $trgfile.Type -MemberType NoteProperty
            $result = $result + $fileobj
            }
        }
    }
    $finalresults = @()
    foreach ($drive in Get-DriveSpace -Instance $instance)
        {
        #Write-Host $drive.volume_mount_point
        foreach ($file in $result)
            {
            if (  $file.TargetPhysicalName -like "$($drive.volume_mount_point)*")
                {
                if ($file.SourceType -eq "D")
                    {
                    #Write-Host "data"
                    $netdata = $netdata + ($file.SourceSizeInMB - $file.TargetSizeInMB)
                    }
                ElseIf  ($file.SourceType -eq "L")
                    {
                    #Write-Host "log"
                    $netlog = $netlog  + ($file.SourceSizeInMB - $file.TargetSizeInMB)
                    }
                }
            }

        
        $Custom = New-Object psobject 
        $Custom | Add-Member -Name "Drive" -Value $drive.volume_mount_point -MemberType NoteProperty
        $Custom | Add-Member -Name DataFileRequired -Value $netdata  -MemberType NoteProperty
        $Custom | Add-Member -Name LogFileRequired -Value $netLog  -MemberType NoteProperty
        $Custom | Add-Member -Name TotalUsedMB -Value $drive.TotalUsedMB  -MemberType NoteProperty     
        $Custom | Add-Member -Name TotalFreeMB -Value $drive.TotalFreeMB  -MemberType NoteProperty
        $Custom | Add-Member -Name FreeAfterRestore -Value $($drive.TotalFreeMB - $netdata - $netLog) -MemberType NoteProperty
        $Custom | Add-Member -Name PercentFreeAfter -Value $($($drive.TotalFreeMB - $netdata - $netLog) / $($drive.TotalFreeMB + $drive.TotalUsedMB) * 100) -MemberType NoteProperty
        #$Custom
        #Write-Host "adding the object to final results"
        $finalresults = $finalresults + $Custom
        }
        }
    $finalresults
    } Catch { log -message "Error getting space on $instance " -Level Error -WriteToHost ; Throw }
}

function Get-SumOfFileSizes ($sourcefilelist)
{
Try
    {
    $sourcefilelist |  Group-Object -Property Type | ForEach-Object { New-Object psobject -Property @{ Type = $_.Name ; SizeinMB = ($_.Group | Measure-Object Sizeinmb -Sum).Sum } }
    }
Catch { Log -message "Error sum of file size" -Level Error ;  Throw }
}

function Get-DefaultPathslegacy ($instance)
{
Try
    {
    $DefaultPathSQL = @"
IF OBJECT_ID('tempdb..#instance_registry_path') IS NOT NULL DROP TABLE #instance_registry_path
DECLARE @default_data_path nvarchar(1000)
DECLARE @print_message   nvarchar (200)
DECLARE @version   nvarchar (50)
DECLARE @error int
DECLARE @on_debug_mode int
DECLARE @detection_mode nvarchar (50)

SET @default_data_path=NULL
SET @error=0
SET @on_debug_mode=1

SELECT @version=
CASE (select @@MICROSOFTVERSION / POWER(2,24))
WHEN 8 THEN N'SQL Server 2000'
WHEN 9 THEN N'SQL Server 2005'
WHEN 10 THEN N'SQL Server 2008'
WHEN 11 THEN N'SQL Server 2012'
WHEN 12 THEN N'SQL Server 2014'
ELSE N'OTHER'
END

BEGIN
	IF (@on_debug_mode=1) PRINT ('DEBUG: Default Data Path - Search with SERVERPROPERTY')
	SET @detection_mode=N'SERVERPROPERTY'
	SELECT @default_data_path = CONVERT(sysname,SERVERPROPERTY('InstanceDefaultDataPath'))
	if (@default_data_path IS NULL)
	BEGIN
		IF (@on_debug_mode=1) PRINT ('DEBUG: Default Data Path - Search with HKEY_LOCAL_MACHINE')
		SET @detection_mode=N'REGISTRY KEY'
		CREATE TABLE #instance_registry_path
		(
			instance_name sysname,
			registry_path sysname
		)

		INSERT INTO #instance_registry_path		
			EXEC master.sys.xp_instance_regenumvalues N'HKEY_LOCAL_MACHINE',N'SOFTWARE\\Microsoft\\Microsoft SQL Server\\Instance Names\\SQL'
		
		DECLARE @registry_path sysname
		DECLARE @instance_name sysname
		DECLARE @backslash_position int
		SET @instance_name=NULL
		SET @registry_path=NULL
		SET @backslash_position=0

		Select @backslash_position= CHARINDEX('\',@@SERVERNAME)
		IF (@backslash_position=0) -- Default instance
			SET @instance_name=N'MSSQLSERVER'
		ELSE
			SET @instance_name=SUBSTRING(@@SERVERNAME,@backslash_position+1,LEN(@@SERVERNAME))

		IF (@on_debug_mode=1)
		BEGIN
			SET @print_message= N'DEBUG: instance name: ' + @instance_name  
			PRINT(@print_message)
		END
		SELECT @registry_path=registry_path FROM #instance_registry_path WHERE @instance_name = instance_name
		IF (@on_debug_mode=1)
		BEGIN
			SET @print_message= N'DEBUG: registry path: ' + @registry_path  
			PRINT(@print_message)
		END
		DROP TABLE #instance_registry_path

		DECLARE @registry_key NVARCHAR(MAX)
		SET @registry_key= N'SOFTWARE\Microsoft\Microsoft SQL Server\'+@registry_path+N'\MSSQLServer'
		IF (@on_debug_mode=1)
		BEGIN
			SET @print_message= N'DEBUG: registry key: ' + @registry_key  
			PRINT(@print_message)
		END
	
		exec master.sys.xp_regread N'HKEY_LOCAL_MACHINE',@registry_key, N'DefaultData',@default_data_path output

		IF(@default_data_path IS NULL)
		BEGIN
			IF (@on_debug_mode=1) PRINT ('DEBUG: Default Data Path - Search with USER-DATABASE')
			DECLARE @database_name sysname;
			SET @database_name=NULL
			SET @detection_mode=N'USER-DATABASE'
			SELECT TOP 1 @database_name=name from sys.databases where database_id>5;
			IF (@database_name IS NULL)
			BEGIN
				PRINT('WARNING: No user-database in this instance');
			END
			ELSE
			BEGIN
				IF (@on_debug_mode=1)
				BEGIN
					SET @print_message= N'DEBUG: user-database name: ' + @database_name
					PRINT(@print_message);
				END
				ELSE
				BEGIN
					DECLARE @sqlexec nvarchar (200);
					--use sys.database_files to find the path
					SET @sqlexec = N'use ['+@database_name+'];select TOP 1 @data_path=physical_name from sys.database_files where type=0;';
					EXEC sp_executesql @sqlexec, N'@data_path nvarchar(max) OUTPUT',@data_path=@default_data_path OUTPUT;
					IF (@on_debug_mode=1)
					BEGIN
						SET @print_message= N'DEBUG: physical name: ' + @default_data_path
						PRINT(@print_message);
					END
					-- Find the path from first character to the last '\'
					SET @default_data_path=REVERSE(STUFF(REVERSE(@default_data_path),1,CHARINDEX('\',REVERSE(@default_data_path)),''))
					IF (@on_debug_mode=1)
					BEGIN
						SET @print_message= N'DEBUG: user database path: ' + @default_data_path
						PRINT(@print_message);
					END
				END
			END
			IF(@default_data_path IS NULL)
			BEGIN
				IF (@on_debug_mode=1) PRINT ('DEBUG: Default Data Path - Search with MASTER DATABASE')
				SET @detection_mode = N'MASTER DATABASE'
				--use master.sys.database_files to find the path
				select TOP 1 @default_data_path = physical_name from master.sys.database_files df where df.type=0;
				IF (@on_debug_mode=1)
					BEGIN
						SET @print_message= N'DEBUG: physical name: ' + @default_data_path
						PRINT(@print_message);
					END
				SET @default_data_path=REVERSE(STUFF(REVERSE(@default_data_path),1,CHARINDEX('\',REVERSE(@default_data_path)),''))
				IF (@on_debug_mode=1)
				BEGIN
					SET @print_message= N'DEBUG: default master data path name: ' + @default_data_path
					PRINT(@print_message);
				END
			END
		END
	END
	IF (@default_data_path IS NULL)
	BEGIN
		SET @error=@@ERROR
		IF (@error<>0)
		BEGIN
			SET @print_message=N'Error N° ' + RTRIM (CAST (@error AS NVARCHAR (10))) + N' - Message: '+ERROR_MESSAGE();
			PRINT(@print_message)
		END
		ELSE
		BEGIN
			PRINT(N'No error number is generated')
		END
	END
	ELSE 
	BEGIN
		SET @print_message=@version+N' - Detection mode: '+ @detection_mode + N' - Path: ' + @default_data_path
		PRINT(@print_message)
	END
--END
DECLARE @default_log_path nvarchar(1000)
SET @default_log_path=NULL
SET @error=0
SET @on_debug_mode=1

SELECT @version=
CASE (select @@MICROSOFTVERSION / POWER(2,24))
WHEN 8 THEN N'SQL Server 2000'
WHEN 9 THEN N'SQL Server 2005'
WHEN 10 THEN N'SQL Server 2008'
WHEN 11 THEN N'SQL Server 2012'
WHEN 12 THEN N'SQL Server 2014'
ELSE N'OTHER'
END

--BEGIN
	IF (@on_debug_mode=1) PRINT ('DEBUG: Default Log Path - Search with SERVERPROPERTY')
	SET @detection_mode=N'SERVERPROPERTY'
	SELECT @default_log_path = CONVERT(sysname,SERVERPROPERTY('InstanceDefaultLogPath'))
	if (@default_log_path IS NULL)
	BEGIN
		IF (@on_debug_mode=1) PRINT ('DEBUG: Default Log Path - Search with HKEY_LOCAL_MACHINE')
		SET @detection_mode=N'REGISTRY KEY'

		IF EXISTS ( SELECT * FROM sys.tables WHERE name LIKE '#instance_registry_path1%') DROP TABLE #instance_registry_path1
		IF OBJECT_ID('tempdb..#instance_registry_path1') IS NOT NULL DROP TABLE #instance_registry_path1
		CREATE TABLE #instance_registry_path1
		(
			instance_name sysname,
			registry_path sysname
		)
		INSERT INTO #instance_registry_path1					EXEC master.sys.xp_instance_regenumvalues N'HKEY_LOCAL_MACHINE',N'SOFTWARE\\Microsoft\\Microsoft SQL Server\\Instance Names\\SQL'

		SET @instance_name=NULL
		SET @registry_path=NULL
		SET @backslash_position=0

		Select @backslash_position= CHARINDEX('\',@@SERVERNAME)
		IF (@backslash_position=0) -- Default instance
			SET @instance_name=N'MSSQLSERVER'
		ELSE
			SET @instance_name=SUBSTRING(@@SERVERNAME,@backslash_position+1,LEN(@@SERVERNAME))

		IF (@on_debug_mode=1)
		BEGIN
			SET @print_message= N'DEBUG: instance name: ' + @instance_name  
			PRINT(@print_message)
		END
		SELECT @registry_path=registry_path FROM #instance_registry_path1 WHERE @instance_name = instance_name
		IF (@on_debug_mode=1)
		BEGIN
			SET @print_message= N'DEBUG: registry path: ' + @registry_path  
			PRINT(@print_message)
		END
		DROP TABLE #instance_registry_path1

		--DECLARE @registry_key NVARCHAR(MAX)
		SET @registry_key= N'SOFTWARE\Microsoft\Microsoft SQL Server\'+@registry_path+N'\MSSQLServer'
		IF (@on_debug_mode=1)
		BEGIN
			SET @print_message= N'DEBUG: registry key: ' + @registry_key  
			PRINT(@print_message)
		END
	
		exec master.sys.xp_regread N'HKEY_LOCAL_MACHINE',@registry_key, N'DefaultLog',@default_log_path output

		IF(@default_log_path IS NULL)
		BEGIN
			IF (@on_debug_mode=1) PRINT ('DEBUG: Default Log Path - Search with USER-DATABASE')
			--DECLARE @database_name sysname;
			SET @database_name=NULL
			SET @detection_mode=N'USER-DATABASE'
			SELECT TOP 1 @database_name=name from sys.databases where database_id>5;
			IF (@database_name IS NULL)
			BEGIN
				PRINT('WARNING: No user-database in this instance');
			END
			ELSE
			BEGIN
				IF (@on_debug_mode=1)
				BEGIN
					SET @print_message= N'DEBUG: user-database name: ' + @database_name
					PRINT(@print_message);
				END
				ELSE
				BEGIN
					--DECLARE @sqlexec nvarchar (200);
					--use sys.database_files to find the path
					SET @sqlexec = N'use ['+@database_name+'];select TOP 1 @log_path=physical_name from sys.database_files where type=1;';
					EXEC sp_executesql @sqlexec, N'@log_path nvarchar(max) OUTPUT',@log_path=@default_log_path OUTPUT;
					IF (@on_debug_mode=1)
					BEGIN
						SET @print_message= N'DEBUG: physical name: ' + @default_log_path
						PRINT(@print_message);
					END
					-- Find the path from first character to the last '\'
					SET @default_log_path=REVERSE(STUFF(REVERSE(@default_log_path),1,CHARINDEX('\',REVERSE(@default_log_path)),''))
					IF (@on_debug_mode=1)
					BEGIN
						SET @print_message= N'DEBUG: user database path: ' + @default_log_path
						PRINT(@print_message);
					END
				END
			END
			IF(@default_log_path IS NULL)
			BEGIN
				IF (@on_debug_mode=1) PRINT ('DEBUG: Default Log Path - Search with MASTER DATABASE')
				SET @detection_mode = N'MASTER DATABASE'
				--use master.sys.database_files to find the path
				select TOP 1 @default_log_path = physical_name from master.sys.database_files df where df.type=1;
				IF (@on_debug_mode=1)
					BEGIN
						SET @print_message= N'DEBUG: physical name: ' + @default_log_path
						PRINT(@print_message);
					END
				SET @default_log_path=REVERSE(STUFF(REVERSE(@default_log_path),1,CHARINDEX('\',REVERSE(@default_log_path)),''))
				IF (@on_debug_mode=1)
				BEGIN
					SET @print_message= N'DEBUG: default master log path name: ' + @default_log_path
					PRINT(@print_message);
				END
			END
		END
	END
	IF (@default_log_path IS NULL)
	BEGIN
		SET @error=@@ERROR
		IF (@error<>0)
		BEGIN
			SET @print_message=N'Error N° ' + RTRIM (CAST (@error AS NVARCHAR (10))) + N' - Message: '+ERROR_MESSAGE();
			PRINT(@print_message)
		END
		ELSE
		BEGIN
			PRINT(N'No error number is generated')
		END
	END
	ELSE 
	BEGIN
		SET @print_message=@version+N' - Detection mode: '+ @detection_mode + N' - Path: ' + @default_log_path
		PRINT(@print_message)
	END
END
--SELECT @default_log_path as [Default Log Path]

IF RIGHT(@default_log_path, 1) <> '\' 	SET @default_log_path = @default_log_path + '\'
IF RIGHT(@default_data_path, 1) <> '\' 	SET @default_data_path = @default_data_path + '\'

SELECT @default_data_path as [DefaultFile], @default_log_path as [DefaultLog]
"@
    $DefaultPathResults = Invoke-Sqlcmd -ServerInstance $instance -Database master -Query $DefaultPathSQL
    $DefaultPathResults
    } Catch { Log -message "Error Getting the default paths using legacy function" -level "Error" ; Throw }
}

function Get-DriveSpace ([string]$Instance)
{
Try
    {
    $ver = $(Get-SQLInstanceVersion -instanceName $Instance)
    If (($ver -match '2008') -or ($ver -match '2005') -or ($ver -match '2008') )
        {
        log -message "Cant check divespace on $Instance because the version is 2008 or below." -Level Info -WriteToHost -ForegroundColour Yellow
        }
    Else
        {
        $DriveSpaceSQL = "SELECT DISTINCT /*f.database_id, f.file_id, */ volume_mount_point, total_bytes / 1024 / 1024 AS TotalUsedMB, available_bytes / 1024 / 1024 AS TotalFreeMB FROM sys.master_files AS f  CROSS APPLY sys.dm_os_volume_stats (f.database_id, f.file_id); "
        Invoke-Sqlcmd -ServerInstance $Instance -Query $DriveSpaceSQL
        }
    } Catch { Log -message "Error getting drivespace" -Level Error ; Throw }
}

function LoginExists ([string]$instance, [string]$login)
{
Try
    {
    $LoginExistsSQL = "select count(*) as LoginExists FROM sys.server_principals where name = '$login'"
    $LoginExistsResult = Invoke-Sqlcmd -ServerInstance $instance -Database master -Query $LoginExistsSQL
    $LoginExistsResult.LoginExists
    } Catch { Log "Error checking if login $login exists" "Error" ; Throw }
}

Function Restore-SQLDatabase ([string]$instancename, [string]$databasename, [string]$backuppath, [bool]$TakeInstanceOffline, [bool]$NoRecovery, [string]$JobName, [int]$id , [bool]$CreateDatabase, [string]$sourceDatabase , [bool]$differential)
{

Try
    {

    Log "takeInstanceOffline is $TakeInstanceOffline , instancename is $instancename , databasename is $databasename , backuppath is $backuppath , TakeInstanceOffline is $TakeInstanceOffline , NoRecovery is $NoRecovery , JobName is $JobName , CreateDatabase is $CreateDatabase , sourceDatabase is $sourceDatabase , differential is $differential"

    $MoveFiles = ""
    If (($CreateDatabase -eq $false) -or ($differential) )
        {
        $Replace = ", REPLACE"
        }
    Else
        {
        $Replace = ""
        }

    If ($CreateDatabase)
        {
        $MoveFiles = Get-MoveFiles -sourcefilelist $(Get-FileListFromBackupFile -backuppath $backuppath -instance $TargetInstance) -instance $TargetInstance -database $targetDatabase -sourcedatabase $sourceDatabase
        log "Movestatement = $MoveFiles"
        }
    Else
        {
        $MoveFiles = ""
        }

    $RestoreSQL = $null

    If ($TakeInstanceOffline -and ($CreateDatabase -eq $false) -and ($differential -eq $false) ) 
        {
        $RestoreSQL = $RestoreSQL + "ALTER DATABASE [" + $databasename + "] SET OFFLINE WITH ROLLBACK IMMEDIATE`n" 
        }

    $RestoreSQL = $RestoreSQL + @"
RESTORE DATABASE [$databasename] FROM DISK = '$backuppath' WITH STATS = 5 $Replace $MoveFiles $(If($NoRecovery) {",NORECOVERY" })`n
"@
    
    #for REPLACE
    $TargetMasterFiles = Invoke-Sqlcmd -query "select name, physical_name from sys.master_files where database_id = DB_ID('$targetDatabase')" -ServerInstance $TargetInstance
    If ($TargetMasterFiles.Count -gt 0) 
        {
        foreach ($file in $TargetMasterFiles)
            {
            $MoveFiles = $MoveFiles + ", MOVE N'" + $file.name + "' TO N'" + $file.physical_name + "'"
            }
        }
    #Do restore here
    Log $RestoreSQL 
    $job = Start-Job -Name $JobName -ScriptBlock { Invoke-Sqlcmd -ServerInstance $args[0] -Database master -Query $args[1] -QueryTimeout 65535 -ErrorAction Stop} -ArgumentList @($instancename, $RestoreSQL) 
    $jobdetails = @{job=$job ; Instance = $instancename ; Database = $databasename ; Path = $backuppath ; JobType = "BackupRestore" ; Id = Get-NextProgressId}
    $jobdetails
    } Catch { log -message "Error in restore." -Level Error ;  Throw }
} #Restore-SQLDatab

function Get-FileListFromBackupFile ($backuppath, $instance)
{
Try
    {
    $FileListSQL = "RESTORE FILELISTONLY FROM DISK = `'$backuppath`'"
    $FileListResult = Invoke-Sqlcmd -ServerInstance $instance -Database master -Query $FileListSQL
    foreach ($file in $FileListResult )
        {
        $Custom = New-Object psobject
        $Custom | Add-Member -Name "Type" -Value $file.Type -MemberType NoteProperty
        $SizeInMB = $file.Size / 1024 / 1024
        $Custom | Add-Member -Name SizeInMB -Value $SizeInMB  -MemberType NoteProperty
        $Custom | Add-Member -Name LogicalName -Value $file.LogicalName -MemberType NoteProperty
        $Custom | Add-Member -Name PhysicalName -Value $file.PhysicalName -MemberType NoteProperty
        $Custom 
        }
    } Catch { Log -message "Error getting file list path = $backuppath , instance = $instance " -Level Error ; Throw }
}

function Get-MoveFiles ($sourcefilelist, $instance, $database, $sourcedatabase, [bool]$CreateDatabase)
{
Try
    {
    $defaultpaths = Get-DefaultPathslegacy -instance $instance
    log "Default paths = $($defaultpaths.defaultfile) and "
    Log "sourcefilelist = $sourcefilelist , instance = $instance , database = $database , sourcedatabase = $sourcedatabase"
    $moves = @()
    foreach ($file in $sourcefilelist)
        {
        $PhysicalNameOnly = Split-Path $($file.physicalname) -Leaf
        $newNameonly = $physicalnameonly -replace $sourcedatabase , $database
        Log $newNameonly
        If ($CreateDatabase -and (Test-PathOnSQLServer -instance $instance -path $($defaultpaths.defaultfile)$newNameonly ) )
            {
            Log -message "The file : $($defaultpaths.defaultfile)$newNameonly already exists." -Level Error
            Throw "The file : $($defaultpaths.defaultfile)$newNameonly already exists."
            }
        If ($file.type -eq "D")
            {
            $moves = $moves + "MOVE '$($file.logicalname)' TO '$($defaultpaths.defaultfile)$newNameonly' `n"
            }
        ElseIf ($file.type -eq "L")
            {
            $moves = $moves + "MOVE '$($file.logicalname)' TO '$($defaultpaths.defaultlog)$newNameonly' `n"
            }
        }
        $results = $(", $($moves -join ',')")
        Log $results
        $results
    } Catch { Log "Error generating MOVE STATEMENTS" "Error" ; Throw }
}

function Get-SAAccountName ($instance)
{
Try
    {
    $SAAccountSQL = "SELECT SUSER_SNAME(0x1) AS SAAccount"
    $SAAccount = $(Invoke-Sqlcmd -ServerInstance $instance -Database master -Query $SAAccountSQL).SAAccount
    $SAAccount
    } Catch {Log "Error getting SA Account name" "Error" ; Throw }
}

function Write-SQLDatabaseOwner ($instancename, $databasename, $owner)
{
Try
    {
     $ChangeOwnerSQL = @"
DECLARE @ChangeOwnerSQL nvarchar(100) 
SELECT  @ChangeOwnerSQL = 'EXEC sp_changedbowner ''$(If ($owner -eq "sa") {$(Get-SAAccountName -instance $instancename)} else {$owner})'''
EXEC ( @ChangeOwnerSQL )
"@
    Log $ChangeOwnerSQL
    Invoke-Sqlcmd -ServerInstance $instancename -Database $databasename -Query $ChangeOwnerSQL -AbortOnError
    } Catch { Log "Error writing owner." "Error" ; Throw }
}

function Get-SQLInstanceLatestSupportedCompatibilityLevel ($instancename)
{
Try
    {
    $LatestTargetCompatabilityLevel = $(Invoke-Sqlcmd -ServerInstance $instancename -Database master -Query "SELECT compatibility_level FROM sys.databases WHERE database_id = DB_ID()").compatibility_level
    $LatestTargetCompatabilityLevel
    } Catch { Log "Error getting latest compatibility level." "Error"; Throw }
}

function Get-SQLDatabaseCompatibilityLevel  ($instancename, $databasename)
{
Try
    {
    $CurrentCompatabilityLevelSQL = "SELECT compatibility_level FROM sys.databases WHERE database_id = DB_ID()"
    $CurrentCompatabilityLevel = $(Invoke-Sqlcmd -ServerInstance $instancename -Database $databasename -Query $CurrentCompatabilityLevelSQL).compatibility_level
    $CurrentCompatabilityLevel
    } Catch { Log "Error getting current compatability Level." "Error"; Throw }
}

function CheckAndCreatesp_hexidecimalProc ([string]$instance){
Try
{
    $procexists = $(Invoke-Sqlcmd -ServerInstance $instance -Database master -Query "IF OBJECT_ID ('sp_hexadecimal') IS  NULL SELECT 0 AS P ELSE SELECT 1 AS P").P
    If ($procexists -eq "0")
            {
        $CreateProc = @"
        CREATE PROCEDURE sp_hexadecimal
            @binvalue varbinary(256),
            @hexvalue varchar (514) OUTPUT
        AS
        DECLARE @charvalue varchar (514)
        DECLARE @i int
        DECLARE @length int
        DECLARE @hexstring char(16)
        SELECT @charvalue = '0x'
        SELECT @i = 1
        SELECT @length = DATALENGTH (@binvalue)
        SELECT @hexstring = '0123456789ABCDEF'
        WHILE (@i <= @length)
        BEGIN
            DECLARE @tempint int
            DECLARE @firstint int
            DECLARE @secondint int
            SELECT @tempint = CONVERT(int, SUBSTRING(@binvalue,@i,1))
            SELECT @firstint = FLOOR(@tempint/16)
            SELECT @secondint = @tempint - (@firstint*16)
            SELECT @charvalue = @charvalue +
            SUBSTRING(@hexstring, @firstint+1, 1) +
            SUBSTRING(@hexstring, @secondint+1, 1)
            SELECT @i = @i + 1
        END

        SELECT @hexvalue = @charvalue
        GO
"@
        Invoke-Sqlcmd -ServerInstance $instance -Database master -Query $CreateProc
            }
} Catch { Log "Error checking and creating hexidecimal procedure" "Error" ; Throw }
}

function Write-SQLDatabaseCompatibilityLevel ($instancename, $databasename, $compatibilityLevel)
{
Try
    {
    $CompatabilityLevelSQL = "ALTER DATABASE [$databasename] SET COMPATIBILITY_LEVEL = $compatibilityLevel"
    Log -message "In Write-compatability" -Level Info -WriteToHost
    Log -message $CompatabilityLevelSQL -Level Info  -WriteToHost
    log -message $instancename -Level Info -WriteToHost
    log -message $databasename -Level Info -WriteToHost
    Invoke-Sqlcmd -ServerInstance $instancename -Database master -Query $CompatabilityLevelSQL -AbortOnError
    } Catch { Log "Error setting compatability level" "Error" ; Throw }
}

function Run-DBCCCHECKDB ([string]$Instance, [string]$Database)
{
Try
    {
    Log "Running DBCC.  May take a few minutes.  Use the -NoDBCC parameter to skip...." -WriteToHost
    $DBCCSQL = "DBCC CHECKDB ('$Database') WITH NO_INFOMSGS, ALL_ERRORMSGS, TABLERESULTS;"
    Log $DBCCSQL
    $DBCCResults = Invoke-Sqlcmd -ServerInstance $Instance -Database $Database -Query $DBCCSQL -QueryTimeout 0
    If ($DBCCResults)
        {
        Log "DBCC CHECKDB reported errors on $Database" -Level Info -WriteToHost -ForegroundColour Red
        $DBCCResults | Select-Object Error, MessageText | Out-String
        }
    } Catch {Log "Error" ; Throw }
}

function ListOrphanedUsers ([string]$InstanceName, [string]$DatabaseName)
{
Try
    {
    $OrphanedUsersSQL = @"
    SELECT dp.name AS DatabaseUser, sp.name AS ServerLogin, dp.sid as DatabaseSID, sp.sid AS LoginSid, dp.create_date as UserCreateDate, sp.create_date as LoginCreateDate
FROM sys.database_principals dp
JOIN sys.server_principals sp ON sp.name = dp.name COLLATE DATABASE_DEFAULT 
WHERE dp.sid <> sp.sid
AND sp.type = 'S' AND dp.type = 'S'
"@
    Log -message $OrphanedUsersSQL -Level Info
    $orphans = Invoke-Sqlcmd -ServerInstance $InstanceName -Database $DatabaseName -Query $OrphanedUsersSQL
    Log -message $($orphans | Out-String) -Level Info 
    $orphans
    } Catch { Log "Error getting orphaned users" -Level Error -WriteToHost ;  Throw }
}

function LinkOrphanedUser ([string]$InstanceName, [string]$DatabaseName, [string]$UserName, [switch]$all)
{
Try
    {
    If ($all)
        {
        Log "Linking all orphaned users"
        $ListOrphanedUsers = ListOrphanedUsers -InstanceName $InstanceName -DatabaseName $DatabaseName
        Log "Found $($ListOrphanedUsers.Count) orphaned users" -writetohost
        foreach ($login in ListOrphanedUsers -InstanceName $InstanceName -DatabaseName $DatabaseName)
            {
                $LinkOrphanedUserSQL = "ALTER USER $($login.databaseuser) WITH LOGIN = $($login.databaseuser)"
                Log $LinkOrphanedUserSQL -WriteToHost
                Invoke-Sqlcmd -ServerInstance $InstanceName -Database $DatabaseName  -Query $LinkOrphanedUserSQL
            }
        }
    Else
        {
        $LinkOrphanedUserSQL = "ALTER USER $UserName WITH LOGIN = $UserName"
        Log $LinkOrphanedUserSQL -WriteToHost
        Invoke-Sqlcmd -ServerInstance $InstanceName -Database $DatabaseName  -Query $LinkOrphanedUserSQL
        }
    } Catch { Log "Error linking user $UserName" "Error" ;  Throw }
}

Function IIf($If, $IfTrue, $IfFalse) {
    If ($If -IsNot "Boolean") {$_ = $If}
    If ($If) {If ($IfTrue -is "ScriptBlock") {&$IfTrue} Else {$IfTrue}}
    Else {If ($IfFalse -is "ScriptBlock") {&$IfFalse} Else {$IfFalse}}
}

function ListUsersWithNoLogins ([string]$InstanceName, [string]$DatabaseName)
{
Try
    {
    $NoLoginsSQL = @"
SELECT dp.name FROM sys.database_principals dp
LEFT OUTER JOIN sys.server_principals sp ON sp.sid = dp.sid
WHERE sp.sid IS NULL AND dp.type IN ('S','U') AND dp.name NOT IN ('guest','sys','INFORMATION_SCHEMA')
"@
    $NoLogins = Invoke-Sqlcmd -ServerInstance $InstanceName -Database $DatabaseName -Query $NoLoginsSQL
    Log -message $($NoLogins | Out-String) -Level Info 
    $NoLogins
    }
Catch {  Log "Error getting users without logins" -Level Error -WriteToHost ;  Throw }
}

function get-RowCountsForDatabase ($instance, $database)
{
$rcSQL = @"
CREATE TABLE #counts
(
    table_name varchar(255),
    row_count int
)

EXEC sp_MSForEachTable @command1='INSERT #counts (table_name, row_count) SELECT ''?'', COUNT(*) FROM ?'
SELECT table_name, row_count FROM #counts ORDER BY  row_count DESC
DROP TABLE #counts
"@
Invoke-Sqlcmd -ServerInstance $instance -Database $database -Query $rcSQL}

function Backup (
[string]$Instance, 
[string]$Database, 
[bool]$CompressIfPossible = $false,
[bool]$Verify = $false,
[bool]$CopyOnly = $true,
[bool]$MarkAsRetain = $false,
[bool]$Differential = $false,
[string]$BackupPath,
[switch]$BackupToNul
)
{
Try
    {
    #$dateForFileName = Get-Date -Format "yyyyMMddhhmm"
    $params = $MyInvocation.BoundParameters | Out-String
    $ScriptStartTime = Get-Date
    $ExecutionID = $([GUID]::newGuid()).Guid
    Log "Starting"
    Log $params

     If ($(Get-DatabaseState -instance $Instance -database $Database) -eq "RESTORING")
            {
            Log "Database is in RESTORING state." "Error"
            Throw "Database is in RESTORING state. A database in this state cannot be backed up. Please use command 'RESTORE DATABASE $Database' and try again."
        }
    $Compress = Get-SQLInstanceCompression -instancename $Instance 
    Log "Compression = $Compress"
    If ($BackupToNul)
        {
        $backupLocation = "nul"
        }
    Else
        {
        $backupLocation = Get-BackupLocation -instancename $Instance -databasename $Database -CreateIfNotExist $true -MarkAsRetain $MarkAsRetain -differential $Differential -BackupLocation $BackupPath
        }
    Log $backupLocation
    $jobs = @{}
    $jobs = $jobs + $(Backup-Database -instancename $Instance -databasename $Database -backuppath $backupLocation -compress $CompressIfPossible -JobName "Backup" -CopyOnly $copyonly -ProgressID 1 -Differential $Differential )
    Progress2 -Jobdetailscollection $jobs
    If (!$BackupToNul) { Check-Backup -instance $Instance -Database $Database -BackupLocation $backupLocation -Verify $Verify }
    $mailsubject = "Backup of $Database on $Instance has completed successfully."
    $mailmessage = "The backup can be found at `n`n $backupLocation"
    }
Catch
    {
    Log -message "Error in Backup" -Level "Error"
    $stacktrace = $Error[0].ScriptStackTrace
    $errorDetails = "ERROR: `n`n STACK TRACE: `n`n $stacktrace `n`n"
    $errorDetails = $errorDetails + $($Error[0] | Out-String) -replace "`'", ""
    $errordetails = $errorDetails -replace "`[$]", "X"
    $mailmessage = $errorDetails
    $mailsubject = "Error in Backup of $Database on $Instance." 
    write-host $errorDetails -ForegroundColor Red
    }
Finally
    {
    Get-Job | ? {$_.Name -ne "dbatools_Timer" }| Remove-Job
    Log "Finished"
    SendEMail -Subject $mailsubject -msg $mailmessage 
    }
}

Function Get-LastUpdatedTablesForDatabase ($instance, $Database)
{
$LastUpdateSQL = @"
SELECT 
t.name AS TableName 
,SUM(User_updates) OVER (PARTITION BY i.object_id, i.index_id) AS user_updates
,SUM(ius.user_seeks) OVER(PARTITION BY i.object_id, i.index_id) AS userSeeks
,SUM(ius.user_scans)  OVER(PARTITION By i.object_id, i.index_id) AS UserScans
,SUM(ius.user_lookups)  OVER(PARTITION BY i.object_id, i.index_id) AS UserLookups
,MAX(ius.last_user_update)  OVER(PARTITION BY i.object_id, i.index_id) AS LastUpdate
,(SELECT sqlserver_start_time FROM sys.dm_os_sys_info) AS StartupTime
FROM sys.indexes i 
LEFT OUTER JOIN sys.dm_db_index_usage_stats ius ON ius.index_id = i.index_id AND i.object_id = ius.object_id
JOIN sys.tables t ON t.object_id = i.object_id
WHERE t.is_ms_shipped = 0-- AND i.type = 0 
AND database_id = DB_ID()
ORDER by LastUpdate DESC
"@
Invoke-Sqlcmd -ServerInstance $instance -Database $Database -Query $LastUpdateSQL
}
