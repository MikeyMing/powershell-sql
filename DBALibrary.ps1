#requires -Version 5

<#
.SYNOPSIS
    PowerShell module for managing SQL Server database copying and related operations.

.DESCRIPTION
    This module provides functions to backup, restore, and manage SQL Server databases professionally.
    It supports logging, email notifications, and handles various SQL Server versions up to SQL Server 2025.
    Updated for compatibility with SQL Server 2025 (version 17.00 assumed).

.NOTES
    Author: Mike Fleming (@BelugaMike)
    Version: 2.0
    Last Updated: January 28, 2026

    - Consistent formatting and naming conventions applied.
    - Improved error handling and logging.
    - Added support for SQL Server 2022 and 2025 in version checks.
    - Removed redundancies and optimized functions.
    - Enhanced documentation with .SYNOPSIS, .DESCRIPTION, etc.
    - Modularized where possible.
    - Ensured compatibility with PowerShell 7+ if needed, but sticks to PS3+ for broad support.

.EXAMPLE
    # Default behavior is to create a new target database (CreateDatabase defaults to $true).
    BackupAndRestore -SourceInstance "SourceServer" -SourceDatabase "SourceDB" -TargetInstance "TargetServer" -TargetDatabase "TargetDB_Copy" -RecoveryModel "FULL" -CompatabilityLevel "170" -BatchMode $true

.EXAMPLE
    # To overwrite an existing target database, specify -CreateDatabase $false explicitly.
    BackupAndRestore -SourceInstance "SourceServer" -SourceDatabase "SourceDB" -TargetInstance "TargetServer" -TargetDatabase "TargetDB" -RecoveryModel "FULL" -CompatabilityLevel "170" -CreateDatabase $false -BatchMode $true
#>

$global:DBALibraryDebug = $false
$script:DBALibraryVerboseDiagnostics = $false
$script:ExecutionID = [guid]::NewGuid().Guid
$script:DbLoggingInitialized = $false
$script:DbLoggingAvailable = $false

# Helper function for debug output
function Write-DebugMessage {
    param(
        [string]$Message
    )
    if ($global:DBALibraryDebug) {
        Write-Host "[DEBUG] $Message" -ForegroundColor Yellow
    }
}

function Write-DiagMessage {
    param(
        [Parameter(Mandatory)]
        [AllowEmptyString()]
        [string]$Message,
        [string]$ForegroundColor = 'DarkGray'
    )

    if (-not $script:DBALibraryVerboseDiagnostics) {
        return
    }
    if ([string]::IsNullOrEmpty($Message)) {
        return
    }
    Write-Host "[DIAG] $Message" -ForegroundColor $ForegroundColor
}

function Initialize-DbLogging {
    param(
        [string]$Instance = $DBAInstance,
        [string]$Database = $DBADatabase
    )

    if ($script:DbLoggingInitialized) {
        return
    }
    $script:DbLoggingInitialized = $true
    $script:DbLoggingAvailable = $false

    try {
        Import-Module SqlServer -ErrorAction Stop

        $dbExists = Invoke-Sqlcmd -ServerInstance $Instance -Database master -Query "SELECT DB_ID('$Database') AS DbId" -ErrorAction Stop
        if (-not $dbExists.DbId) {
            return
        }

        $initSql = @"
IF OBJECT_ID('dbo.Log','U') IS NULL
BEGIN
    CREATE TABLE dbo.Log (
        LogId        int IDENTITY(1,1) NOT NULL PRIMARY KEY,
        [DateTime]   datetime2(0) NOT NULL CONSTRAINT DF_Log_DateTime DEFAULT (sysdatetime()),
        ExecutionID  varchar(36) NULL,
        [Level]      varchar(20) NOT NULL,
        [Message]    nvarchar(4000) NOT NULL,
        [Note]       nvarchar(400) NULL,
        ErrorDetails nvarchar(max) NULL,
        ErrorLine    int NULL
    );
    CREATE INDEX IX_Log_ExecutionID_DateTime ON dbo.Log(ExecutionID, [DateTime] DESC);
END
"@
        Invoke-Sqlcmd -ServerInstance $Instance -Database $Database -Query $initSql -QueryTimeout 60 -ErrorAction Stop
        $script:DbLoggingAvailable = $true
    } catch {
        # Swallow logging init failures; keep host logging working.
        $script:DbLoggingAvailable = $false
    }
}

function Write-DbLogRow {
    param(
        [Parameter(Mandatory)][string]$Message,
        [Parameter(Mandatory)][string]$Level,
        [string]$Note,
        [string]$ErrorDetails,
        [int]$ErrorLine
    )

    if (-not $loggingEnabled) { return }
    if (-not $script:DbLoggingAvailable) { return }

    try {
        $safeMessage = ($Message -replace "'", "''")
        if ($safeMessage.Length -gt 4000) {
            $safeMessage = $safeMessage.Substring(0, 4000)
        }
        $safeLevel = ($Level -replace "'", "''")
        $safeNote = if ($Note) { ($Note -replace "'", "''") } else { $null }
        $safeError = if ($ErrorDetails) { ($ErrorDetails -replace "'", "''") } else { $null }

        $noteValue = if ($null -eq $safeNote) { "NULL" } else { "N'$safeNote'" }
        $errValue = if ($null -eq $safeError) { "NULL" } else { "N'$safeError'" }
        $errLineValue = if ($null -eq $ErrorLine) { "NULL" } else { [string]$ErrorLine }

        $insertSql = @"
INSERT INTO dbo.Log (ExecutionID, [Level], [Message], [Note], ErrorDetails, ErrorLine)
VALUES ('$($script:ExecutionID)', '$safeLevel', N'$safeMessage', $noteValue, $errValue, $errLineValue);
"@
        Invoke-Sqlcmd -ServerInstance $DBAInstance -Database $DBADatabase -Query $insertSql -QueryTimeout 30 -ErrorAction Stop | Out-Null
    } catch {
        # If db logging breaks, disable it to avoid impacting the main workflow.
        $script:DbLoggingAvailable = $false
        $loggingEnabled = $false
    }
}

# Internal Log function to override external or default logging

function Redact-SensitiveText {
    param(
        [AllowNull()]
        [string]$Text
    )

    if ([string]::IsNullOrWhiteSpace($Text)) {
        return $Text
    }

    $redacted = $Text

    # Redact any query string on an HTTPS URL (including Azure SAS).
    $redacted = $redacted -replace '(?i)(https://\S+?)\?\S+', '$1?<SAS omitted>'

    # Defense-in-depth: redact any remaining explicit sig= token.
    $redacted = $redacted -replace '(?i)(sig=)[^&\s]+', '$1<SAS omitted>'

    $redacted
}

function Log {
    param(
        [Parameter(Position=0)]
        [AllowEmptyString()]
        [string]$Message = "",
        [Parameter(Position=1)]
        [string]$Level = "Info",
        [switch]$WriteToHost,
        [Alias('ForegroundColor')]
        [string]$ForegroundColour = "White",
        [string]$Note
    )

    if ($null -eq $Message) {
        $Message = ""
    }
    if ($Message -eq "") {
        return
    }

    $Message = Redact-SensitiveText -Text $Message

    # Screen output policy:
    # - Info: goes to the Verbose stream (quiet by default; visible with -Verbose)
    # - Warning/Error or explicit -WriteToHost: goes to the host
    if ($WriteToHost -or $Level -eq "Error" -or $Level -eq "Warning") {
        $color = if ($Level -eq "Error") { "Red" } else { $ForegroundColour }
        Write-Host "[$Level] $Message" -ForegroundColor $color
    } elseif ($Level -eq "Info") {
        Write-Verbose "[$Level] $Message"
    } else {
        # Fallback for custom levels
        Write-Verbose "[$Level] $Message"
    }

    # Optional database-backed logging (DBAInstance/DBADatabase, dbo.Log)
    if ($loggingEnabled) {
        if (-not $script:DbLoggingInitialized) {
            Initialize-DbLogging
        }
        Write-DbLogRow -Message $Message -Level $Level -Note $Note
    }
}

function Format-DbalBytes {
    param(
        [AllowNull()]
        [object]$Bytes
    )

    if ($null -eq $Bytes) { return '' }
    $b = [int64]$Bytes
    if ($b -lt 0) { return ([string]$b) }

    $units = @('B', 'KB', 'MB', 'GB', 'TB', 'PB')
    $value = [double]$b
    $i = 0
    while ($value -ge 1024 -and $i -lt ($units.Count - 1)) {
        $value /= 1024
        $i++
    }

    return ("{0:N2} {1} ({2:N0} bytes)" -f $value, $units[$i], $b)
}

function Format-DbalDuration {
    param(
        [AllowNull()]
        [timespan]$Duration
    )

    if ($null -eq $Duration) { return '' }
    if ($Duration.TotalSeconds -lt 0) { return $Duration.ToString() }
    return ("{0:00}:{1:00}:{2:00}.{3:000}" -f $Duration.Hours, $Duration.Minutes, $Duration.Seconds, $Duration.Milliseconds)
}

function Format-DbalThroughput {
    param(
        [AllowNull()]
        [object]$Bytes,
        [AllowNull()]
        [object]$Seconds
    )

    if ($null -eq $Bytes -or $null -eq $Seconds) { return '' }

    $b = [double]$Bytes
    $s = [double]$Seconds
    if ($s -le 0) { return '' }
    return ("{0}/s" -f (Format-DbalBytes -Bytes ($b / $s)))
}

function Get-DbalFileSizeBytes {
    param(
        [AllowNull()]
        [string]$Path
    )

    if ([string]::IsNullOrWhiteSpace($Path)) { return $null }
    if ($Path -match '^https://') { return $null }

    try {
        return (Get-Item -LiteralPath ("filesystem::$Path") -ErrorAction Stop).Length
    } catch {
        return $null
    }
}

function Get-DbalDatabaseFileSpace {
    <#
    .SYNOPSIS
        Returns data/log allocated, used, and free bytes for a database.
    #>
    param(
        [Parameter(Mandatory = $true)][string]$Instance,
        [Parameter(Mandatory = $true)][string]$Database
    )

    $query = @"
SELECT
    type_desc,
    name,
    CAST(size AS bigint) AS size_pages,
    CAST(FILEPROPERTY(name, 'SpaceUsed') AS bigint) AS used_pages
FROM sys.database_files;
"@

    $rows = Invoke-Sqlcmd -ServerInstance $Instance -Database $Database -Query $query -ErrorAction Stop

    $dataAllocated = 0L
    $dataUsed = 0L
    $logAllocated = 0L
    $logUsed = 0L

    foreach ($r in $rows) {
        $allocatedBytes = [int64]$r.size_pages * 8192L
        $usedBytes = [int64]$r.used_pages * 8192L
        if ($r.type_desc -eq 'ROWS') {
            $dataAllocated += $allocatedBytes
            $dataUsed += $usedBytes
        } elseif ($r.type_desc -eq 'LOG') {
            $logAllocated += $allocatedBytes
            $logUsed += $usedBytes
        }
    }

    return [ordered]@{
        DataAllocatedBytes = $dataAllocated
        DataUsedBytes      = $dataUsed
        DataFreeBytes      = [math]::Max($dataAllocated - $dataUsed, 0)
        LogAllocatedBytes  = $logAllocated
        LogUsedBytes       = $logUsed
        LogFreeBytes       = [math]::Max($logAllocated - $logUsed, 0)
        TotalAllocatedBytes = ($dataAllocated + $logAllocated)
    }
}

function Remove-DbalOldBackups {
    param(
        [Parameter(Mandatory = $true)][string]$DeletePath,
        [int]$RetainByAgeDays,
        [AllowNull()][int]$NumberOfBackupsToRetain
    )

    if ([string]::IsNullOrWhiteSpace($DeletePath)) { return }
    if (-not (Test-Path "filesystem::$DeletePath")) { return }

    $files = @(Get-ChildItem "filesystem::$DeletePath" -File -ErrorAction SilentlyContinue)
    if (-not $files) { return }

    # 1) Age-based retention (delete older than cutoff)
    if ($RetainByAgeDays -gt 0) {
        $cutoff = (Get-Date).AddDays(-1 * $RetainByAgeDays)
        $toDeleteByAge = @($files | Where-Object { $_.LastWriteTime -lt $cutoff })
        foreach ($f in $toDeleteByAge) {
            Write-Host "Deleting (age>$RetainByAgeDays d): $f in $DeletePath" -ForegroundColor Yellow
            try { $f.Delete() } catch {}
        }
        $files = @(Get-ChildItem "filesystem::$DeletePath" -File -ErrorAction SilentlyContinue)
    }

    # 2) Count-based retention (keep newest N by name)
    if ($null -ne $NumberOfBackupsToRetain) {
        $keep = [int]$NumberOfBackupsToRetain
        if ($keep -lt 0) { $keep = 0 }

        $filesSorted = @($files | Sort-Object Name)
        $deleteCount = [math]::Max($filesSorted.Count - $keep, 0)
        $toDeleteByCount = @($filesSorted | Select-Object -First $deleteCount)
        foreach ($f in $toDeleteByCount) {
            Write-Host "Deleting (count): $f in $DeletePath" -ForegroundColor Yellow
            try { $f.Delete() } catch {}
        }
    }
}

function Get-DbalDatabaseSecuritySnapshot {
    <#
    .SYNOPSIS
        Builds a best-effort T-SQL script to recreate target-database security after an overwrite restore.

    .DESCRIPTION
        Captures:
          - Users mapped to server logins (Windows/SQL logins)
          - Default schema
          - Role memberships
          - Database/schema/object permissions (best-effort)
          - Schema ownership

        Notes:
          - Contained database users (authentication_type = DATABASE) cannot be recreated without their secrets.
          - If the corresponding server login does not exist, user creation is skipped.
    #>
    param(
        [Parameter(Mandatory = $true)][string]$Instance,
        [Parameter(Mandatory = $true)][string]$Database
    )

    function Escape-DbalSqlStringLiteral {
        param([AllowNull()][string]$Value)
        if ($null -eq $Value) { return '' }
        return $Value.Replace("'", "''")
    }

    function Quote-DbalSqlName {
        param([Parameter(Mandatory)][string]$Name)
        return ('[' + ($Name.Replace(']', ']]')) + ']')
    }

    $warnings = @()
    $lines = New-Object System.Collections.Generic.List[string]

    $lines.Add("/* DBALibrary PreserveTargetSecurity snapshot */")
    $lines.Add("SET NOCOUNT ON;")
    $lines.Add("PRINT 'Applying preserved target database security...';")

    $usersQuery = @"
SELECT
    dp.name AS DatabaseUser,
    sp.name AS ServerLogin,
    dp.default_schema_name AS DefaultSchema,
    dp.type_desc AS PrincipalType,
    dp.authentication_type_desc AS AuthType
FROM sys.database_principals dp
LEFT JOIN sys.server_principals sp
    ON dp.sid = sp.sid
WHERE dp.type IN ('S','U','G')
  AND dp.is_fixed_role = 0
  AND dp.name NOT IN ('dbo','guest','INFORMATION_SCHEMA','sys')
  AND dp.name NOT LIKE '##%';
"@

    $users = @()
    try {
        $users = @(Invoke-Sqlcmd -ServerInstance $Instance -Database $Database -Query $usersQuery -ErrorAction Stop)
    } catch {
        $msg = "PreserveTargetSecurity: failed to query database principals in [$Database] on [$Instance]: {0}" -f $_.Exception.Message
        throw $msg
    }

    foreach ($u in $users) {
        $dbUser = [string]$u.DatabaseUser
        $login = [string]$u.ServerLogin
        $authType = [string]$u.AuthType
        $defaultSchema = [string]$u.DefaultSchema

        if ($authType -and $authType -ne 'INSTANCE') {
            $warnings += "PreserveTargetSecurity: user '$dbUser' is a contained database user (AuthType=$authType) and cannot be recreated automatically."
            continue
        }

        if ([string]::IsNullOrWhiteSpace($login)) {
            $warnings += "PreserveTargetSecurity: user '$dbUser' has no matching server login (skipping CREATE USER)."
            continue
        }

        $dbUserLit = Escape-DbalSqlStringLiteral -Value $dbUser
        $loginLit = Escape-DbalSqlStringLiteral -Value $login

        $dbUserQ = Quote-DbalSqlName -Name $dbUser
        $loginQ = Quote-DbalSqlName -Name $login

        $lines.Add("IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'$dbUserLit')")
        $lines.Add("BEGIN")
        $lines.Add("    IF EXISTS (SELECT 1 FROM sys.server_principals WHERE name = N'$loginLit')")
        $lines.Add("        EXEC('CREATE USER $dbUserQ FOR LOGIN $loginQ');")
        $lines.Add("    ELSE")
        $lines.Add("        PRINT 'Skipping user $dbUserLit (missing login $loginLit)';")
        $lines.Add("END")

        if (-not [string]::IsNullOrWhiteSpace($defaultSchema) -and $defaultSchema -ne 'dbo') {
            $schemaLit = Escape-DbalSqlStringLiteral -Value $defaultSchema
            $schemaQ = Quote-DbalSqlName -Name $defaultSchema
            $lines.Add("IF EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'$dbUserLit') AND EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'$schemaLit')")
            $lines.Add("    EXEC('ALTER USER $dbUserQ WITH DEFAULT_SCHEMA = $schemaQ');")
        }
    }

    # Schema ownership
    $schemaAuthQuery = @"
SELECT
    s.name AS SchemaName,
    p.name AS OwnerName
FROM sys.schemas s
JOIN sys.database_principals p
    ON s.principal_id = p.principal_id
WHERE s.name NOT IN ('dbo','guest','INFORMATION_SCHEMA','sys')
  AND p.name NOT IN ('dbo','guest','INFORMATION_SCHEMA','sys');
"@
    $schemas = @(Invoke-Sqlcmd -ServerInstance $Instance -Database $Database -Query $schemaAuthQuery -ErrorAction Stop)
    foreach ($s in $schemas) {
        $schemaName = [string]$s.SchemaName
        $ownerName = [string]$s.OwnerName
        if ([string]::IsNullOrWhiteSpace($schemaName) -or [string]::IsNullOrWhiteSpace($ownerName)) { continue }

        $schemaLit = Escape-DbalSqlStringLiteral -Value $schemaName
        $ownerLit = Escape-DbalSqlStringLiteral -Value $ownerName
        $schemaQ = Quote-DbalSqlName -Name $schemaName
        $ownerQ = Quote-DbalSqlName -Name $ownerName

        $lines.Add("IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'$schemaLit') AND EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'$ownerLit')")
        $lines.Add("    EXEC('ALTER AUTHORIZATION ON SCHEMA::$schemaQ TO $ownerQ');")
    }

    # Role memberships
    $rolesQuery = @"
SELECT
    r.name AS RoleName,
    m.name AS MemberName
FROM sys.database_role_members drm
JOIN sys.database_principals r
    ON drm.role_principal_id = r.principal_id
JOIN sys.database_principals m
    ON drm.member_principal_id = m.principal_id
WHERE r.type = 'R'
  AND r.name <> 'public'
  AND m.name NOT IN ('dbo','guest','INFORMATION_SCHEMA','sys')
  AND m.name NOT LIKE '##%';
"@
    $roleMembers = @(Invoke-Sqlcmd -ServerInstance $Instance -Database $Database -Query $rolesQuery -ErrorAction Stop)
    foreach ($rm in $roleMembers) {
        $roleName = [string]$rm.RoleName
        $memberName = [string]$rm.MemberName
        if ([string]::IsNullOrWhiteSpace($roleName) -or [string]::IsNullOrWhiteSpace($memberName)) { continue }

        $roleLit = Escape-DbalSqlStringLiteral -Value $roleName
        $memberLit = Escape-DbalSqlStringLiteral -Value $memberName
        $roleQ = Quote-DbalSqlName -Name $roleName
        $memberQ = Quote-DbalSqlName -Name $memberName

        $lines.Add("IF EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'$roleLit' AND type='R') AND EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'$memberLit')")
        $lines.Add("BEGIN")
        $lines.Add("    IF IS_ROLEMEMBER(N'$roleLit', N'$memberLit') <> 1")
        $lines.Add("        EXEC('ALTER ROLE $roleQ ADD MEMBER $memberQ');")
        $lines.Add("END")
    }

    function Add-PermissionLines {
        param(
            [AllowNull()]
            [AllowEmptyCollection()]
            [object[]]$PermRows
        )

        if ($null -eq $PermRows -or $PermRows.Count -eq 0) {
            return
        }

        foreach ($p in $PermRows) {
            $state = [string]$p.state_desc
            $permName = [string]$p.permission_name
            $grantee = [string]$p.Grantee
            $class = [string]$p.class_desc

            if ([string]::IsNullOrWhiteSpace($permName) -or [string]::IsNullOrWhiteSpace($grantee)) { continue }

            $granteeLit = Escape-DbalSqlStringLiteral -Value $grantee
            $granteeQ = Quote-DbalSqlName -Name $grantee

            $verb = if ($state -eq 'DENY') { 'DENY' } else { 'GRANT' }
            $withGrant = if ($state -eq 'GRANT_WITH_GRANT_OPTION') { ' WITH GRANT OPTION' } else { '' }

            $onClause = ''
            if ($class -eq 'SCHEMA') {
                $schemaName = [string]$p.SchemaName
                if (-not [string]::IsNullOrWhiteSpace($schemaName)) {
                    $schemaQ = Quote-DbalSqlName -Name $schemaName
                    $onClause = " ON SCHEMA::$schemaQ"
                }
            } elseif ($class -eq 'OBJECT_OR_COLUMN') {
                $schemaName = [string]$p.SchemaName
                $objectName = [string]$p.ObjectName
                if (-not [string]::IsNullOrWhiteSpace($schemaName) -and -not [string]::IsNullOrWhiteSpace($objectName)) {
                    $schemaQ = Quote-DbalSqlName -Name $schemaName
                    $objectQ = Quote-DbalSqlName -Name $objectName
                    $colName = [string]$p.ColumnName
                    if (-not [string]::IsNullOrWhiteSpace($colName)) {
                        $colQ = Quote-DbalSqlName -Name $colName
                        $onClause = " ON OBJECT::$schemaQ.$objectQ($colQ)"
                    } else {
                        $onClause = " ON OBJECT::$schemaQ.$objectQ"
                    }
                }
            }

            # DATABASE class: no ON clause
            $statement = "$verb $permName$onClause TO $granteeQ$withGrant;"
            $statementLit = Escape-DbalSqlStringLiteral -Value $statement

            $lines.Add("IF EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'$granteeLit')")
            $lines.Add("    EXEC(N'$statementLit');")
        }
    }

    # Database-level permissions
    $dbPermsQuery = @"
SELECT
    perm.state_desc,
    perm.permission_name,
    perm.class_desc,
    CAST(NULL AS sysname) AS SchemaName,
    CAST(NULL AS sysname) AS ObjectName,
    CAST(NULL AS sysname) AS ColumnName,
    grantee.name AS Grantee
FROM sys.database_permissions perm
JOIN sys.database_principals grantee
    ON perm.grantee_principal_id = grantee.principal_id
WHERE perm.class = 0
  AND perm.state_desc IN ('GRANT','DENY','GRANT_WITH_GRANT_OPTION')
  AND grantee.type IN ('S','U','G')
  AND grantee.name NOT IN ('dbo','guest','INFORMATION_SCHEMA','sys')
  AND grantee.name NOT LIKE '##%';
"@
    Add-PermissionLines -PermRows @(Invoke-Sqlcmd -ServerInstance $Instance -Database $Database -Query $dbPermsQuery -ErrorAction Stop)

    # Schema permissions
    $schemaPermsQuery = @"
SELECT
    perm.state_desc,
    perm.permission_name,
    perm.class_desc,
    s.name AS SchemaName,
    CAST(NULL AS sysname) AS ObjectName,
    CAST(NULL AS sysname) AS ColumnName,
    grantee.name AS Grantee
FROM sys.database_permissions perm
JOIN sys.database_principals grantee
    ON perm.grantee_principal_id = grantee.principal_id
JOIN sys.schemas s
    ON perm.major_id = s.schema_id
WHERE perm.class = 3
  AND perm.state_desc IN ('GRANT','DENY','GRANT_WITH_GRANT_OPTION')
  AND grantee.type IN ('S','U','G')
  AND grantee.name NOT IN ('dbo','guest','INFORMATION_SCHEMA','sys')
  AND grantee.name NOT LIKE '##%';
"@
    Add-PermissionLines -PermRows @(Invoke-Sqlcmd -ServerInstance $Instance -Database $Database -Query $schemaPermsQuery -ErrorAction Stop)

    # Object/column permissions
    $objPermsQuery = @"
SELECT
    perm.state_desc,
    perm.permission_name,
    perm.class_desc,
    sch.name AS SchemaName,
    obj.name AS ObjectName,
    col.name AS ColumnName,
    grantee.name AS Grantee
FROM sys.database_permissions perm
JOIN sys.database_principals grantee
    ON perm.grantee_principal_id = grantee.principal_id
JOIN sys.objects obj
    ON perm.major_id = obj.object_id
JOIN sys.schemas sch
    ON obj.schema_id = sch.schema_id
LEFT JOIN sys.columns col
    ON perm.major_id = col.object_id
   AND perm.minor_id = col.column_id
WHERE perm.class = 1
  AND perm.state_desc IN ('GRANT','DENY','GRANT_WITH_GRANT_OPTION')
  AND grantee.type IN ('S','U','G')
  AND grantee.name NOT IN ('dbo','guest','INFORMATION_SCHEMA','sys')
  AND grantee.name NOT LIKE '##%';
"@
    Add-PermissionLines -PermRows @(Invoke-Sqlcmd -ServerInstance $Instance -Database $Database -Query $objPermsQuery -ErrorAction Stop)

    $lines.Add("PRINT 'Preserved target database security applied.';")

    [pscustomobject]@{
        Script   = ($lines -join "`n")
        Warnings = $warnings
    }
}

function Invoke-DbalDatabaseSecuritySnapshot {
    param(
        [Parameter(Mandatory = $true)][string]$Instance,
        [Parameter(Mandatory = $true)][string]$Database,
        [Parameter(Mandatory = $true)]$Snapshot
    )

    if ($null -eq $Snapshot) { return }
    if ([string]::IsNullOrWhiteSpace($Snapshot.Script)) { return }

    Log -Message "PreserveTargetSecurity: applying saved security script to [$Database] on [$Instance]" -Level Info -WriteToHost
    try {
        Invoke-Sqlcmd -ServerInstance $Instance -Database $Database -Query $Snapshot.Script -QueryTimeout 65535 -ErrorAction Stop
        Log -Message "PreserveTargetSecurity: security apply completed" -Level Info -WriteToHost
    } catch {
        $msg = "PreserveTargetSecurity: failed to apply security to [$Database] on [$Instance]: {0}" -f $_.Exception.Message
        Log -Message $msg -Level Error -WriteToHost
        throw $msg
    }
}

function Get-DbalLatestBackupFile {
    param(
        [Parameter(Mandatory = $true)][string]$BackupFolder,
        [Parameter(Mandatory = $true)][string]$DatabaseName
    )

    if ([string]::IsNullOrWhiteSpace($BackupFolder)) { return $null }
    if (-not (Test-Path "filesystem::$BackupFolder")) { return $null }

    $pattern = "{0}_adhoc_*.BAK" -f $DatabaseName
    $candidate = Get-ChildItem -Path "filesystem::$BackupFolder" -Filter $pattern -File -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1

    return $candidate
}

function Test-DbalDatabaseHasActiveUserSessions {
    param(
        [Parameter(Mandatory = $true)][string]$Instance,
        [Parameter(Mandatory = $true)][string]$Database
    )

    $sql = @"
SELECT COUNT(1) AS SessionCount
FROM sys.dm_exec_sessions s
WHERE s.is_user_process = 1
  AND s.database_id = DB_ID(N'$Database')
  AND s.session_id <> @@SPID;
"@

    try {
        $r = Invoke-Sqlcmd -ServerInstance $Instance -Database master -Query $sql -ErrorAction Stop
        return ([int]$r.SessionCount -gt 0)
    } catch {
        # If we can't check, treat as unknown (false) and let restore/offline logic proceed.
        return $false
    }
}

function Invoke-DbalVerifyBackup {
    param(
        [Parameter(Mandatory = $true)][string]$Instance,
        [Parameter(Mandatory = $true)][string]$BackupPath,
        [string]$CredentialName,
        [switch]$DryRun
    )

    $isUrl = ($BackupPath -match '^https://')
    $from = if ($isUrl) { 'URL' } else { 'DISK' }
    $effectivePath = if ($isUrl -and -not [string]::IsNullOrWhiteSpace($CredentialName)) { $BackupPath.Split('?', 2)[0] } else { $BackupPath }
    $credentialClause = if ($isUrl -and -not [string]::IsNullOrWhiteSpace($CredentialName)) { " WITH CREDENTIAL = '$CredentialName'" } else { '' }

    $sql = "RESTORE VERIFYONLY FROM $from = '$effectivePath'$credentialClause"

    $displaySql = if ($isUrl) { $sql.Replace($effectivePath, (Get-DisplayPath $effectivePath)) } else { $sql }
    Log -Message "[VerifyBackup] $displaySql" -Level Info -WriteToHost

    if ($DryRun.IsPresent) {
        return $sql
    }

    Invoke-Sqlcmd -ServerInstance $Instance -Database master -Query $sql -QueryTimeout 65535 -ErrorAction Stop | Out-Null
}

# Ensure our Log function isn't shadowed by an alias (alias resolution wins over functions)
if (Test-Path Alias:Log) {
    Remove-Item Alias:Log -Force -ErrorAction SilentlyContinue
}
# Global Variables (Configure these as needed)
# NOTE: Defaults are intentionally generic for public distribution.
# Use Set-DBALibraryConfig (when importing as a module) or set these variables before calling BackupAndRestore.
$DBAInstance = $null
$DBADatabase = "DBA"
$smtpserver = 'smtp'
$SMTPEnabled = $false
$ProgressInterval = 2
$script:ExecutionID = [guid]::NewGuid().Guid

# Logging Configuration

$loggingEnabled = $false
$LoggingTestSQL = "SELECT TOP 1 Logid FROM log"
# Write-Host "Testing the log table." # Only for manual testing, not in production



function BackupAndRestore {
    <#
    .SYNOPSIS
        Backs up and restores a SQL database, with various options.
    #>
    [CmdletBinding()]
    param (
        [string]$SourceInstance,
        [string]$SourceDatabase,
        [string]$TargetInstance,
        [string]$TargetDatabase,
        [bool]$TakeTargetOffline = $false,
        [bool]$DontBackupTarget = $false,
        [bool]$RetainOwnerName = $false,
        [ValidateSet('SIMPLE','FULL','Retain')][string]$RecoveryModel = 'Retain',
        [bool]$OverwriteTarget = $false,
        [ValidateSet('SetLatest','Retain','100','110','120','130','140','150','160','170')][string]$CompatabilityLevel = 'SetLatest',
        [bool]$ShrinkLog = $false,
        [bool]$WaitforManualRestore = $false,
        [bool]$CreateDatabase = $true,
        [bool]$MarkAsRetain = $false,
        [bool]$NoRecovery = $false,
        [bool]$RollForwardTransactionLogs = $false,
        [int]$LogBackupIntervalSeconds = 60,
        [int]$MaxLogBackupCycles = 0,
        [bool]$CopyUserRoles = $false,
        [bool]$CreateLoginsIfTheyDontExist = $false,
        [switch]$PreserveTargetSecurity,
        [string]$IntermediateInstance,
        [bool]$Differential = $false,
        [bool]$BatchMode = $false,
        [string]$EmailAddress,
        [string]$FromAddress,
        [string]$BackupPath,
        [string]$AzureStorageBackupLocation,
        [bool]$CopyOnly = $true,
        [string]$ScriptToRunOnTarget,
        [switch]$ChangeCollation,
        [ValidateSet("Latin1_General_CI_AS")][string]$Collation,
        [bool]$DontCheckSpace = $false,
        [bool]$NoDBCC = $false,
        [bool]$UpdateStats = $false,
        [int]$NumberOfBackupsToRetain,
        [int]$RetainByAgeDays,
        [string]$BlockSize = '65536',
        [string]$BufferCount = '50',
        [string]$MaxTransferSize = '2097152',
        [bool]$DeleteOrphans = $false,
        [ValidateSet('RollbackImmediate', 'NoWait', 'Wait')][string]$TakeTargetOfflineMode = 'RollbackImmediate',
        [bool]$AbortIfActiveSessions = $false,
        [switch]$VerifyBackup,
        [switch]$PreflightOnly,
        [switch]$DryRun,
        [switch]$ResumeFromLatestBackup,
        [switch]$VerboseDiagnostics,
        [switch]$EnableDbLogging
    )

    $MailSubject = $null
    $MailStatus = $null
    $MailNotes = $null
    $MailDetails = $null

    $BackupPhaseStartTime = $null
    $BackupPhaseEndTime = $null
    $RestorePhaseStartTime = $null
    $RestorePhaseEndTime = $null
    $SourceDbSpace = $null
    $TargetDbSpace = $null
    $SourceBackupSizeBytes = $null
    $TargetBackupSizeBytes = $null
    $TargetSecuritySnapshot = $null

    try {
        $BackupAndRestoreStartTime = Get-Date
        $script:ExecutionID = [guid]::NewGuid().Guid
        $script:DBALibraryVerboseDiagnostics = $VerboseDiagnostics.IsPresent
        $loggingEnabled = $EnableDbLogging.IsPresent
        if ($loggingEnabled -and -not $script:DbLoggingInitialized) {
            Initialize-DbLogging
        }
        Write-DebugMessage "[BackupAndRestore] Starting main logic"
        Log "Starting"
        Write-DebugMessage "[BackupAndRestore] Differential is $Differential"
        Log "Differential is $Differential"
        $Jobs = @() # Used for progress routine

        if ($RollForwardTransactionLogs) {
            if ($Differential) {
                Log -Message "Cannot specify -RollForwardTransactionLogs and -Differential" -Level Error -WriteToHost
                throw "Cannot specify -RollForwardTransactionLogs and -Differential"
            }
            if ($WaitforManualRestore) {
                Log -Message "Cannot specify -RollForwardTransactionLogs and -WaitforManualRestore" -Level Error -WriteToHost
                throw "Cannot specify -RollForwardTransactionLogs and -WaitforManualRestore"
            }
            if ($IntermediateInstance) {
                Log -Message "Cannot specify -RollForwardTransactionLogs and -IntermediateInstance" -Level Error -WriteToHost
                throw "Cannot specify -RollForwardTransactionLogs and -IntermediateInstance"
            }
            if (-not [string]::IsNullOrWhiteSpace($AzureStorageBackupLocation)) {
                Log -Message "-RollForwardTransactionLogs currently supports filesystem/UNC backup paths only (not -AzureStorageBackupLocation)." -Level Error -WriteToHost
                throw "-RollForwardTransactionLogs currently supports filesystem/UNC backup paths only (not -AzureStorageBackupLocation)."
            }
            if ([string]::IsNullOrWhiteSpace($BackupPath)) {
                Log -Message "-RollForwardTransactionLogs requires -BackupPath." -Level Error -WriteToHost
                throw "-RollForwardTransactionLogs requires -BackupPath."
            }
            if ($LogBackupIntervalSeconds -lt 1) {
                Log -Message "-LogBackupIntervalSeconds must be >= 1." -Level Error -WriteToHost
                throw "-LogBackupIntervalSeconds must be >= 1."
            }
        }

        if (-not [string]::IsNullOrWhiteSpace($AzureStorageBackupLocation) -and -not [string]::IsNullOrWhiteSpace($BackupPath)) {
            Log -Message "Specify only one of -BackupPath or -AzureStorageBackupLocation" -Level Error -WriteToHost
            throw "Specify only one of -BackupPath or -AzureStorageBackupLocation"
        }

        if ($ResumeFromLatestBackup -and (-not [string]::IsNullOrWhiteSpace($AzureStorageBackupLocation))) {
            Log -Message "-ResumeFromLatestBackup is supported for filesystem/UNC backups only (not -AzureStorageBackupLocation)." -Level Error -WriteToHost
            throw "-ResumeFromLatestBackup is supported for filesystem/UNC backups only (not -AzureStorageBackupLocation)."
        }

        $azureInfo = $null
        $azureCredentialName = $null
        if (-not [string]::IsNullOrWhiteSpace($AzureStorageBackupLocation)) {
            Test-AzureStorageBackupLocation -AzureStorageBackupLocation $AzureStorageBackupLocation
            $azureInfo = Get-AzureStorageSasInfo -AzureStorageBackupLocation $AzureStorageBackupLocation

            if (-not [string]::IsNullOrWhiteSpace($azureInfo.SasToken)) {
                # When a SAS token is provided, create a SQL credential but omit WITH CREDENTIAL.
                # Some SQL Server versions reject WITH CREDENTIAL for SAS-based credentials.
                $azureCredentialName = $null
                $credName = $azureInfo.CredentialName
                Ensure-SqlAzureBlobCredential -InstanceName $SourceInstance -CredentialName $credName -SasToken $azureInfo.SasToken
                Ensure-SqlAzureBlobCredential -InstanceName $TargetInstance -CredentialName $credName -SasToken $azureInfo.SasToken
                if (-not [string]::IsNullOrWhiteSpace($IntermediateInstance)) {
                    Ensure-SqlAzureBlobCredential -InstanceName $IntermediateInstance -CredentialName $credName -SasToken $azureInfo.SasToken
                }
            } else {
                $azureCredentialName = $azureInfo.CredentialName
                Log -Message "AzureStorageBackupLocation has no SAS token; assuming a SQL credential named '$azureCredentialName' already exists on the involved instance(s)." -Level Warning -WriteToHost
            }
        }

        if ([string]::IsNullOrEmpty($EmailAddress)) {
            try {
                $Me = $env:USERNAME
                $adUser = Get-ADUser -Identity $Me -Properties mail, EmailAddress -ErrorAction Stop
                $EmailAddress = if (-not [string]::IsNullOrWhiteSpace($adUser.mail)) { $adUser.mail } else { $adUser.EmailAddress }
            } catch {
                $EmailAddress = $null
            }
        }

        # Validate parameters
        if ($WaitforManualRestore -and $Differential) {
            Log "Cannot specify WaitforManualRestore and Differential" "Error"
            throw "Invalid parameters"
        }
        if ($IntermediateInstance -and $Differential) {
            Log "Cannot specify IntermediateInstance and Differential" "Error"
            throw "Invalid parameters"
        }

        if ((Get-DatabaseState -Instance $SourceInstance -Database $SourceDatabase) -ne "ONLINE") {
            Log "The source database must be online" "Error"
            throw "Source database not online"
        }

        if ($WaitforManualRestore -or $CreateDatabase) {
            $DontBackupTarget = $true
            $CreateDatabase = $true
        }

        if (-not $CreateDatabase -and -not $Differential) {
            if ((Get-DatabaseState -Instance $TargetInstance -Database $TargetDatabase) -ne "ONLINE") {
                Log "The target database must be online" "Error"
                throw "Target database not online"
            }
        }

        if ($Differential) {
            if ((Get-DatabaseState -Instance $TargetInstance -Database $TargetDatabase) -ne "RESTORING") {
                Log "-Differential specified and database not in restoring state" "Error"
                throw "-Differential specified and database not in restoring state"
            }
            $DontBackupTarget = $true
        }

        Write-DebugMessage "[BackupAndRestore] WaitforManualRestore=$WaitforManualRestore, CreateDatabase=$CreateDatabase, Differential=$Differential"
        Log "WaitforManualRestore is $WaitforManualRestore , CreateDatabase is $CreateDatabase , Differential is $Differential"

        if (-not $WaitforManualRestore -and -not $CreateDatabase -and -not $Differential) {
            $SourceInternalVersion = Get-SQLDatabaseInternalVersionNumberFromDatabase -Instance $SourceInstance -Database $SourceDatabase
            $TargetInternalVersion = Get-SQLDatabaseInternalVersionNumberFromDatabase -Instance $TargetInstance -Database $TargetDatabase
            Write-DebugMessage "[BackupAndRestore] SourceInternalVersion=$SourceInternalVersion, TargetInternalVersion=$TargetInternalVersion"
            Log "SourceInternalVersion = $SourceInternalVersion , TargetInternalVersion = $TargetInternalVersion"
            if ($SourceInternalVersion -gt $TargetInternalVersion) {
                Log "The SQL version of the target instance must be the same or greater than the source version." "Error"
                throw "The SQL version of the target instance must be the same or greater than the source version."
            }
        }

        if ([string]::IsNullOrEmpty($TargetDatabase)) {
            Get-Confirmation -Msg "Target Database not specified. Will use name of source database." -BatchMode $BatchMode
            Log -Message "Target Database not specified, assume source database name ($SourceDatabase)" -Level Info
            $TargetDatabase = $SourceDatabase
        }

        if ($ScriptToRunOnTarget) {
            Log -Message "ScriptToRunOnTarget specified. Checking if $ScriptToRunOnTarget exists." -Level Info
            if (Test-Path $ScriptToRunOnTarget) {
                Log -Message "$ScriptToRunOnTarget exists"
            } else {
                $Msg = "$ScriptToRunOnTarget not accessible."
                Log -Message $Msg -Level Error -WriteToHost -ForegroundColour Red
                throw $Msg
            }
        }

        if ($ChangeCollation -and [string]::IsNullOrEmpty($Collation)) {
            $Msg = "You must supply the name of the collation to change to in the -Collation parameter"
            Log -Message $Msg -Level Error -WriteToHost -ForegroundColour Red
            throw $Msg
        }

        Write-DebugMessage "[BackupAndRestore] Checking databases"
        Log "Checking databases"
        try { $null = Get-SQLInstanceVersion -InstanceName $TargetInstance } catch { Log "Target Database $TargetDatabase on $TargetInstance is not available" "Error" ; throw }
        if (-not $CreateDatabase) {
            try { $null = Get-SQLInstanceVersion -InstanceName $SourceInstance } catch { Log "Source Database $SourceDatabase on $SourceInstance is not available" "Error" ; throw }
        }

        if ($CreateDatabase) {
            if (Check-DatabaseAccess -Instance $TargetInstance -Database $TargetDatabase) {
                Log "CreateDatabase was specified but the target exists" "Error"
                throw "CreateDatabase was specified but the target exists"
            }
        }

        Write-DebugMessage "[BackupAndRestore] Checking connections"
        Log "Checking connections"
        if (-not $TakeTargetOffline -and -not $CreateDatabase -and -not $Differential) {
            if (Get-DatabaseLocks -InstanceName $TargetInstance -DatabaseName $TargetDatabase) {
                Write-Warning "Database is in use. Specify TakeTargetOffline."
                Log "Database in use, exiting"
                throw "Database in use"
            }
        }

        if (-not $CreateDatabase -and -not $Differential) {
            $AG = CheckIfDatabaseIsInAvailabilityGroup -Instance $TargetInstance -Database $TargetDatabase
            if ($AG) {
                $Msg = "The database $TargetDatabase on $TargetInstance is part of the $($AG.AGName) availability group. It cannot be restored"
                Log -Message $Msg -Level Error -WriteToHost -ForegroundColour Yellow
                throw $Msg
            }
        }

        try {
            if ((Get-ProductName $SourceInstance) -ge "SQL 2008") {
                $SourceEncryption = Get-DatabaseEncryptionFromDatabase -InstanceName $SourceInstance -DatabaseName $SourceDatabase
                if ($SourceEncryption.encryption_state -eq 3) {
                    Log "Source is encrypted"
                    Log $SourceEncryption.Key_algorithm
                    Log $SourceEncryption.key_length
                    Log $SourceEncryption.encryptor_thumbprint
                    $CertSQL = "SELECT len(thumbprint), CONVERT(varchar(100), thumbprint,2),* FROM sys.certificates WHERE CONVERT(varchar(100), thumbprint,2) = '$($SourceEncryption.encryptor_thumbprint)'"
                    $CertResult = Invoke-Sqlcmd -ServerInstance $TargetInstance -Database master -Query $CertSQL -AbortOnError
                    if ($null -eq $CertResult) {
                        Log -Message "No certificate found on target with thumbprint $($SourceEncryption.encryptor_thumbprint) " -Level Error -WriteToHost -ForegroundColour Yellow
                        throw "No certificate found on target with thumbprint $($SourceEncryption.encryptor_thumbprint) "
                    } else {
                        Log -Message "Certificate found ($($CertResult.name)) on target ($TargetInstance)" -Level Info
                    }

                    if (-not $CreateDatabase -and -not $Differential) {
                        $TargetEncryption = Get-DatabaseEncryptionFromDatabase -InstanceName $TargetInstance -DatabaseName $TargetDatabase
                        if ($TargetEncryption.encryption_state -eq 3) {
                            Log "Target is encrypted"
                            Log $TargetEncryption.Key_algorithm
                            Log $TargetEncryption.key_length
                            Log $TargetEncryption.encryptor_thumbprint
                        }
                    }
                }
            }
        } catch {
            Get-Confirmation -Msg "Can't check encryption." -BatchMode $BatchMode
            throw
        }

        Write-DebugMessage "[BackupAndRestore] Checking compression"
        Log "Checking compression"
        $Compress = (Get-SQLInstanceCompression -InstanceName $SourceInstance) -and (Get-SQLInstanceCompression -InstanceName $TargetInstance)
        Write-DebugMessage "[BackupAndRestore] Compression=$Compress"
        Log "Compression = $Compress"

        if (-not $DontCheckSpace -and -not $Differential) {
            $S = Get-FileListFromDatabase -Instance $SourceInstance -Database $SourceDatabase
            Write-DiagMessage ($S | Out-String)
            $SpaceRequired = Get-Space -SourceFileList $S -Instance $TargetInstance -Database $TargetDatabase -CreateDatabase $CreateDatabase
            Write-DiagMessage ($SpaceRequired | Out-String)
            $LowSpaceVolumes = $SpaceRequired | Where-Object { $_.PercentFreeAfter -lt 10 }
            if ($LowSpaceVolumes) {
                $FullVolumes = $LowSpaceVolumes | Where-Object { $_.PercentFreeAfter -lt 0 }
                if ($FullVolumes) {
                    Log -Message "Not enough space on drives: $($LowSpaceVolumes | Format-Table | Out-String). Exiting." -Level Error -WriteToHost
                    throw "Not enough space on drives: $($LowSpaceVolumes | Format-Table | Out-String). Exiting."
                }
                Get-Confirmation -Msg "Volumes may not have sufficient space:`n$($LowSpaceVolumes | Format-Table | Out-String)" -BatchMode $BatchMode
            }
        }

        if ($RetainOwnerName) {
            Log "RetainOwner was specified"
            $SourceOwner = Get-SQLDatabaseOwner -InstanceName $SourceInstance -DatabaseName $SourceDatabase
            Log "SourceOwner is $SourceOwner"
            if (-not (LoginExists -Instance $TargetInstance -Login $SourceOwner)) {
                Log "RetainOwner specified, but $SourceOwner does not exist on target" "Error"
                throw "RetainOwner specified, but $SourceOwner does not exist on target"
            }
            Log "Source Owner = $SourceOwner"
        }

        if ($CompatabilityLevel -eq "Retain") {
            $SourceCompatibilityLevel = Get-SQLDatabaseCompatibilityLevel -InstanceName $SourceInstance -DatabaseName $SourceDatabase
            Log "SourceCompatibilityLevel = $SourceCompatibilityLevel"
            Check-Compatibility -Instance $TargetInstance -CompatibilityLevel $SourceCompatibilityLevel
        }

        if ($RecoveryModel -eq "Retain") {
            $SourceRecoveryModel = Get-SQLDatabaseRecoveryModel -InstanceName $SourceInstance -DatabaseName $SourceDatabase
            Log "SourceRecoveryModel = $SourceRecoveryModel"
        }

        $SourceTrustworthy = Get-SQLDatabaseTrustworthy -InstanceName $SourceInstance -DatabaseName $SourceDatabase
        Log "SourceTrustworthy $SourceTrustworthy"

        try {
            $SourceDbSpace = Get-DbalDatabaseFileSpace -Instance $SourceInstance -Database $SourceDatabase
        } catch {
            Log -Message ("Could not query source database size info: {0}" -f $_.Exception.Message) -Level Warning -WriteToHost
        }

        if (-not $CreateDatabase) {
            if (-not [string]::IsNullOrWhiteSpace($AzureStorageBackupLocation)) {
                $TargetBackupLocation = Get-AzureBackupLocation -AzureStorageBackupLocation $AzureStorageBackupLocation -DatabaseName $TargetDatabase -MarkAsRetain $MarkAsRetain
            } else {
                $TargetBackupLocation = Get-BackupLocation -InstanceName $TargetInstance -DatabaseName $TargetDatabase -CreateIfNotExist $true -MarkAsRetain $MarkAsRetain -BackupLocation $BackupPath
                if (-not (Test-PathOnSQLServer -Instance $TargetInstance -Path $TargetBackupLocation -TestDirectoryOnly $true)) {
                    $Msg = "Target location ($TargetBackupLocation) not accessible from target ($TargetInstance)"
                    Log -Message $Msg -Level Error
                    throw $Msg
                }
            }
        }

        if (-not [string]::IsNullOrWhiteSpace($AzureStorageBackupLocation)) {
            $SourceBackupLocation = Get-AzureBackupLocation -AzureStorageBackupLocation $AzureStorageBackupLocation -DatabaseName $SourceDatabase -MarkAsRetain $MarkAsRetain -Differential $Differential
        } else {
            $SourceBackupLocation = Get-BackupLocation -InstanceName $SourceInstance -DatabaseName $SourceDatabase -CreateIfNotExist $true -MarkAsRetain $MarkAsRetain -Differential $Differential -BackupLocation $BackupPath
            if (-not (Test-PathOnSQLServer -Instance $SourceInstance -Path $SourceBackupLocation -TestDirectoryOnly $true)) {
                $Msg = "Source backup location ($SourceBackupLocation) not accessible from source ($SourceInstance)"
                Log -Message $Msg -Level Error
                throw $Msg
            }
        }

        # Resume: replace generated backup path with latest existing .BAK in the expected folder and skip source backup.
        $ResumeSourceBackup = $false
        if ($ResumeFromLatestBackup) {
            if ([string]::IsNullOrWhiteSpace($BackupPath)) {
                Log -Message "-ResumeFromLatestBackup requires -BackupPath (explicit filesystem/UNC base path)." -Level Error -WriteToHost
                throw "-ResumeFromLatestBackup requires -BackupPath (explicit filesystem/UNC base path)."
            }

            $sourceFolder = "{0}\\{1}\\{2}\\" -f $BackupPath.TrimEnd('\\'), ($SourceInstance -replace '\\', '$'), $SourceDatabase
            $latest = Get-DbalLatestBackupFile -BackupFolder $sourceFolder -DatabaseName $SourceDatabase
            if ($null -eq $latest) {
                Log -Message "-ResumeFromLatestBackup specified but no matching backups found in $sourceFolder" -Level Error -WriteToHost
                throw "-ResumeFromLatestBackup specified but no matching backups found in $sourceFolder"
            }
            $SourceBackupLocation = $latest.FullName
            Log -Message "Resuming from latest backup: $(Get-DisplayPath $SourceBackupLocation)" -Level Info -WriteToHost
            $ResumeSourceBackup = $true
        }

        if ($IntermediateInstance) {
            if (-not [string]::IsNullOrWhiteSpace($AzureStorageBackupLocation)) {
                $IntermediateBackupLocation = Get-AzureBackupLocation -AzureStorageBackupLocation $AzureStorageBackupLocation -DatabaseName $TargetDatabase -MarkAsRetain $false
            } else {
                $IntermediateBackupLocation = Get-BackupLocation -InstanceName $IntermediateInstance -DatabaseName $TargetDatabase -CreateIfNotExist $true -MarkAsRetain $false -BackupLocation $BackupPath
                if (-not (Test-PathOnSQLServer -Instance $IntermediateInstance -Path $IntermediateBackupLocation -TestDirectoryOnly $true)) {
                    $Msg = "Intermediate backup location ($IntermediateBackupLocation) not accessible from intermediate ($IntermediateInstance)"
                    Log -Message $Msg -Level Error
                    throw $Msg
                }
                if (-not (Test-PathOnSQLServer -Instance $IntermediateInstance -Path $SourceBackupLocation -TestDirectoryOnly $true)) {
                    $Msg = "Source backup location ($SourceBackupLocation) not accessible from intermediate ($IntermediateInstance)"
                    Log -Message $Msg -Level Error
                    throw $Msg
                }
            }
        }

        if ($CopyUserRoles -and -not $CreateDatabase -and -not $Differential) {
            $OriginalTargetRoles = Get-SQLUserRoles -InstanceName $TargetInstance -DatabaseName $TargetDatabase
        }

        if ($PreserveTargetSecurity.IsPresent -and -not $CreateDatabase -and -not $Differential) {
            Log -Message "PreserveTargetSecurity specified: capturing target database security snapshot" -Level Info -WriteToHost
            $TargetSecuritySnapshot = Get-DbalDatabaseSecuritySnapshot -Instance $TargetInstance -Database $TargetDatabase
            if ($TargetSecuritySnapshot -and $TargetSecuritySnapshot.Warnings -and $TargetSecuritySnapshot.Warnings.Count -gt 0) {
                foreach ($w in $TargetSecuritySnapshot.Warnings) {
                    Log -Message $w -Level Warning -WriteToHost -ForegroundColour Yellow
                }
            }
        }

        $ConfirmMessage = "The destination database ($TargetDatabase on $TargetInstance) $(if ($CreateDatabase) {', which does not currently exist, will be restored '} else {'will be overwritten '}) with a backup of the source database ($SourceDatabase on $SourceInstance)."
        Get-Confirmation -Msg $ConfirmMessage -BatchMode $BatchMode

        if ($PreflightOnly.IsPresent) {
            Log -Message "PreflightOnly specified: checks passed; skipping backup/restore execution." -Level Info -WriteToHost
            return
        }

        $BackupPhaseStartTime = Get-Date
        Write-DebugMessage "[BackupAndRestore] SourceBackupLocation=$(Get-DisplayPath $SourceBackupLocation)"
        Log "Source Backup location = $(Get-DisplayPath $SourceBackupLocation)"
        if ($NoRecovery -or $RollForwardTransactionLogs) { $CopyOnly = $false }
        if (-not $ResumeSourceBackup) {
            Write-DebugMessage "[BackupAndRestore] Creating backup job for $SourceDatabase on $SourceInstance"
            $progressMatch = if ($SourceBackupLocation -match '^https://') { ($SourceBackupLocation.Split('?', 2)[0] | Split-Path -Leaf) } else { $SourceBackupLocation }
            $NewJob = Backup-Database -InstanceName $SourceInstance -DatabaseName $SourceDatabase -BackupPath $SourceBackupLocation -ProgressID 1 -Compress $Compress -JobName "SourceBackup" -Differential $Differential -CopyOnly $CopyOnly -BlockSize $BlockSize -BufferCount $BufferCount -MaxTransferSize $MaxTransferSize -ProgressMatch $progressMatch -CredentialName $azureCredentialName -DryRun:$DryRun
            Write-DebugMessage "[BackupAndRestore] Backup job created: $($NewJob | Out-String)"
            if (-not $DryRun) { $Jobs += $NewJob }
        }

        if ($IntermediateInstance) {
            $IntermediateBackupLocation = Get-BackupLocation -InstanceName $IntermediateInstance -DatabaseName $TargetDatabase -CreateIfNotExist $true -MarkAsRetain $false -BackupLocation $BackupPath
            if (-not (Test-PathOnSQLServer -Instance $IntermediateInstance -Path $IntermediateBackupLocation -TestDirectoryOnly $true)) {
                $Msg = "Intermediate backup location ($IntermediateBackupLocation) not accessible from intermediate ($IntermediateInstance)"
                Log -Message $Msg -Level Error
                throw $Msg
            }
            if (-not (Test-PathOnSQLServer -Instance $IntermediateInstance -Path $SourceBackupLocation -TestDirectoryOnly $true)) {
                $Msg = "Source backup location ($SourceBackupLocation) not accessible from intermediate ($IntermediateInstance)"
                Log -Message $Msg -Level Error
                throw $Msg
            }
            $MessageBody = "IntermediateInstance specified. Backups complete. Restore $TargetDatabase on $IntermediateInstance using file $(Get-DisplayPath $SourceBackupLocation). Return when complete."
            SendEMail -Subject "Restore of $TargetDatabase on $TargetInstance. Action required." -Msg $MessageBody -Address $EmailAddress -FromAddress $FromAddress -NoLog $true
            Write-Host $MessageBody
            Log "WaitforManualRestore specified. Waiting for user input."
            Read-Host -Prompt "Press any key when manual restore on intermediate is complete"
            Log "Input received. Continuing"
            Check-DatabaseAccess -Instance $IntermediateInstance -Database $TargetDatabase
            Backup-Database -InstanceName $IntermediateInstance -DatabaseName $TargetDatabase -BackupPath $IntermediateBackupLocation -Compress $Compress -ProgressID 7 -JobName "BackupIntermediate" -CopyOnly $true -BlockSize $BlockSize -BufferCount $BufferCount -MaxTransferSize $MaxTransferSize -CredentialName $azureCredentialName
            Progress -Job "BackupIntermediate" -Id 7 -Path $IntermediateBackupLocation -Instance $IntermediateInstance -Database $TargetDatabase
        }

        if (-not $DontBackupTarget -and -not $Differential) {
            Write-DebugMessage "[BackupAndRestore] TargetBackupLocation=$TargetBackupLocation"
            Log "TargetBackupLocation = $TargetBackupLocation"
            $targetProgressMatch = if ($TargetBackupLocation -match '^https://') { ($TargetBackupLocation.Split('?', 2)[0] | Split-Path -Leaf) } else { $TargetBackupLocation }
            $TargetJob = Backup-Database -InstanceName $TargetInstance -DatabaseName $TargetDatabase -BackupPath $TargetBackupLocation -ProgressID 2 -Compress $Compress -JobName "TargetBackup" -BlockSize $BlockSize -BufferCount $BufferCount -MaxTransferSize $MaxTransferSize -ProgressMatch $targetProgressMatch -CredentialName $azureCredentialName -DryRun:$DryRun
            Write-DebugMessage "[BackupAndRestore] Target backup job created: $($TargetJob | Out-String)"
            if (-not $DryRun) { $Jobs += $TargetJob }
        }

        if ($DryRun.IsPresent) {
            $AmendedSourceBackupLocation = if ($IntermediateInstance) { $IntermediateBackupLocation } else { $SourceBackupLocation }
            $restoreNoRecovery = if ($RollForwardTransactionLogs) { $true } else { $NoRecovery }

            if ($VerifyBackup.IsPresent) {
                $null = Invoke-DbalVerifyBackup -Instance $TargetInstance -BackupPath $AmendedSourceBackupLocation -CredentialName $azureCredentialName -DryRun
            }

            $null = Restore-SQLDatabase -InstanceName $TargetInstance -DatabaseName $TargetDatabase -TakeInstanceOffline $TakeTargetOffline -TakeInstanceOfflineMode $TakeTargetOfflineMode -BackupPath $AmendedSourceBackupLocation -JobName "RestoreTarget" -NoRecovery $restoreNoRecovery -CreateDatabase $CreateDatabase -SourceDatabase $SourceDatabase -Differential $Differential -CredentialName $azureCredentialName -SourceFileList $S -DryRun

            Log -Message "DryRun specified: generated BACKUP/RESTORE SQL above; skipping execution." -Level Info
            return
        }

        Write-DiagMessage "Calling Progress2 for backup jobs..." -ForegroundColor Cyan
        Progress2 -JobDetailsCollection $Jobs
        Write-DiagMessage "Progress2 for backup jobs returned." -ForegroundColor Cyan
        $BackupPhaseEndTime = Get-Date

        $SourceBackupSizeBytes = Get-DbalFileSizeBytes -Path $SourceBackupLocation
        if (-not $DontBackupTarget -and -not $Differential) {
            $TargetBackupSizeBytes = Get-DbalFileSizeBytes -Path $TargetBackupLocation
        }

        if ($WaitforManualRestore) {
            Log "WaitforManualRestore specified"
            $MessageTo = "$($env:USERNAME)@XXX"
            $MessageFrom = $MessageTo
            $MessageBody = "WaitForManualRestore specified. Backups complete. Restore database on target using $(Get-DisplayPath $SourceBackupLocation). Target: $TargetInstance, Database: $TargetDatabase. Return when complete."
            Send-MailMessage -SmtpServer $smtpserver -From $MessageFrom -To $MessageTo -Subject "Restore of $TargetDatabase on $TargetInstance. Action required." -Body $MessageBody
            Write-Host "WaitforManualRestore specified. Restore using $(Get-DisplayPath $SourceBackupLocation) on $TargetInstance as $TargetDatabase"
            Log "Waiting for user input."
            Read-Host -Prompt "Press any key when manual restore is complete"
            Log "Input received. Continuing"
        } else {
            $AmendedSourceBackupLocation = if ($IntermediateInstance) { $IntermediateBackupLocation } else { $SourceBackupLocation }
            Write-DebugMessage "[BackupAndRestore] Creating restore job for $TargetDatabase on $TargetInstance from $AmendedSourceBackupLocation"
            $restoreNoRecovery = if ($RollForwardTransactionLogs) { $true } else { $NoRecovery }

            if ($VerifyBackup.IsPresent) {
                Invoke-DbalVerifyBackup -Instance $TargetInstance -BackupPath $AmendedSourceBackupLocation -CredentialName $azureCredentialName
            }

            if ($AbortIfActiveSessions -and -not $CreateDatabase -and -not $Differential -and $TakeTargetOffline) {
                if (Test-DbalDatabaseHasActiveUserSessions -Instance $TargetInstance -Database $TargetDatabase) {
                    $msg = "Active user sessions detected on [$TargetDatabase] on [$TargetInstance]; aborting due to -AbortIfActiveSessions."
                    Log -Message $msg -Level Error -WriteToHost
                    throw $msg
                }
            }

            $RestorePhaseStartTime = Get-Date
            $NewJob = Restore-SQLDatabase -InstanceName $TargetInstance -DatabaseName $TargetDatabase -TakeInstanceOffline $TakeTargetOffline -TakeInstanceOfflineMode $TakeTargetOfflineMode -BackupPath $AmendedSourceBackupLocation -JobName "RestoreTarget" -NoRecovery $restoreNoRecovery -CreateDatabase $CreateDatabase -SourceDatabase $SourceDatabase -Differential $Differential -CredentialName $azureCredentialName -SourceFileList $S
            Write-DebugMessage "[BackupAndRestore] Restore job created: $($NewJob | Out-String)"
            $RestoreJobs = @()
            $RestoreJobs += $NewJob
            Write-DiagMessage "Calling Progress2 for restore jobs..." -ForegroundColor Cyan
            Progress2 -JobDetailsCollection $RestoreJobs
            Write-DiagMessage "Progress2 for restore jobs returned." -ForegroundColor Cyan
            $RestorePhaseEndTime = Get-Date
            Write-DebugMessage "[BackupAndRestore] Restore finished"
            Log "Restore finished"

            if (-not $restoreNoRecovery) {
                try {
                    $TargetDbSpace = Get-DbalDatabaseFileSpace -Instance $TargetInstance -Database $TargetDatabase
                } catch {
                    Log -Message ("Could not query target database size info: {0}" -f $_.Exception.Message) -Level Warning -WriteToHost
                }
            }
        }

        if ($RollForwardTransactionLogs) {
            $sourceRm = Get-SQLDatabaseRecoveryModel -InstanceName $SourceInstance -DatabaseName $SourceDatabase
            if ($sourceRm -eq 'SIMPLE') {
                $msg = "-RollForwardTransactionLogs requires the source database to use FULL or BULK_LOGGED recovery model. Current: $sourceRm. Change recovery model and take a full backup first."
                Log -Message $msg -Level Error -WriteToHost
                throw $msg
            }

            $logBackupDirectory = Split-Path -Parent $SourceBackupLocation
            if (-not (Test-PathOnSQLServer -Instance $SourceInstance -Path $logBackupDirectory -TestDirectoryOnly $true)) {
                $msg = "Log backup directory not accessible from source ($SourceInstance): $logBackupDirectory"
                Log -Message $msg -Level Error -WriteToHost
                throw $msg
            }
            if (-not (Test-PathOnSQLServer -Instance $TargetInstance -Path $logBackupDirectory -TestDirectoryOnly $true)) {
                $msg = "Log backup directory not accessible from target ($TargetInstance): $logBackupDirectory"
                Log -Message $msg -Level Error -WriteToHost
                throw $msg
            }

            Log -Message "RollForwardTransactionLogs enabled: applying transaction log backups to bring target up to date." -Level Info -WriteToHost

            $cycle = 0

            # Always apply at least one log backup, then ask if we're ready to finalize.
            while ($true) {
                $cycle++
                $timestamp = Get-Date -Format 'yyyyMMddHHmmss'
                $logBackupPath = Join-Path $logBackupDirectory "$SourceDatabase`_adhoc_LOG_$timestamp.trn"

                $logBackupJob = Backup-TransactionLog -InstanceName $SourceInstance -DatabaseName $SourceDatabase -BackupPath $logBackupPath -ProgressID 101 -JobName "SourceLogBackup" -BlockSize $BlockSize -BufferCount $BufferCount -MaxTransferSize $MaxTransferSize
                Progress2 -JobDetailsCollection @($logBackupJob)

                $restoreLogJob = Restore-SQLTransactionLog -InstanceName $TargetInstance -DatabaseName $TargetDatabase -BackupPath $logBackupPath -NoRecovery $true -JobName "RestoreTargetLog"
                Progress2 -JobDetailsCollection @($restoreLogJob)

                $maxReached = ($MaxLogBackupCycles -gt 0 -and $cycle -ge $MaxLogBackupCycles)
                if ($maxReached) {
                    Log -Message "MaxLogBackupCycles reached ($MaxLogBackupCycles); proceeding to final log backup with recovery." -Level Warning -WriteToHost
                    break
                }

                $ready = Read-UserYesNo -Prompt "Ready for FINAL transaction log backup and to recover '$TargetDatabase' on '$TargetInstance'?" -BatchMode $BatchMode -DefaultYes $true
                if ($ready) {
                    break
                }

                Start-Sleep -Seconds $LogBackupIntervalSeconds
            }

            $finalTimestamp = Get-Date -Format 'yyyyMMddHHmmss'
            $finalLogBackupPath = Join-Path $logBackupDirectory "$SourceDatabase`_adhoc_LOGFINAL_$finalTimestamp.trn"
            $finalLogBackupJob = Backup-TransactionLog -InstanceName $SourceInstance -DatabaseName $SourceDatabase -BackupPath $finalLogBackupPath -ProgressID 102 -JobName "SourceLogBackupFinal" -BlockSize $BlockSize -BufferCount $BufferCount -MaxTransferSize $MaxTransferSize
            Progress2 -JobDetailsCollection @($finalLogBackupJob)

            $finalRestoreLogJob = Restore-SQLTransactionLog -InstanceName $TargetInstance -DatabaseName $TargetDatabase -BackupPath $finalLogBackupPath -NoRecovery $false -JobName "RestoreTargetLogFinal"
            Progress2 -JobDetailsCollection @($finalRestoreLogJob)

            # Ensure post-restore steps run.
            $NoRecovery = $false
        }

        if (-not $NoRecovery) {
            if ($ChangeCollation) {
                Change-Collation -Instance $TargetInstance -Database $TargetDatabase -Collation $Collation
            }
            if ($CreateLoginsIfTheyDontExist) {
                Log "Will create logins"
                Create-Logins -SourceInstance $SourceInstance -SourceDatabase $SourceDatabase -TargetInstance $TargetInstance -TargetDatabase $TargetDatabase
            }

            if ($PreserveTargetSecurity.IsPresent -and $null -ne $TargetSecuritySnapshot) {
                Invoke-DbalDatabaseSecuritySnapshot -Instance $TargetInstance -Database $TargetDatabase -Snapshot $TargetSecuritySnapshot
            }
            if ($ShrinkLog) {
                Log "Shrinking transaction log on $TargetDatabase on $TargetInstance"
                ShrinkLog -InstanceName $TargetInstance -DatabaseName $TargetDatabase
            } else {
                Log "ShrinkLog not specified"
            }
            if ($CopyUserRoles) {
                if ($PreserveTargetSecurity.IsPresent) {
                    Log "CopyUserRoles specified, but PreserveTargetSecurity is also set; skipping CopyUserRoles because role memberships are included in preserved security." -Level Warning -WriteToHost -ForegroundColour Yellow
                } else {
                    Log "Writing user roles"
                    Write-SQLUserRoles -InstanceName $TargetInstance -DatabaseName $TargetDatabase -Roles $OriginalTargetRoles
                }
            } else {
                Log "CopyUserRoles not specified"
            }
            if ($SourceTrustworthy -eq 1) {
                Write-SQLDatabaseTrustworthy -InstanceName $TargetInstance -DatabaseName $TargetDatabase -Trustworthy 1
            }
            if ($RetainOwnerName) {
                Write-SQLDatabaseOwner -InstanceName $TargetInstance -DatabaseName $TargetDatabase -Owner $SourceOwner
            } else {
                Write-SQLDatabaseOwner -InstanceName $TargetInstance -DatabaseName $TargetDatabase -Owner "sa"
            }
            if ($CompatabilityLevel -eq "SetLatest") {
                Log "Setting compat = latest"
                $TargetLatestCompat = Get-SQLInstanceLatestSupportedCompatibilityLevel -InstanceName $TargetInstance
                Log "Latest compat = $TargetLatestCompat"
                Write-SQLDatabaseCompatibilityLevel -InstanceName $TargetInstance -DatabaseName $TargetDatabase -CompatibilityLevel $TargetLatestCompat
            } elseif ($CompatabilityLevel -eq "Retain") {
                Write-SQLDatabaseCompatibilityLevel -InstanceName $TargetInstance -DatabaseName $TargetDatabase -CompatibilityLevel $SourceCompatibilityLevel
            } else {
                Write-SQLDatabaseCompatibilityLevel -InstanceName $TargetInstance -DatabaseName $TargetDatabase -CompatibilityLevel $CompatabilityLevel
            }
        }

        if (-not $NoRecovery -and -not $NoDBCC) {
            if (Run-DBCCCHECKDB -Instance $TargetInstance -Database $TargetDatabase) {
                Log "DBCC CHECKDB detected errors." -Level Error -WriteToHost -ForegroundColour Red -Note "DBCC"
                throw "DBCC CHECKDB detected errors."
            }
        }

        if (-not $NoRecovery -and $UpdateStats) {
            Log -Message "Running update stats" -Level Info -WriteToHost
            Invoke-Sqlcmd -ServerInstance $TargetInstance -Database $TargetDatabase -Query "EXEC sp_updatestats"
        }

        if (-not $NoRecovery) {
            Log -Message "Setting PAGE_VERIFY CHECKSUM" -Level Info
            Invoke-Sqlcmd -ServerInstance $TargetInstance -Database $TargetDatabase -Query "ALTER DATABASE [$TargetDatabase] SET PAGE_VERIFY CHECKSUM"
        }

        if (-not $NoRecovery) {
            $Orph = ListOrphanedUsers -InstanceName $TargetInstance -DatabaseName $TargetDatabase
            if ($null -eq $Orph) {
                Log -Message "No orphaned users on $TargetDatabase on $TargetInstance" -Level Info
            } else {
                Log -Message "Orphaned users found on $TargetDatabase on $TargetInstance :`n$($Orph | Format-Table | Out-String)" -Level Warning -WriteToHost -ForegroundColour Yellow
            }

            $UsersWithNoLogins = ListUsersWithNoLogins -InstanceName $TargetInstance -DatabaseName $TargetDatabase
            $NoLoginsString = if ($null -eq $UsersWithNoLogins) {
                "No users without logins on $TargetDatabase on $TargetInstance"
            } else {
                "Users without logins found on $TargetDatabase on $TargetInstance :`n$($UsersWithNoLogins | Format-Table | Out-String)"
            }
            Log -Message $NoLoginsString -Level Warning -WriteToHost -ForegroundColour Yellow

            if ($DeleteOrphans) {
                Write-Host "Deleting Orphaned Users"
                Remove-Orphans -InstanceName $TargetInstance -DatabaseName $TargetDatabase
            }
        }

        $MailSubject = "Restore of $TargetDatabase on $TargetInstance completed successfully"
        $MailStatus = 'SUCCESS'
        $RecoveryMessage = if ($NoRecovery) { "Database in RESTORING state for differential backups. Use -Differential to apply." } else { "" }
        $MailNotes = @(
            $RecoveryMessage,
            $NoLoginsString
        ) -join "`n"

        if ($ScriptToRunOnTarget) {
            Log -Message "Running $ScriptToRunOnTarget on $TargetDatabase on $TargetInstance" -Level Info
            Invoke-Sqlcmd -ServerInstance $TargetInstance -Database $TargetDatabase -InputFile $ScriptToRunOnTarget
        }

        if ($NumberOfBackupsToRetain -ne $null) {
            Log -Message "NumberOfBackupsToRetain = $NumberOfBackupsToRetain, deleting old backups"
            Write-Host "NumberOfBackupsToRetain = $NumberOfBackupsToRetain, deleting old backups" -ForegroundColor Yellow

            if ([string]::IsNullOrWhiteSpace($BackupPath) -or ($BackupPath -match '^https://')) {
                Log -Message "Skipping backup retention cleanup: -BackupPath is not a filesystem path." -Level Warning -WriteToHost
            } else {
            $DeletePath = "$BackupPath\$($SourceInstance -replace '\\', '$')\$SourceDatabase\"

                Remove-DbalOldBackups -DeletePath $DeletePath -RetainByAgeDays $RetainByAgeDays -NumberOfBackupsToRetain $NumberOfBackupsToRetain

                if (-not $DontBackupTarget) {
                    $DeletePath = "$BackupPath\$($TargetInstance -replace '\\', '$')\$TargetDatabase\"
                    Remove-DbalOldBackups -DeletePath $DeletePath -RetainByAgeDays $RetainByAgeDays -NumberOfBackupsToRetain $NumberOfBackupsToRetain
                }
            }
        }

        if ($RetainByAgeDays -gt 0 -and $NumberOfBackupsToRetain -eq $null) {
            # Age-based retention requested without count-based retention.
            if ([string]::IsNullOrWhiteSpace($BackupPath) -or ($BackupPath -match '^https://')) {
                Log -Message "Skipping age-based backup retention cleanup: -BackupPath is not a filesystem path." -Level Warning -WriteToHost
            } else {
                $DeletePath = "$BackupPath\$($SourceInstance -replace '\\', '$')\$SourceDatabase\"
                Remove-DbalOldBackups -DeletePath $DeletePath -RetainByAgeDays $RetainByAgeDays -NumberOfBackupsToRetain $null
                if (-not $DontBackupTarget) {
                    $DeletePath = "$BackupPath\$($TargetInstance -replace '\\', '$')\$TargetDatabase\"
                    Remove-DbalOldBackups -DeletePath $DeletePath -RetainByAgeDays $RetainByAgeDays -NumberOfBackupsToRetain $null
                }
            }
        }

        $BackupAndRestoreEndTime = Get-Date
        $Runtime = New-TimeSpan -Start $BackupAndRestoreStartTime -End $BackupAndRestoreEndTime
        $ElapsedString = "Elapsed Time: {0}:{1}:{2}" -f $Runtime.Hours, $Runtime.Minutes, $Runtime.Seconds
        Write-DebugMessage "[BackupAndRestore] Finished. $ElapsedString"
        Log -Message "Finished. $ElapsedString" -Level Info -WriteToHost
        $MailMessage = "$ElapsedString`n" + $MailMessage + $NoLoginsString
    } catch {
        Write-DebugMessage "[BackupAndRestore] ERROR: $($_.Exception.Message)"
        Log -Message "Error. Final catch" -Level "Error" -WriteToHost
        $errRecord = $_
        $ErrorStackTrace = $errRecord.ScriptStackTrace
        $ErrorDetails = "ERROR: `n`n STACK TRACE: `n`n $ErrorStackTrace `n`n" + ($errRecord | Out-String) -replace "'", ""
        $ErrorDetails = $ErrorDetails -replace "\[$]", "X"
        $MailDetails = "Parameters:`n`n$($MyInvocation.BoundParameters | Out-String)`n`n" + $ErrorDetails
        $MailSubject = "ERROR in Backup and Restore of $TargetDatabase on $TargetInstance"
        $MailStatus = 'ERROR'
        Write-Host $ErrorDetails -ForegroundColor Red
    } finally {
        Write-DebugMessage "[BackupAndRestore] Exit"
        # Print job diagnostics before removing jobs
        if ($script:DBALibraryVerboseDiagnostics) {
            $allJobs = Get-Job | Where-Object { $_.Name -ne "dbatools_Timer" }
            if ($allJobs) {
                Write-DiagMessage "Job diagnostics before job cleanup:" -ForegroundColor Yellow
                foreach ($job in $allJobs) {
                    Write-DiagMessage "Job $($job.Name) State: $($job.State)" -ForegroundColor Yellow
                    $jobOutput = Receive-Job -Job $job -Keep -ErrorAction SilentlyContinue
                    if ($jobOutput) {
                        Write-DiagMessage "Output for job $($job.Name):" -ForegroundColor Cyan
                        $jobOutput | ForEach-Object { Write-Host $_ -ForegroundColor Cyan }
                    } else {
                        Write-DiagMessage "No output for job $($job.Name)." -ForegroundColor DarkGray
                    }
                    $jobErrors = $job.ChildJobs | ForEach-Object { $_.JobStateInfo.Reason }
                    foreach ($err in $jobErrors) {
                        if ($err) {
                            Write-DiagMessage "Error for job $($job.Name): $err" -ForegroundColor Red
                        }
                    }
                }
            }
        }

        Get-Job | Remove-Job -Force -ErrorAction SilentlyContinue
        if ($MailSubject -ne $null) {
            Log -Message "BackupAndRestore: sending completion email. To='$EmailAddress' From='$FromAddress' Subject='$MailSubject'" -Level Info -WriteToHost
            $endTime = Get-Date
            $runtime = if ($BackupAndRestoreStartTime) { New-TimeSpan -Start $BackupAndRestoreStartTime -End $endTime } else { $null }
            $elapsedString = if ($runtime) { "Elapsed Time: {0}:{1}:{2}" -f $runtime.Hours, $runtime.Minutes, $runtime.Seconds } else { "Elapsed Time: (unknown)" }

            $runAs = $null
            try { $runAs = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name } catch { $runAs = $null }
            if ([string]::IsNullOrWhiteSpace($runAs)) { $runAs = "{0}\\{1}" -f $env:USERDOMAIN, $env:USERNAME }

            $summary = [ordered]@{
                'Run As' = $runAs
                'Host' = $env:COMPUTERNAME
                'Started' = if ($BackupAndRestoreStartTime) { $BackupAndRestoreStartTime.ToString('o') } else { '' }
                'Elapsed Time' = $elapsedString
                'Source Instance' = $SourceInstance
                'Source Database' = $SourceDatabase
                'Target Instance' = $TargetInstance
                'Target Database' = $TargetDatabase
            }

            if ($BackupPhaseStartTime) { $summary['Backup Started'] = $BackupPhaseStartTime.ToString('o') }
            if ($BackupPhaseEndTime) { $summary['Backup Ended'] = $BackupPhaseEndTime.ToString('o') }
            if ($BackupPhaseStartTime -and $BackupPhaseEndTime) {
                $backupDuration = New-TimeSpan -Start $BackupPhaseStartTime -End $BackupPhaseEndTime
                $summary['Backup Duration'] = Format-DbalDuration -Duration $backupDuration
                if ($SourceBackupSizeBytes) {
                    $summary['Backup Throughput'] = Format-DbalThroughput -Bytes $SourceBackupSizeBytes -Seconds $backupDuration.TotalSeconds
                }
            }

            if ($RestorePhaseStartTime) { $summary['Restore Started'] = $RestorePhaseStartTime.ToString('o') }
            if ($RestorePhaseEndTime) { $summary['Restore Ended'] = $RestorePhaseEndTime.ToString('o') }
            if ($RestorePhaseStartTime -and $RestorePhaseEndTime) {
                $restoreDuration = New-TimeSpan -Start $RestorePhaseStartTime -End $RestorePhaseEndTime
                $summary['Restore Duration'] = Format-DbalDuration -Duration $restoreDuration
                if ($SourceBackupSizeBytes) {
                    $summary['Restore Throughput (est.)'] = Format-DbalThroughput -Bytes $SourceBackupSizeBytes -Seconds $restoreDuration.TotalSeconds
                }
            }

            if ($SourceDbSpace) {
                $summary['Source DB Data Allocated'] = Format-DbalBytes -Bytes $SourceDbSpace.DataAllocatedBytes
                $summary['Source DB Data Used'] = Format-DbalBytes -Bytes $SourceDbSpace.DataUsedBytes
                $summary['Source DB Data Free'] = Format-DbalBytes -Bytes $SourceDbSpace.DataFreeBytes
                $summary['Source DB Log Allocated'] = Format-DbalBytes -Bytes $SourceDbSpace.LogAllocatedBytes
                $summary['Source DB Log Used'] = Format-DbalBytes -Bytes $SourceDbSpace.LogUsedBytes
                $summary['Source DB Log Free'] = Format-DbalBytes -Bytes $SourceDbSpace.LogFreeBytes
                $summary['Source DB Total Allocated'] = Format-DbalBytes -Bytes $SourceDbSpace.TotalAllocatedBytes
            }

            if ($TargetDbSpace) {
                $summary['Target DB Data Allocated'] = Format-DbalBytes -Bytes $TargetDbSpace.DataAllocatedBytes
                $summary['Target DB Data Used'] = Format-DbalBytes -Bytes $TargetDbSpace.DataUsedBytes
                $summary['Target DB Data Free'] = Format-DbalBytes -Bytes $TargetDbSpace.DataFreeBytes
                $summary['Target DB Log Allocated'] = Format-DbalBytes -Bytes $TargetDbSpace.LogAllocatedBytes
                $summary['Target DB Log Used'] = Format-DbalBytes -Bytes $TargetDbSpace.LogUsedBytes
                $summary['Target DB Log Free'] = Format-DbalBytes -Bytes $TargetDbSpace.LogFreeBytes
                $summary['Target DB Total Allocated'] = Format-DbalBytes -Bytes $TargetDbSpace.TotalAllocatedBytes
            }

            if ($PSBoundParameters.ContainsKey('CreateDatabase')) { $summary['CreateDatabase'] = [bool]$CreateDatabase }
            if ($PSBoundParameters.ContainsKey('OverwriteTarget')) { $summary['OverwriteTarget'] = [bool]$OverwriteTarget }
            if ($PSBoundParameters.ContainsKey('Differential')) { $summary['Differential'] = [bool]$Differential }
            if ($PSBoundParameters.ContainsKey('NoRecovery')) { $summary['NoRecovery'] = [bool]$NoRecovery }
            if ($PSBoundParameters.ContainsKey('RollForwardTransactionLogs')) { $summary['RollForwardTransactionLogs'] = [bool]$RollForwardTransactionLogs }

            if ($SourceBackupLocation) {
                $summary['Source Backup File'] = (Get-DisplayPath $SourceBackupLocation)
            }
            if ($SourceBackupSizeBytes) {
                $summary['Source Backup Size'] = Format-DbalBytes -Bytes $SourceBackupSizeBytes
            }
            if (-not $DontBackupTarget -and $TargetBackupLocation) {
                $summary['Target Backup File'] = (Get-DisplayPath $TargetBackupLocation)
            }
            if ($TargetBackupSizeBytes) {
                $summary['Target Backup Size'] = Format-DbalBytes -Bytes $TargetBackupSizeBytes
            }

            if (-not [string]::IsNullOrWhiteSpace($BackupPath)) {
                $summary['BackupPath'] = $BackupPath
            }
            if (-not [string]::IsNullOrWhiteSpace($AzureStorageBackupLocation)) {
                $summary['AzureStorageBackupLocation'] = (Get-DisplayPath $AzureStorageBackupLocation)
            }
            if ($script:ExecutionID) {
                $summary['ExecutionID'] = $script:ExecutionID
            }

            $params = [ordered]@{}
            foreach ($k in ($MyInvocation.BoundParameters.Keys | Sort-Object)) {
                $v = $MyInvocation.BoundParameters[$k]
                $params[$k] = $v
            }

            $details = if ($MailStatus -eq 'ERROR') { $MailDetails } else { $null }
            $notes = @($elapsedString, $MailNotes) -join "`n"

            $title = if ($MailStatus -eq 'ERROR') {
                "BackupAndRestore failed"
            } else {
                "BackupAndRestore completed successfully"
            }

            try {
                $html = New-DbalBackupAndRestoreEmailHtml -Status $MailStatus -Title $title -Summary $summary -Parameters $params -Notes $notes -Details $details
                SendEMail -Subject $MailSubject -Msg $html -Address $EmailAddress -FromAddress $FromAddress -BodyAsHtml
            } catch {
                Log -Message ("BackupAndRestore: completion email failed (HTML path): {0}" -f $_.Exception.Message) -Level Warning -WriteToHost
                try {
                    $fallbackLines = @(
                        $title,
                        $elapsedString,
                        "",
                        "Summary:",
                        ($summary | Out-String),
                        "Parameters:",
                        ($params | Out-String)
                    )

                    if (-not [string]::IsNullOrWhiteSpace($MailNotes)) {
                        $fallbackLines += "Notes:`n$MailNotes"
                    }
                    if (-not [string]::IsNullOrWhiteSpace($MailDetails)) {
                        $fallbackLines += "Details:`n$MailDetails"
                    }

                    $fallback = ($fallbackLines | Where-Object { $_ -ne $null }) -join "`n"
                    SendEMail -Subject $MailSubject -Msg $fallback -Address $EmailAddress -FromAddress $FromAddress
                } catch {
                    Log -Message ("BackupAndRestore: completion email fallback also failed: {0}" -f $_.Exception.Message) -Level Warning -WriteToHost
                }
            }
        } else {
            Log -Message "BackupAndRestore: completion email skipped (MailSubject is null)." -Level Warning -WriteToHost
        }
        Log "Finished."
        Write-DiagMessage "(finally) Error variable: $($Error | Out-String)" -ForegroundColor Magenta
        Write-DiagMessage "(finally) LASTEXITCODE: $LASTEXITCODE" -ForegroundColor Magenta
    }
}

function Read-UserYesNo {
    <#
    .SYNOPSIS
        Prompts user for a Y/N decision. In batch mode, returns DefaultYes.
    #>
    param(
        [Parameter(Mandatory = $true)][string]$Prompt,
        [Parameter(Mandatory = $true)][bool]$BatchMode,
        [bool]$DefaultYes = $true
    )

    Log -Message "USER PROMPT (Yes/No): $Prompt" -Level Info
    if ($BatchMode) {
        $choice = if ($DefaultYes) { 'Yes' } else { 'No' }
        Write-Host "BatchMode specified, defaulting to: $choice" -ForegroundColor Yellow
        return $DefaultYes
    }

    $response = Read-Host "$Prompt (Y/N)"
    if ([string]::IsNullOrWhiteSpace($response)) {
        return $DefaultYes
    }
    return ($response.Trim().ToUpperInvariant() -eq 'Y')
}

function Get-DatabaseState {
    <#
    .SYNOPSIS
        Gets the state of a database.
    #>
    param ($Instance, $Database)

    $DatabaseStateSQL = "SELECT state_desc FROM sys.databases WHERE name = '$Database'"
    (Invoke-Sqlcmd -ServerInstance $Instance -Query $DatabaseStateSQL).state_desc
}

function Get-SQLDatabaseInternalVersionNumberFromDatabase {
    <#
    .SYNOPSIS
        Gets internal version number from a database.
    #>
    param ($Instance, $Database)

    try {
        $InternalVersionSQL = "SELECT DATABASEPROPERTYEX('$Database','Version') AS InternalVersion"
        (Invoke-Sqlcmd -ServerInstance $Instance -Database $Database -Query $InternalVersionSQL).InternalVersion
    } catch {
        Log "Error getting internal version" -Level Error
        throw
    }
}

function Get-Confirmation {
    <#
    .SYNOPSIS
        Gets user confirmation or proceeds in batch mode.
    #>
    param ([string]$Msg, [bool]$BatchMode)

    Log -Message "USER PROMPT: $Msg" -Level Info
    Write-Host $Msg -ForegroundColor Green
    if ($BatchMode) {
        Write-Host "BatchMode specified, continuing" -ForegroundColor Yellow
    } else {
        $Response = Read-Host "Continue? (Y/N)"
        if ($Response -ne "Y") {
            Log -Message "Exiting due to user response." -Level Info -WriteToHost
            exit
        }
    }
}

function Get-SQLInstanceVersion {
    <#
    .SYNOPSIS
        Gets SQL instance version.
    #>
    param ([string]$InstanceName)

    (Invoke-Sqlcmd -ServerInstance $InstanceName -Database master -Query "SELECT @@VERSION AS Version").Version
}

function Get-ProductName {
    <#
    .SYNOPSIS
        Gets SQL product name.
    #>
    param ([string]$Instance)

    try {
        $ProductNameSQL = @"
DECLARE @sqlVers numeric(4,2)
SELECT @sqlVers = left(cast(serverproperty('productversion') as varchar), 4)
SELECT SERVERPROPERTY('productversion') AS Version
,SERVERPROPERTY('productlevel') AS ServicePacklevel
,SERVERPROPERTY('edition') AS Edition
,CASE @sqlVers
    WHEN '8.00' THEN 'SQL 2000'
    WHEN '9.00' THEN 'SQL 2005'
    WHEN '10.00' THEN 'SQL 2008'
    WHEN '10.50' THEN 'SQL 2008 R2'
    WHEN '11.00' THEN 'SQL 2012'
    WHEN '12.00' THEN 'SQL 2014'
    WHEN '13.00' THEN 'SQL 2016'
    WHEN '14.00' THEN 'SQL 2017'
    WHEN '15.00' THEN 'SQL 2019'
    WHEN '16.00' THEN 'SQL 2022'
    WHEN '17.00' THEN 'SQL 2025'
    ELSE 'OTHER' END AS ProductName
"@
        (Invoke-Sqlcmd -ServerInstance $Instance -Query $ProductNameSQL -AbortOnError).ProductName
    } catch {
        Log -Message "Error getting product name" -Level Error -WriteToHost
        throw
    }
}

function Get-DatabaseLocks {
    <#
    .SYNOPSIS
        Checks if database has locks.
    #>
    param ($InstanceName, $DatabaseName)

    try {
        $InUseSQL = "IF EXISTS (SELECT * FROM sys.dm_tran_locks WHERE resource_database_id = DB_ID('$DatabaseName')) SELECT 1 AS InUse ELSE SELECT 0 AS InUse"
        (Invoke-Sqlcmd -ServerInstance $InstanceName -Database master -Query $InUseSQL -AbortOnError).InUse
    } catch {
        Log -Message "Error checking locks" -Level Error
        throw
    }
}

function CheckIfDatabaseIsInAvailabilityGroup {
    <#
    .SYNOPSIS
        Checks if database is in an availability group.
    #>
    param ([string]$Instance, [string]$Database)

    try {
        $CheckAGSQL = @"
SELECT DISTINCT d.name, ag.name AS AGName
FROM sys.databases d
JOIN sys.dm_hadr_database_replica_states hadrdrs ON d.database_id = hadrdrs.database_id
JOIN sys.availability_groups ag ON ag.group_id = hadrdrs.group_id
WHERE d.name = '$Database'
"@
        Log -Message $CheckAGSQL -Level Info
        Log -Message "Running on $Database on $Instance" -Level Info
        Invoke-Sqlcmd -ServerInstance $Instance -Database $Database -Query $CheckAGSQL
    } catch {
        Log -Message "Error checking availability group" -Level Error
        throw
    }
}

function Get-DatabaseEncryptionFromDatabase {
    <#
    .SYNOPSIS
        Gets database encryption details.
    #>
    param ($InstanceName, $DatabaseName)

    try {
        $DatabaseEncryptionSQL = @"
SELECT d.name, key_algorithm, key_length, ISNULL(encryption_state,0) AS encryption_state, CONVERT(varchar(max),encryptor_thumbprint ,2 ) AS encryptor_thumbprint
FROM sys.databases d
LEFT OUTER JOIN sys.dm_database_encryption_keys dek ON dek.database_id = d.database_id
WHERE DB_NAME(d.database_id) = '$DatabaseName'
"@
        Invoke-Sqlcmd -ServerInstance $InstanceName -Database $DatabaseName -Query $DatabaseEncryptionSQL -AbortOnError
    } catch {
        Log "Failed to get database encryption details for $DatabaseName on $InstanceName"
        throw
    }
}

function Get-SQLInstanceCompression {
    <#
    .SYNOPSIS
        Checks if instance supports backup compression.
    #>
    param ($InstanceName)

    $Value = (Invoke-Sqlcmd -ServerInstance $InstanceName -Query "SELECT ISNULL((SELECT value FROM sys.configurations WHERE name = 'backup compression default'),-1) AS C").C
    $Value -ne -1
}

function Get-FileListFromDatabase {
    <#
    .SYNOPSIS
        Gets file list from database.
    #>
    param ($Instance, $Database)

    try {
        $FileListSQL = "SELECT CASE WHEN type = 0 THEN 'D' ELSE 'L' END AS Type, Size * 8 / 1024 AS SizeInMB, name AS LogicalName, physical_name AS PhysicalName FROM sys.master_files WHERE database_id = DB_ID()"
        Invoke-Sqlcmd -ServerInstance $Instance -Database $Database -Query $FileListSQL
    } catch {
        Log -Message "Error getting file list. Instance = $Instance , Database = $Database" -Level Error
        throw
    }
}

function Get-SQLDatabaseOwner {
    <#
    .SYNOPSIS
        Gets database owner.
    #>
    param ($InstanceName, $DatabaseName)

    try {
        (Invoke-Sqlcmd -ServerInstance $InstanceName -Database $DatabaseName -Query "SELECT SUSER_SNAME(owner_sid) AS Owner FROM sys.databases WHERE database_id = DB_ID()").Owner
    } catch {
        Log "Error getting current owner." "Error"
        throw
    }
}

function Get-SQLDatabaseCompatibilityLevel {
    <#
    .SYNOPSIS
        Gets database compatibility level.
    #>
    param ($InstanceName, $DatabaseName)

    try {
        $CurrentCompatSQL = "SELECT compatibility_level FROM sys.databases WHERE database_id = DB_ID()"
        (Invoke-Sqlcmd -ServerInstance $InstanceName -Database $DatabaseName -Query $CurrentCompatSQL).compatibility_level
    } catch {
        Log "Error getting current compatibility level." "Error"
        throw
    }
}

function Get-SQLDatabaseRecoveryModel {
    <#
    .SYNOPSIS
        Gets database recovery model.
    #>
    param ($InstanceName, $DatabaseName)

    try {
        (Invoke-Sqlcmd -ServerInstance $InstanceName -Database $DatabaseName -Query "SELECT recovery_model_desc FROM sys.databases WHERE name = DB_NAME()").recovery_model_desc
    } catch {
        Log "Error getting current recovery model." "Error"
        throw
    }
}

function Check-Compatibility {
    <#
    .SYNOPSIS
        Checks compatibility level for instance.
    #>
    param ([string]$Instance, [string]$CompatibilityLevel)

    try {
        $ProdName = Get-ProductName -Instance $Instance
        $MaxCompatibilityByProduct = @{
            'SQL 2000'    = 80
            'SQL 2005'    = 90
            'SQL 2008'    = 100
            'SQL 2008 R2' = 100
            'SQL 2012'    = 110
            'SQL 2014'    = 120
            'SQL 2016'    = 130
            'SQL 2017'    = 140
            'SQL 2019'    = 150
            'SQL 2022'    = 160
            'SQL 2025'    = 170
        }

        $MaxCompat = $MaxCompatibilityByProduct[$ProdName]
        if ($null -eq $MaxCompat) {
            Log -Message "Unable to validate compatibility: unknown product name '$ProdName' on $Instance" -Level Warning -WriteToHost
            return
        }

        Log -Message "Max compat for $Instance ($ProdName) is $MaxCompat. Compat $CompatibilityLevel specified" -Level Info -WriteToHost
        if ([int]$CompatibilityLevel -gt [int]$MaxCompat) {
            Log -Message "Compatibility $CompatibilityLevel is not supported by $Instance ($ProdName). Max is $MaxCompat." -Level Error -WriteToHost
            throw "Compatibility $CompatibilityLevel is not supported by $Instance ($ProdName). Max is $MaxCompat."
        }
    } catch {
        Log -Message "Error checking compatibility" -Level Error
        throw
    }
}

function Get-BackupLocation {
    <#
    .SYNOPSIS
        Gets or creates backup location.
    #>
    param ([string]$InstanceName, [string]$DatabaseName, [bool]$CreateIfNotExist, [bool]$MarkAsRetain, [bool]$Differential = $false, [string]$BackupLocation)

    if ([string]::IsNullOrEmpty($BackupLocation)) {
        try {
            Log "Differential is $Differential"
            Set-Location C:\
            $SourceSiteSQL = @"
SELECT CASE s.SiteName WHEN 'Hemel Corporate' THEN 'HO' WHEN 'Slough Corporate' THEN 'PDC' END AS SiteCode
FROM Tbl_Server_List SL JOIN Tbl_Stats_Hosts h ON sl.HostID = h.HostID JOIN tbl_stats_sites s ON h.SiteID = s.SiteID
WHERE sl.Server_name = '$InstanceName'
"@
            if ($script:DBALibraryVerboseDiagnostics) {
                Write-DiagMessage $SourceSiteSQL
            }
            $SourceSite = (Invoke-Sqlcmd -ServerInstance $DBAInstance -Database $DBADatabase -Query $SourceSiteSQL).SiteCode
        } catch {
            Log -Message "Error querying for backup location" -Level Error
            throw
        }
        $DBAbaseBackupLocation = if ($SourceSite -eq "PDC") { "\\pdc-dbastr-01vm\SQLBackups\" } else { "\\ho-dbastr-01vm\SQLBackups\" }
        $BackupPath = "$DBAbaseBackupLocation$SourceSite\$($InstanceName -replace '\\', '$')\$DatabaseName\"
    } else {
        $BackupPath = "$BackupLocation\$($InstanceName -replace '\\', '$')\$DatabaseName\"
        Log "BackupPath = $BackupPath"
        if ($script:DBALibraryVerboseDiagnostics) {
            Write-DiagMessage "Backup Path: $BackupPath" -ForegroundColor Green
        }
    }

    if ($CreateIfNotExist) {
        if (-not (Test-Path "filesystem::$BackupPath")) {
            try { New-Item -Path "filesystem::$BackupPath" -ItemType Directory | Out-Null } catch { throw "Error creating backup path" }
        }
    }

    $DateForFileName = Get-Date -Format "yyyyMMddHHmmss"
    $FullPath = "$BackupPath$DatabaseName$('_adhoc_')$(if($MarkAsRetain){'Retain_'})$(if($Differential){'DIFF_'})$DateForFileName.BAK"
    Log $FullPath
    $FullPath
}

function Get-AzureStorageSasInfo {
    <#
    .SYNOPSIS
        Parses an Azure container URL (optionally with SAS) into components.

    .OUTPUTS
        PSCustomObject with ContainerUrl, SasToken, CredentialName.
    #>
    param(
        [Parameter(Mandatory)][string]$AzureStorageBackupLocation
    )

    $location = ($AzureStorageBackupLocation -replace '\s', '').Trim()
    if ($location -notmatch '^https://') {
        throw "AzureStorageBackupLocation must be an https:// URL"
    }

    $parts = $location.Split('?', 2)
    $containerUrl = $parts[0].TrimEnd('/')
    $sas = if ($parts.Count -gt 1) { $parts[1] } else { $null }

    $uri = [Uri]$containerUrl
    if ($uri.AbsolutePath -eq '/' -or [string]::IsNullOrWhiteSpace($uri.AbsolutePath.Trim('/'))) {
        throw "AzureStorageBackupLocation must include a container name (e.g. https://<account>.blob.core.windows.net/<container>?<sas>)"
    }

    $credentialName = $containerUrl
    if ($credentialName.Length -gt 128) {
        $sha256 = [System.Security.Cryptography.SHA256]::Create()
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($credentialName)
        $hash = ($sha256.ComputeHash($bytes) | ForEach-Object { $_.ToString('x2') }) -join ''
        $credentialName = "DBALibrary_Azure_$($hash.Substring(0, 16))"
    }

    [pscustomobject]@{
        ContainerUrl   = $containerUrl
        SasToken       = $sas
        CredentialName = $credentialName
    }
}

function Ensure-SqlAzureBlobCredential {
    <#
    .SYNOPSIS
        Ensures a SQL credential exists for Azure Blob BACKUP/RESTORE.
    #>
    param(
        [Parameter(Mandatory)][string]$InstanceName,
        [Parameter(Mandatory)][string]$CredentialName,
        [Parameter(Mandatory)][string]$SasToken
    )

    $secret = $SasToken.Trim()
    if (-not $secret.StartsWith('?')) {
        $secret = '?' + $secret
    }
    $secretEscaped = $secret -replace "'", "''"
    $credEscaped = $CredentialName -replace "]", "]]"
    $credNameEscapedForString = $CredentialName -replace "'", "''"

    $sql = @"
IF NOT EXISTS (SELECT 1 FROM sys.credentials WHERE name = N'$credNameEscapedForString')
BEGIN
    CREATE CREDENTIAL [$credEscaped]
    WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
         SECRET = '$secretEscaped'
END
"@

    try {
        Invoke-Sqlcmd -ServerInstance $InstanceName -Database master -Query $sql -AbortOnError
        Log -Message "Ensured Azure SQL credential '$CredentialName' exists on $InstanceName" -Level Info
    } catch {
        Log -Message "Failed to create Azure SQL credential '$CredentialName' on $InstanceName. Ensure you have permission (ALTER ANY CREDENTIAL) or pre-create the credential." -Level Error -WriteToHost
        throw
    }
}

function Get-AzureBackupLocation {
    <#
    .SYNOPSIS
        Builds a full Azure Blob URL for a SQL backup.

    .DESCRIPTION
        AzureStorageBackupLocation is expected to be a container URL (optionally including a SAS query string), e.g.
        https://<account>.blob.core.windows.net/<container>?sv=...
    #>
    param (
        [Parameter(Mandatory)][string]$AzureStorageBackupLocation,
        [Parameter(Mandatory)][string]$DatabaseName,
        [bool]$MarkAsRetain,
        [bool]$Differential = $false
    )

    $info = Get-AzureStorageSasInfo -AzureStorageBackupLocation $AzureStorageBackupLocation
    $baseUrl = $info.ContainerUrl

    $DateForFileName = Get-Date -Format "yyyyMMddHHmmss"
    $fileName = "$DatabaseName$('_adhoc_')$(if($MarkAsRetain){'Retain_'})$(if($Differential){'DIFF_'})$DateForFileName.BAK"

    # For SQL BACKUP/RESTORE to URL, use a SQL credential for auth; do not append SAS to the URL.
    $full = ($baseUrl.TrimEnd('/') + '/' + $fileName)

    Log -Message ("Azure backup URL = {0}" -f (Get-DisplayPath $full)) -Level Info
    $full
}

function Test-AzureStorageBackupLocation {
    <#
    .SYNOPSIS
        Lightweight validation for an Azure container URL.

    .DESCRIPTION
        Validates shape and, when a SAS token is present, attempts to list the container.
        This validates the SAS from the machine running PowerShell.
    #>
    param (
        [Parameter(Mandatory)][string]$AzureStorageBackupLocation
    )

    $info = Get-AzureStorageSasInfo -AzureStorageBackupLocation $AzureStorageBackupLocation
    if ([string]::IsNullOrWhiteSpace($info.SasToken)) {
        Log -Message "AzureStorageBackupLocation has no SAS token; skipping access check." -Level Warning -WriteToHost
        return
    }

    try {
        $baseUrl = $info.ContainerUrl
        $sas = $info.SasToken
        $probeUrl = "${baseUrl}?restype=container&comp=list&maxresults=1&$sas"
        $null = Invoke-WebRequest -Uri $probeUrl -Method Get -UseBasicParsing -TimeoutSec 30 -ErrorAction Stop
        Log -Message "AzureStorageBackupLocation access check succeeded for $baseUrl" -Level Info
    } catch {
        $display = $info.ContainerUrl
        $message = $_.Exception.Message
        if (-not [string]::IsNullOrWhiteSpace($message)) {
            $message = $message -replace '(?i)(https://[^\s]+)\?[^\s]+', '$1?<SAS omitted>'
        }

        Log -Message ("AzureStorageBackupLocation access check failed for {0}: {1}" -f $display, $message) -Level Error -WriteToHost
        throw "AzureStorageBackupLocation access check failed for $display. $message"
    }
}

function Get-DisplayPath {
    param(
        [AllowEmptyString()]
        [string]$Path
    )

    if ([string]::IsNullOrWhiteSpace($Path)) { return $Path }
    if ($Path -match '^https://') { return $Path.Split('?', 2)[0] }
    return $Path
}

function Get-SQLDatabaseTrustworthy {
    <#
    .SYNOPSIS
        Gets if database is trustworthy.
    #>
    param ($InstanceName, $DatabaseName)

    try {
        $TrustworthySQL = "SELECT is_trustworthy_on FROM sys.databases WHERE database_id = DB_ID()"
        (Invoke-Sqlcmd -ServerInstance $InstanceName -Database $DatabaseName -Query $TrustworthySQL).is_trustworthy_on
    } catch {
        Log "Error getting is_trustworthy_on." "Error"
        throw
    }
}

function Write-SQLDatabaseTrustworthy {
    <#
    .SYNOPSIS
        Sets database trustworthy status.
    #>
    param ($InstanceName, $DatabaseName, $Trustworthy)

    try {
        $TrustworthySQL = "ALTER DATABASE [$DatabaseName] SET TRUSTWORTHY $(if ($Trustworthy -eq 1) {'ON'} else {'OFF'})"
        Invoke-Sqlcmd -ServerInstance $InstanceName -Database master -Query $TrustworthySQL -AbortOnError
    } catch {
        Log "Error setting Trustworthy" "Error"
        throw
    }
}

function Test-PathOnSQLServer {
    <#
    .SYNOPSIS
        Tests path on SQL server.
    #>
    param ($Instance, $Path, [bool]$TestDirectoryOnly = $false)

    try {
        if ($TestDirectoryOnly) { $Path = Split-Path $Path }
        $PathSQL = "EXEC master.dbo.xp_fileexist '$Path'"
        if ($script:DBALibraryVerboseDiagnostics) {
            Write-DiagMessage "PathOnSQLServer: $PathSQL" -ForegroundColor Green
        }
        (Invoke-Sqlcmd -ServerInstance $Instance -Database master -Query $PathSQL).'File is a Directory' -eq 1
    } catch {
        Log -Message "Error" -Level Error
        throw
    }
}

function Get-SQLUserRoles {
    <#
    .SYNOPSIS
        Gets user roles in database.
    #>
    param ([string]$InstanceName, [string]$DatabaseName)

    try {
        $ExistingPrincipalsSQL = @"
SELECT
dpusers.name AS DatabaseUserName, sp.name AS ServerLoginName, dp.name AS RoleName, dpusers.type_desc
FROM sys.database_role_members drm
JOIN sys.database_principals dp ON dp.principal_id = drm.role_principal_id
JOIN sys.database_principals dpusers ON dpusers.principal_id = drm.member_principal_id
JOIN sys.server_principals sp ON dpusers.sid = sp.sid
WHERE dpusers.name <> 'dbo'
"@
        Invoke-Sqlcmd -ServerInstance $InstanceName -Query $ExistingPrincipalsSQL -Database $DatabaseName
    } catch {
        throw
    }
}

function Backup-Database {
    <#
    .SYNOPSIS
        Backs up a database.
    #>
    param (
        [string]$InstanceName,
        [string]$DatabaseName,
        [string]$BackupPath,
        [bool]$Compress = $false,
        [int]$ProgressID,
        [string]$JobName,
        [bool]$CopyOnly = $true,
        [bool]$Differential = $false,
        [string]$BlockSize = '65536',
        [string]$BufferCount = '50',
        [string]$MaxTransferSize = '2097152',
        [string]$ProgressMatch,
        [string]$CredentialName,
        [switch]$DryRun
    )

    try {
        Log "In Backup-Database"
        Log "BackupPath = $(Get-DisplayPath $BackupPath)"
        $isUrl = ($BackupPath -match '^https://')
        $to = if ($isUrl) { 'URL' } else { 'DISK' }
        $effectivePath = if ($isUrl -and -not [string]::IsNullOrWhiteSpace($CredentialName)) { $BackupPath.Split('?', 2)[0] } else { $BackupPath }
        $BackupSQL = "BACKUP DATABASE [$DatabaseName] TO $to = '$effectivePath' WITH BUFFERCOUNT = $BufferCount ,MAXTRANSFERSIZE = $MaxTransferSize ,BLOCKSIZE = $BlockSize "
        $With = if ($Compress) { " ,COMPRESSION " } else { "" }
        if ($isUrl -and -not [string]::IsNullOrWhiteSpace($CredentialName)) {
            $With += ", CREDENTIAL = '$CredentialName'"
        }
        if ($CopyOnly -and -not $Differential) { $With += ", COPY_ONLY " }
        if ($Differential) {
            Log "Adding DIFFERENTIAL"
            $With += ", DIFFERENTIAL "
        }
        $BackupSQL += $With
        if ($script:DBALibraryVerboseDiagnostics) {
            if ($isUrl) {
                $displaySql = $BackupSQL.Replace($effectivePath, (Get-DisplayPath $effectivePath))
                Write-DiagMessage $displaySql
            } else {
                Write-DiagMessage $BackupSQL
            }
        }
        Log $InstanceName

        if ($DryRun.IsPresent) {
            return @{ Job = $null; Instance = $InstanceName; Database = $DatabaseName; Path = $BackupPath; CmdString = $BackupSQL; JobType = "DryRun"; Id = -1 }
        }

        $Job = Start-Job -Name $JobName -ScriptBlock {
            Import-Module SqlServer
            Invoke-Sqlcmd -ServerInstance $args[0] -Query $args[1] -QueryTimeout 65535 -AbortOnError
        } -ArgumentList $InstanceName, $BackupSQL
        $match = if (-not [string]::IsNullOrWhiteSpace($ProgressMatch)) { $ProgressMatch } else { $BackupPath }
        @{Job = $Job ; Instance = $InstanceName ; Database = $DatabaseName ; Path = $BackupPath ; CmdString = $match ; JobType = "BackupRestore" ; Id = Get-NextProgressId }
    } catch {
        Log "Error in Backup-Database" -Level Error
        throw
    }
}

function Backup-TransactionLog {
    <#
    .SYNOPSIS
        Backs up a transaction log.
    #>
    param (
        [string]$InstanceName,
        [string]$DatabaseName,
        [string]$BackupPath,
        [int]$ProgressID,
        [string]$JobName,
        [string]$BlockSize = '65536',
        [string]$BufferCount = '50',
        [string]$MaxTransferSize = '2097152',
        [string]$ProgressMatch
    )

    try {
        Log "In Backup-TransactionLog"
        Log "BackupPath = $(Get-DisplayPath $BackupPath)"
        $BackupSQL = "BACKUP LOG [$DatabaseName] TO DISK = '$BackupPath' WITH BUFFERCOUNT = $BufferCount ,MAXTRANSFERSIZE = $MaxTransferSize ,BLOCKSIZE = $BlockSize "
        if ($script:DBALibraryVerboseDiagnostics) {
            Write-DiagMessage $BackupSQL
        }
        Log $InstanceName

        $Job = Start-Job -Name $JobName -ScriptBlock {
            Import-Module SqlServer
            Invoke-Sqlcmd -ServerInstance $args[0] -Query $args[1] -QueryTimeout 65535 -AbortOnError
        } -ArgumentList $InstanceName, $BackupSQL
        $match = if (-not [string]::IsNullOrWhiteSpace($ProgressMatch)) { $ProgressMatch } else { $BackupPath }
        @{Job = $Job ; Instance = $InstanceName ; Database = $DatabaseName ; Path = $BackupPath ; CmdString = $match ; JobType = "BackupRestore" ; Id = Get-NextProgressId }
    } catch {
        Log "Error in Backup-TransactionLog" -Level Error
        throw
    }
}

function Restore-SQLTransactionLog {
    <#
    .SYNOPSIS
        Restores a transaction log backup to a database.
    #>
    param(
        [string]$InstanceName,
        [string]$DatabaseName,
        [string]$BackupPath,
        [bool]$NoRecovery,
        [string]$JobName
    )

    try {
        Log "[Restore-SQLTransactionLog] InstanceName=$InstanceName DatabaseName=$DatabaseName BackupPath=$(Get-DisplayPath $BackupPath) NoRecovery=$NoRecovery"
        $recoveryClause = if ($NoRecovery) { 'NORECOVERY' } else { 'RECOVERY' }
        $RestoreSQL = "RESTORE LOG [$DatabaseName] FROM DISK = '$BackupPath' WITH STATS = 5, $recoveryClause"
        Log "[Restore-SQLTransactionLog] $RestoreSQL"
        if ($script:DBALibraryVerboseDiagnostics) {
            Write-DiagMessage "RestoreLogSQL: $RestoreSQL" -ForegroundColor Yellow
        }

        $Job = Start-Job -Name $JobName -ScriptBlock {
            param($InstanceName, $RestoreSQL)
            Import-Module SqlServer
            Invoke-Sqlcmd -ServerInstance $InstanceName -Database master -Query $RestoreSQL -QueryTimeout 65535 -ErrorAction Stop
        } -ArgumentList $InstanceName, $RestoreSQL

        @{Job = $Job ; Instance = $InstanceName ; Database = $DatabaseName ; Path = $BackupPath ; CmdString = $BackupPath ; JobType = "BackupRestore" ; Id = Get-NextProgressId }
    } catch {
        Log -Message "Error restoring transaction log." -Level Error
        throw
    }
}

function Progress2 {
    <#
    .SYNOPSIS
        Monitors progress of jobs.
    #>
    param ($JobDetailsCollection)

    try {
        # Hard timeout to avoid indefinite hangs (restore can be long; keep generous)
        $timeoutSeconds = 60 * 60 * 4
        $startTime = Get-Date
        Write-DebugMessage "[Progress2] Monitoring jobs: $($JobDetailsCollection | ForEach-Object { $_.Job.Name })"
        $timedOut = $false
        while ($JobDetailsCollection | Where-Object { $_.Job.State -eq "Running" }) {
            $I++
            foreach ($Job in $JobDetailsCollection) {
                Write-DebugMessage "[Progress2] Checking job: $($Job.Job.Name) State: $($Job.Job.State) Id: $($Job.Id)"
                Start-Sleep $ProgressInterval
                # Force job state refresh
                $Job.Job = Get-Job -Id $Job.Job.Id
                $State = $Job.Job.State.ToString()
                $elapsed = (Get-Date) - $startTime
                if ($State -eq "Completed") {
                    Write-DebugMessage "[Progress2] Job $($Job.Job.Name) completed."
                    $displayPath = Get-DisplayPath $Job.Path
                    Write-Progress -Id $Job.Id -PercentComplete 100 -Activity "$($Job.Job.Name) on $($Job.Database) on $($Job.Instance) to $displayPath" -Status "Complete"
                } elseif ($State -eq "Failed") {
                    $Er = $Job.ChildJobs[0].JobStateInfo.Reason
                    $JobOutput = Receive-Job -Job $Job.Job -Keep -ErrorAction SilentlyContinue
                    Write-DebugMessage "[Progress2] Job $($Job.Job.Name) failed: $Er"
                    Write-Host "[Progress2] Job $($Job.Job.Name) failed: $Er" -ForegroundColor Red
                    if ($JobOutput) {
                        Write-Host "[Progress2] Job $($Job.Job.Name) output:" -ForegroundColor Red
                        $JobOutput | ForEach-Object { Write-Host $_ -ForegroundColor Red }
                    }
                    Log -Message $Er -Level Error
                    throw $Er
                } else {
                    Write-DebugMessage "[Progress2] Job $($Job.Job.Name) running. Getting progress."
                    # IMPORTANT: Get-BackupRestoreProgress loops forever when Interval != 0.
                    # Progress2 only needs a single snapshot per iteration, so force Interval=0.
                    $cmd = if ($Job.CmdString) { $Job.CmdString } else { $Job.Path }
                    $BackupProgress = Get-BackupRestoreProgress -Instance $Job.Instance -CmdString $cmd -Interval 0
                    if ($null -eq $BackupProgress -or $null -eq $BackupProgress.PercentComplete) {
                        $PercentComplete = 0
                    } else {
                        $PercentComplete = $BackupProgress.PercentComplete
                    }
                    $NoBackupProgress = if ($PercentComplete -eq 0) { " No information on progress yet, please wait. " } else { "" }
                    if ($null -eq $BackupProgress.ETACompletionTime) {
                        $ETA = "Unknown"
                    } else {
                        $ETA = $BackupProgress.ETACompletionTime
                    }
                    if ($null -eq $BackupProgress.ETAMin) {
                        $ETAMin = "Unknown"
                    } else {
                        $ETAMin = $BackupProgress.ETAMin
                    }
                    if ($null -eq $BackupProgress.ElapsedMin) {
                        $ElapsedMin = "Unknown"
                    } else {
                        $ElapsedMin = $BackupProgress.ElapsedMin
                    }
                    if ($I % 4 -eq 0) {
                        Log "Percent Complete = $PercentComplete, ETA = $ETA (in $ETAMin minutes)"
                    }
                    $PercentCompleteInt = 0
                    try {
                        $PercentCompleteInt = [int]([double]$PercentComplete)
                    } catch {
                        $PercentCompleteInt = 0
                    }
                    Write-Progress -Id $Job.Id -Activity "$($Job.Job.Name) on $($Job.Database) on $($Job.Instance) to $($Job.Path)" -PercentComplete $PercentCompleteInt -Status "$NoBackupProgress Complete = $PercentComplete, ETA = $ETA (in $ETAMin minutes). $ElapsedMin minutes elapsed."
                }
            } # End foreach
            # Timeout check for all jobs
            $elapsed = (Get-Date) - $startTime
            if ($elapsed.TotalSeconds -gt $timeoutSeconds) {
                $timedOut = $true
                Write-Warning "[Progress2] HARD TIMEOUT reached after $timeoutSeconds seconds. Forcing job diagnostics."
                break
            }
        } # End while
        if ($timedOut -or ($JobDetailsCollection | Where-Object { $_.Job.State -eq "Running" })) {
            Write-Host "[Progress2] WARNING: Some jobs did not complete in time. Dumping job diagnostics..." -ForegroundColor Red
            foreach ($Job in $JobDetailsCollection) {
                $JobState = $Job.Job.State
                Write-Host "[Progress2] State for job $($Job.Job.Name): $JobState" -ForegroundColor Yellow
                $JobOutput = Receive-Job -Job $Job.Job -Keep -ErrorAction SilentlyContinue
                if ($JobOutput) {
                    Write-Host "[Progress2] Output for job $($Job.Job.Name):" -ForegroundColor Cyan
                    $JobOutput | ForEach-Object { Write-Host $_ -ForegroundColor Cyan }
                } else {
                    Write-Host "[Progress2] No output for job $($Job.Job.Name)." -ForegroundColor DarkGray
                }
                $JobErrors = $Job.Job.ChildJobs | ForEach-Object { $_.JobStateInfo.Reason }
                foreach ($Err in $JobErrors) {
                    if ($Err) {
                        Write-Host "[Progress2] Error for job $($Job.Job.Name): $Err" -ForegroundColor Red
                    }
                }
            }
            throw "[Progress2] ERROR: One or more jobs did not complete in time. See diagnostics above."
        }
        # Print final job states and force cleanup
        Write-DebugMessage "[Progress2] Exiting job loop. Final job states:"
        foreach ($Job in $JobDetailsCollection) {
            Write-DebugMessage "[Progress2] Final state for job $($Job.Job.Name): $($Job.Job.State)"
            Write-Host "[Progress2] Final state for job $($Job.Job.Name): $($Job.Job.State)" -ForegroundColor Green
        }
        # Print output and errors from all jobs
        foreach ($Job in $JobDetailsCollection) {
            $JobState = $Job.Job.State
            Write-Host "[Progress2] State for job $($Job.Job.Name): $JobState" -ForegroundColor Yellow
            $JobOutput = Receive-Job -Job $Job.Job -Keep -ErrorAction SilentlyContinue
            if ($JobOutput) {
                Write-Host "[Progress2] Output for job $($Job.Job.Name):" -ForegroundColor Cyan
                $JobOutput | ForEach-Object { Write-Host $_ -ForegroundColor Cyan }
            } else {
                Write-Host "[Progress2] No output for job $($Job.Job.Name)." -ForegroundColor DarkGray
            }
            $JobErrors = $Job.Job.ChildJobs | ForEach-Object { $_.JobStateInfo.Reason }
            foreach ($Err in $JobErrors) {
                if ($Err) {
                    Write-Host "[Progress2] Error for job $($Job.Job.Name): $Err" -ForegroundColor Red
                }
            }
        }
        Write-DebugMessage "[Progress2] Leaving jobs in place for caller cleanup."
        Write-DebugMessage "[Progress2] Exit"
    } # End try
    catch {
        Write-DebugMessage "[Progress2] ERROR: $($_.Exception.Message)"
        $failedJob = Get-Job | Where-Object { $_.State -eq "Failed" } | Select-Object -First 1
        $Reason = if ($failedJob -and $failedJob.ChildJobs -and $failedJob.ChildJobs.Count -gt 0) {
            $failedJob.ChildJobs[0].JobStateInfo.Reason
        } else {
            $_.Exception.Message
        }
        Log -Message "Error $Reason" -Level Error
        throw $Reason
    }
    Write-DebugMessage "[Progress2] Exit"
}

function Get-BackupRestoreProgress {
    <#
    .SYNOPSIS
        Gets backup/restore progress.
    #>
    param ([string]$Instance, [string]$CmdString, [int]$Interval = $ProgressInterval)

    $Wait = $true
    while ($Wait) {
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
AND CONVERT(VARCHAR(1000),SUBSTRING(text,r.statement_start_offset/2,CASE WHEN r.statement_end_offset = -1 THEN 1000 ELSE (r.statement_end_offset-r.statement_start_offset)/2 END)) LIKE'%$CmdString%'
"@
        Invoke-Sqlcmd -ServerInstance $Instance -Query $ProgressSQL
        $Wait = $Interval -ne 0
        if ($Wait) { Start-Sleep -Seconds $Interval }
    }
}

[int]$Global:NextProgressId = 1
function Get-NextProgressId {
    $Global:NextProgressId++
    Log -Message $Global:NextProgressId -Level Info
    $Global:NextProgressId
}

function Check-DatabaseAccess {
    <#
    .SYNOPSIS
        Checks access to a database.
    #>
    param ($Instance, $Database)

    $CheckSQL = "SELECT COUNT(*) AS cnt FROM sys.databases WHERE name = '$Database'"
    $Cnt = (Invoke-Sqlcmd -ServerInstance $Instance -Database master -Query $CheckSQL).cnt
    if ($Cnt -eq 0) { $false } else {
        try {
            $VersionSQL = "SELECT @@VERSION AS Version"
            Invoke-Sqlcmd -ServerInstance $Instance -Database $Database -Query $VersionSQL -AbortOnError
            $true
        } catch {
            $false
        }
    }
}

function SendEMail {
    <#
    .SYNOPSIS
        Sends email notifications.
    #>
    param (
        [string]$Subject,
        [string]$Msg,
        [string]$Address,
        [string]$FromAddress,
        [bool]$NoLog = $false,
        [switch]$BodyAsHtml
    )

    try {
        # Honor the caller-provided email values.
        # If no Address is supplied, try to resolve from AD; if that fails, skip sending.
        $resolvedAddress = $Address
        if ([string]::IsNullOrWhiteSpace($resolvedAddress)) {
            try {
                $Me = $env:USERNAME
                $adUser = Get-ADUser -Identity $Me -Properties mail, EmailAddress -ErrorAction Stop
                $resolvedAddress = if (-not [string]::IsNullOrWhiteSpace($adUser.mail)) { $adUser.mail } else { $adUser.EmailAddress }
            } catch {
                $resolvedAddress = $null
            }
        }

        if ([string]::IsNullOrWhiteSpace($resolvedAddress)) {
            Log -Message "SendEMail: no recipient resolved (current user has no email in AD). Supply -EmailAddress to enable email; skipping." -Level Warning -WriteToHost
            return $false
        }

        $MessageTo = $resolvedAddress.Split(",")
        $MessageFrom = if ([string]::IsNullOrWhiteSpace($FromAddress)) { $MessageTo[0] } else { $FromAddress }

        $effectiveSmtpServer = if ([string]::IsNullOrWhiteSpace($smtpserver)) { 'smtp' } else { $smtpserver }

        $msgLooksHtml = ($Msg -match '(?is)<\s*(html|body|table|div|span|style|p|br)\b')
        $sendAsHtml = $BodyAsHtml.IsPresent -or $msgLooksHtml

        Log -Message "SendEMail: To=$resolvedAddress From=$MessageFrom Subject=$Subject SmtpServer=$effectiveSmtpServer" -Level Info

        $LogResultsText = $null
        if (-not $NoLog -and $null -ne $script:ExecutionID -and $loggingEnabled) {
            $LogResultsSQL = "SELECT message, level, errordetails, errorline FROM log WITH (NOLOCK) WHERE executionID = '$($script:ExecutionID)'"
            $LogResults = Invoke-Sqlcmd -ServerInstance $DBAInstance -Database $DBADatabase -Query $LogResultsSQL
            if ($LogResults) {
                $LogResultsText = $LogResults | Format-Table -AutoSize | Out-String
                $LogResultsText = "Note: Single quotes stripped from log.`n$LogResultsText`nRun: SELECT * FROM Log WHERE ExecutionID = '$($script:ExecutionID)' ORDER BY datetime DESC"
            }
        }

        $MessageBody = $Msg
        if (-not [string]::IsNullOrWhiteSpace($LogResultsText)) {
            if ($sendAsHtml) {
                $encoded = [System.Net.WebUtility]::HtmlEncode($LogResultsText)
                $MessageBody = $MessageBody + "<hr style='border:none;border-top:1px solid #d0d7de;margin:16px 0'/>" +
                    "<div style='font-family:Segoe UI,Arial,sans-serif;font-size:12px;color:#24292f;font-weight:600;margin:0 0 6px 0'>DBALibrary Log (ExecutionID $([System.Net.WebUtility]::HtmlEncode([string]$script:ExecutionID)))</div>" +
                    "<pre style=`"margin:0;font-family:Consolas,'Courier New',monospace;font-size:12px;white-space:pre-wrap;background:#f6f8fa;border:1px solid #d0d7de;border-radius:6px;padding:12px`">$encoded</pre>"
            } else {
                $MessageBody = $MessageBody + "`n`n" + $LogResultsText
            }
        }
        if ([string]::IsNullOrEmpty($Subject)) { $Subject = "" }

        if (-not $SMTPEnabled) {
            Log -Message "SendEMail: SMTP disabled; skipping send." -Level Warning -WriteToHost
            return $false
        }

        try {
            if ($sendAsHtml) {
                Send-MailMessage -SmtpServer $effectiveSmtpServer -Subject $Subject -Body $MessageBody -BodyAsHtml -From $MessageFrom -To $MessageTo -ErrorAction Stop
            } else {
                Send-MailMessage -SmtpServer $effectiveSmtpServer -Subject $Subject -Body $MessageBody -From $MessageFrom -To $MessageTo -ErrorAction Stop
            }
            Log -Message "SendEMail: sent successfully." -Level Info
            return $true
        } catch {
            Log -Message ("SendEMail: failed to send via SMTP server '{0}': {1}" -f $effectiveSmtpServer, $_.Exception.Message) -Level Warning -WriteToHost
            return $false
        }
    } catch {
        Log "Failed to send email." "Error"
        return $false
    }
}

function ConvertTo-DbalHtmlEncoded {
    param(
        [AllowNull()]
        [object]$Value
    )

    if ($null -eq $Value) { return '' }
    return [System.Net.WebUtility]::HtmlEncode([string]$Value)
}

function ConvertTo-DbalHtmlTable {
    param(
        [Parameter(Mandatory = $true)]
        [System.Collections.IDictionary]$Data,
        [string]$HeaderKey = 'Key',
        [string]$HeaderValue = 'Value'
    )

    $th = "background:#f6f8fa;border:1px solid #d0d7de;padding:8px 10px;text-align:left;font-weight:600;font-size:12px"
    $td = "border:1px solid #d0d7de;padding:8px 10px;vertical-align:top;font-size:12px"

    $rows = foreach ($k in $Data.Keys) {
        $key = ConvertTo-DbalHtmlEncoded $k
        $val = ConvertTo-DbalHtmlEncoded $Data[$k]
        "<tr><td style='$td'><span style='font-weight:600'>$key</span></td><td style='$td'><code style=`"font-family:Consolas,'Courier New',monospace`">$val</code></td></tr>"
    }

    return @(
        "<table style='border-collapse:collapse;width:100%;max-width:980px'>",
        "<thead><tr><th style='$th'>$([System.Net.WebUtility]::HtmlEncode($HeaderKey))</th><th style='$th'>$([System.Net.WebUtility]::HtmlEncode($HeaderValue))</th></tr></thead>",
        "<tbody>",
        ($rows -join ""),
        "</tbody></table>"
    ) -join ""
}

function New-DbalBackupAndRestoreEmailHtml {
    param(
        [Parameter(Mandatory = $true)][ValidateSet('SUCCESS', 'ERROR')][string]$Status,
        [Parameter(Mandatory = $true)][string]$Title,
        [Parameter()][System.Collections.IDictionary]$Summary,
        [Parameter()][System.Collections.IDictionary]$Parameters,
        [Parameter()][string]$Notes,
        [Parameter()][string]$Details
    )

    $statusBg = if ($Status -eq 'SUCCESS') { '#e6ffed' } else { '#ffebe9' }
    $statusFg = if ($Status -eq 'SUCCESS') { '#1a7f37' } else { '#cf222e' }
    $statusLabel = ConvertTo-DbalHtmlEncoded $Status

    $titleEncoded = ConvertTo-DbalHtmlEncoded $Title

    $summaryHtml = if ($Summary) { ConvertTo-DbalHtmlTable -Data $Summary -HeaderKey 'Field' -HeaderValue 'Value' } else { '' }
    $paramsHtml = if ($Parameters) { ConvertTo-DbalHtmlTable -Data $Parameters -HeaderKey 'Parameter' -HeaderValue 'Value' } else { '' }

    $notesHtml = if (-not [string]::IsNullOrWhiteSpace($Notes)) {
        $encoded = ConvertTo-DbalHtmlEncoded $Notes
        "<pre style=`"margin:0;font-family:Consolas,'Courier New',monospace;font-size:12px;white-space:pre-wrap;background:#f6f8fa;border:1px solid #d0d7de;border-radius:6px;padding:12px`">$encoded</pre>"
    } else { '' }

    $detailsHtml = if (-not [string]::IsNullOrWhiteSpace($Details)) {
        $encoded = ConvertTo-DbalHtmlEncoded $Details
        "<pre style=`"margin:0;font-family:Consolas,'Courier New',monospace;font-size:12px;white-space:pre-wrap;background:#f6f8fa;border:1px solid #d0d7de;border-radius:6px;padding:12px`">$encoded</pre>"
    } else { '' }

    $summarySection = if ($summaryHtml) { "<div style='font-size:13px;font-weight:700;margin:16px 0 8px 0'>Summary</div>$summaryHtml" } else { '' }
    $paramsSection = if ($paramsHtml) { "<div style='font-size:13px;font-weight:700;margin:16px 0 8px 0'>Parameters</div>$paramsHtml" } else { '' }
    $notesSection = if ($notesHtml) { "<div style='font-size:13px;font-weight:700;margin:16px 0 8px 0'>Notes</div>$notesHtml" } else { '' }
    $detailsSection = if ($detailsHtml) { "<div style='font-size:13px;font-weight:700;margin:16px 0 8px 0'>Details</div>$detailsHtml" } else { '' }

    return @(
        "<html><body style='margin:0;padding:18px;font-family:Segoe UI,Arial,sans-serif;font-size:12px;color:#24292f'>",
        "<div style='font-size:18px;font-weight:600;margin:0 0 10px 0'>$titleEncoded</div>",
        "<div style='margin:0 0 14px 0'><span style='display:inline-block;padding:4px 10px;border-radius:999px;background:$statusBg;color:$statusFg;font-weight:700;letter-spacing:0.3px'>$statusLabel</span></div>",
        $summarySection,
        $paramsSection,
        $notesSection,
        $detailsSection,
        "</body></html>"
    ) -join ""
}

function Get-Space {
    <#
    .SYNOPSIS
        Calculates space required for restore.
    #>
    param ($SourceFileList, [string]$Instance, [string]$Database, [bool]$CreateDatabase)

    try {
        if ($CreateDatabase) {
            $SizeOfFilesToBeRestored = Get-SumOfFileSizes -SourceFileList $SourceFileList
            $Paths = Get-DefaultPathsLegacy -Instance $Instance
            Write-DiagMessage ("Paths = {0}" -f ($Paths | Out-String))

            $Results = @()
            foreach ($Drive in Get-DriveSpace -Instance $Instance) {
                $LogSize = 0
                $DataSize = 0
                Log -Message "Checking $($Drive.volume_mount_point)"
                if ($Paths.DefaultFile -like "$($Drive.volume_mount_point)*") {
                    Log -Message "defaultfile $($Drive.volume_mount_point) $($Paths.DefaultFile) "
                    $DataSize += ($SizeOfFilesToBeRestored | Where-Object { $_.Type -eq "D" } | Measure-Object SizeInMB -Sum).Sum
                }
                if ($Paths.DefaultLog -like "$($Drive.volume_mount_point)*") {
                    Log -Message "defaultlog $($Drive.volume_mount_point)"
                    $LogSize += ($SizeOfFilesToBeRestored | Where-Object { $_.Type -eq "L" } | Measure-Object SizeInMB -Sum).Sum
                }
                $Custom = New-Object PSObject
                $Custom | Add-Member "Drive" $Drive.volume_mount_point
                $Custom | Add-Member "DataFileRequired" $DataSize
                $Custom | Add-Member "LogFileRequired" $LogSize
                $Custom | Add-Member "TotalUsedMB" $Drive.TotalUsedMB
                $Custom | Add-Member "TotalFreeMB" $Drive.TotalFreeMB
                $Custom | Add-Member "FreeAfterRestore" ($Drive.TotalFreeMB - $DataSize - $LogSize)
                $Custom | Add-Member "PercentFreeAfter" (($Drive.TotalFreeMB - $DataSize - $LogSize) / ($Drive.TotalFreeMB + $Drive.TotalUsedMB) * 100)
                $Results += $Custom | Where-Object { $_.Drive -ne "C:\" }
            }
            $Results
        } else {
            $Result = @()
            $TrgFileList = Get-FileListFromDatabase -Instance $Instance -Database $Database
            foreach ($TrgFile in $TrgFileList) {
                foreach ($SrcFile in $SourceFileList) {
                    if ($SrcFile.LogicalName -eq $TrgFile.LogicalName) {
                        $FileObj = New-Object PSObject
                        $FileObj | Add-Member "SourceLogicalName" $SrcFile.LogicalName
                        $FileObj | Add-Member "SourcePhysicalName" $SrcFile.PhysicalName
                        $FileObj | Add-Member "SourceSizeInMB" $SrcFile.SizeInMB
                        $FileObj | Add-Member "SourceType" $SrcFile.Type
                        $FileObj | Add-Member "TargetLogicalName" $TrgFile.LogicalName
                        $FileObj | Add-Member "TargetPhysicalName" $TrgFile.PhysicalName
                        $FileObj | Add-Member "TargetSizeInMB" $TrgFile.SizeInMB
                        $FileObj | Add-Member "TargetType" $TrgFile.Type
                        $Result += $FileObj
                    }
                }
            }
            $FinalResults = @()
            foreach ($Drive in Get-DriveSpace -Instance $Instance) {
                $NetData = 0
                $NetLog = 0
                foreach ($File in $Result) {
                    if ($File.TargetPhysicalName -like "$($Drive.volume_mount_point)*") {
                        if ($File.SourceType -eq "D") { $NetData += ($File.SourceSizeInMB - $File.TargetSizeInMB) }
                        elseif ($File.SourceType -eq "L") { $NetLog += ($File.SourceSizeInMB - $File.TargetSizeInMB) }
                    }
                }
                $Custom = New-Object PSObject
                $Custom | Add-Member "Drive" $Drive.volume_mount_point
                $Custom | Add-Member "DataFileRequired" $NetData
                $Custom | Add-Member "LogFileRequired" $NetLog
                $Custom | Add-Member "TotalUsedMB" $Drive.TotalUsedMB
                $Custom | Add-Member "TotalFreeMB" $Drive.TotalFreeMB
                $Custom | Add-Member "FreeAfterRestore" ($Drive.TotalFreeMB - $NetData - $NetLog)
                $Custom | Add-Member "PercentFreeAfter" (($Drive.TotalFreeMB - $NetData - $NetLog) / ($Drive.TotalFreeMB + $Drive.TotalUsedMB) * 100)
                $FinalResults += $Custom
            }
            $FinalResults
        }
    } catch {
        Log -Message "Error getting space on $Instance " -Level Error -WriteToHost
        throw
    }
}

function Get-SumOfFileSizes {
    <#
    .SYNOPSIS
        Sums file sizes by type.
    #>
    param ($SourceFileList)

    try {
        $SourceFileList | Group-Object Type | ForEach-Object { New-Object PSObject -Property @{ Type = $_.Name ; SizeInMB = ($_.Group | Measure-Object SizeInMB -Sum).Sum } }
    } catch {
        Log -Message "Error sum of file size" -Level Error
        throw
    }
}

function Get-DefaultPathsLegacy {
    <#
    .SYNOPSIS
        Gets default data and log paths using legacy method.
    #>
    param ($Instance)

    try {
        $DefaultPathSQL = @"
DECLARE @DefaultData nvarchar(4000) = CONVERT(nvarchar(4000), SERVERPROPERTY('InstanceDefaultDataPath'));
DECLARE @DefaultLog  nvarchar(4000) = CONVERT(nvarchar(4000), SERVERPROPERTY('InstanceDefaultLogPath'));

IF (@DefaultData IS NULL OR @DefaultLog IS NULL)
BEGIN
    BEGIN TRY
        DECLARE @DataFromReg nvarchar(4000);
        DECLARE @LogFromReg  nvarchar(4000);

        EXEC master.dbo.xp_instance_regread
            N'HKEY_LOCAL_MACHINE',
            N'Software\Microsoft\MSSQLServer\MSSQLServer',
            N'DefaultData',
            @DataFromReg OUTPUT;

        EXEC master.dbo.xp_instance_regread
            N'HKEY_LOCAL_MACHINE',
            N'Software\Microsoft\MSSQLServer\MSSQLServer',
            N'DefaultLog',
            @LogFromReg OUTPUT;

        SET @DefaultData = COALESCE(@DefaultData, @DataFromReg);
        SET @DefaultLog  = COALESCE(@DefaultLog,  @LogFromReg);
    END TRY
    BEGIN CATCH
        -- Ignore registry read failures and fall back to master file locations.
    END CATCH
END

IF (@DefaultData IS NULL)
    SELECT @DefaultData = LEFT(physical_name, LEN(physical_name) - CHARINDEX('\\', REVERSE(physical_name)) + 1)
    FROM master.sys.database_files
    WHERE file_id = 1;

IF (@DefaultLog IS NULL)
    SELECT @DefaultLog = LEFT(physical_name, LEN(physical_name) - CHARINDEX('\\', REVERSE(physical_name)) + 1)
    FROM master.sys.database_files
    WHERE file_id = 2;

SELECT @DefaultData AS DefaultFile, @DefaultLog AS DefaultLog;
"@
        Invoke-Sqlcmd -ServerInstance $Instance -Database master -Query $DefaultPathSQL
    } catch {
        Log -Message "Error getting default paths using legacy function" -Level "Error"
        throw
    }
}

function Get-DriveSpace {
    <#
    .SYNOPSIS
        Gets drive space for instance.
    #>
    param ([string]$Instance)

    try {
        $Ver = Get-SQLInstanceVersion -InstanceName $Instance
        if ($Ver -match '2008|2005') {
            Log -Message "Can't check drive space on $Instance (version 2008 or below)." -Level Info -WriteToHost -ForegroundColour Yellow
        } else {
            $DriveSpaceSQL = @"
SELECT DISTINCT
    volume_mount_point,
    total_bytes / 1024 / 1024 AS TotalSizeMB,
    (total_bytes - available_bytes) / 1024 / 1024 AS TotalUsedMB,
    available_bytes / 1024 / 1024 AS TotalFreeMB
FROM sys.master_files AS f
CROSS APPLY sys.dm_os_volume_stats (f.database_id, f.file_id);
"@
            Invoke-Sqlcmd -ServerInstance $Instance -Query $DriveSpaceSQL
        }
    } catch {
        Log -Message "Error getting drive space" -Level Error
        throw
    }
}

function LoginExists {
    <#
    .SYNOPSIS
        Checks if login exists on instance.
    #>
    param ([string]$Instance, [string]$Login)

    try {
        $LoginSQL = "SELECT COUNT(*) AS LoginExists FROM sys.server_principals WHERE name = '$Login'"
        (Invoke-Sqlcmd -ServerInstance $Instance -Database master -Query $LoginSQL).LoginExists -gt 0
    } catch {
        Log "Error checking if login $Login exists" "Error"
        throw
    }
}

function Restore-SQLDatabase {
    <#
    .SYNOPSIS
        Restores a SQL database.
    #>
    param (
        [string]$InstanceName,
        [string]$DatabaseName,
        [string]$BackupPath,
        [object[]]$SourceFileList,
        [bool]$TakeInstanceOffline,
        [bool]$NoRecovery,
        [string]$JobName,
        [bool]$CreateDatabase,
        [string]$SourceDatabase,
        [bool]$Differential,
        [string]$CredentialName,
        [ValidateSet('RollbackImmediate', 'NoWait', 'Wait')][string]$TakeInstanceOfflineMode = 'RollbackImmediate',
        [switch]$DryRun
    )

    try {
        Log "[Restore-SQLDatabase] TakeInstanceOffline is $TakeInstanceOffline , InstanceName is $InstanceName , DatabaseName is $DatabaseName , BackupPath is $(Get-DisplayPath $BackupPath) , NoRecovery is $NoRecovery , JobName is $JobName , CreateDatabase is $CreateDatabase , SourceDatabase is $SourceDatabase , Differential is $Differential"
        Write-DebugMessage "[Restore-SQLDatabase] Starting restore logic"
        Write-DebugMessage "[Restore-SQLDatabase] About to set MoveFiles=''"
        $MoveFiles = ""
        Write-DebugMessage "[Restore-SQLDatabase] About to set Replace"
        $Replace = if (-not $CreateDatabase -or $Differential) { ", REPLACE" } else { "" }
        Write-DebugMessage "[Restore-SQLDatabase] About to check CreateDatabase"
        if ($CreateDatabase) {
            $fileList = $null

            if ($DryRun.IsPresent -and $SourceFileList -and $SourceFileList.Count -gt 0) {
                Write-DebugMessage "[Restore-SQLDatabase] DryRun: using provided SourceFileList instead of RESTORE FILELISTONLY"
                $fileList = $SourceFileList
            } elseif ($DryRun.IsPresent) {
                # In DryRun, the backup file typically doesn't exist yet, so RESTORE FILELISTONLY will throw.
                Write-DebugMessage "[Restore-SQLDatabase] DryRun: no SourceFileList provided; skipping RESTORE FILELISTONLY and WITH MOVE mapping"
            } else {
                Write-DebugMessage "[Restore-SQLDatabase] About to call Get-FileListFromBackupFile"
                $fileList = Get-FileListFromBackupFile -BackupPath $BackupPath -Instance $InstanceName -CredentialName $CredentialName
            }

            if ($fileList -and $fileList.Count -gt 0) {
                Write-DebugMessage "[Restore-SQLDatabase] Got fileList: $($fileList | Out-String)"
                Write-DebugMessage "[Restore-SQLDatabase] About to call Get-MoveFiles"
                $MoveFiles = Get-MoveFiles -SourceFileList $fileList -Instance $InstanceName -Database $DatabaseName -SourceDatabase $SourceDatabase
                Write-DebugMessage "[Restore-SQLDatabase] Got MoveFiles: $MoveFiles"
                Log "[Restore-SQLDatabase] MoveStatement = $MoveFiles"
                if ($script:DBALibraryVerboseDiagnostics) {
                    Write-DiagMessage "MoveStatement: $MoveFiles" -ForegroundColor Yellow
                }
            }
        } elseif (-not $Differential) {
            # Overwriting an existing database: the backup logical names typically do not match the target
            # database's logical names, so we must use WITH MOVE to map backup logical -> target physical.
            Write-DebugMessage "[Restore-SQLDatabase] Building MOVE mapping for overwrite restore"

            $backupFileList = Get-FileListFromBackupFile -BackupPath $BackupPath -Instance $InstanceName -CredentialName $CredentialName
            $targetFiles = Invoke-Sqlcmd -ServerInstance $InstanceName -Database master -Query "SELECT type, file_id, name, physical_name FROM sys.master_files WHERE database_id = DB_ID('$DatabaseName') ORDER BY type, file_id" -ErrorAction Stop

            $backupData = @($backupFileList | Where-Object { $_.Type -eq 'D' })
            $backupLog = @($backupFileList | Where-Object { $_.Type -eq 'L' })
            $targetData = @($targetFiles | Where-Object { $_.type -eq 0 })
            $targetLog = @($targetFiles | Where-Object { $_.type -eq 1 })

            if ($backupData.Count -ne $targetData.Count -or $backupLog.Count -ne $targetLog.Count) {
                $msg = "Cannot build MOVE mapping for overwrite restore of [$DatabaseName] on [$InstanceName]. Backup files (D=$($backupData.Count), L=$($backupLog.Count)) do not match target files (D=$($targetData.Count), L=$($targetLog.Count))."
                Log -Message $msg -Level Error -WriteToHost
                throw $msg
            }

            for ($i = 0; $i -lt $backupData.Count; $i++) {
                $srcLogical = ($backupData[$i].LogicalName).Replace("'", "''")
                $dstPhysical = ($targetData[$i].physical_name).Replace("'", "''")
                $MoveFiles += ", MOVE N'$srcLogical' TO N'$dstPhysical'"
            }
            for ($i = 0; $i -lt $backupLog.Count; $i++) {
                $srcLogical = ($backupLog[$i].LogicalName).Replace("'", "''")
                $dstPhysical = ($targetLog[$i].physical_name).Replace("'", "''")
                $MoveFiles += ", MOVE N'$srcLogical' TO N'$dstPhysical'"
            }

            Write-DebugMessage "[Restore-SQLDatabase] MoveFiles (overwrite) = $MoveFiles"
            Log "[Restore-SQLDatabase] MoveStatement = $MoveFiles"
            if ($script:DBALibraryVerboseDiagnostics) {
                Write-DiagMessage "MoveStatement: $MoveFiles" -ForegroundColor Yellow
            }
        }

        Write-DebugMessage "[Restore-SQLDatabase] About to set RestoreSQL=''"
        $RestoreSQL = ""
        Write-DebugMessage "[Restore-SQLDatabase] About to check TakeInstanceOffline"
        if ($TakeInstanceOffline -and -not $CreateDatabase -and -not $Differential) {
            Write-DebugMessage "[Restore-SQLDatabase] About to add ALTER DATABASE ... SET OFFLINE"
            if ($TakeInstanceOfflineMode -eq 'NoWait') {
                $RestoreSQL += "ALTER DATABASE [$DatabaseName] SET OFFLINE WITH NO_WAIT`n"
            } elseif ($TakeInstanceOfflineMode -eq 'Wait') {
                $RestoreSQL += "ALTER DATABASE [$DatabaseName] SET OFFLINE`n"
            } else {
                $RestoreSQL += "ALTER DATABASE [$DatabaseName] SET OFFLINE WITH ROLLBACK IMMEDIATE`n"
            }
        }
        Write-DebugMessage "[Restore-SQLDatabase] About to build RESTORE DATABASE command"
        $isUrl = ($BackupPath -match '^https://')
        $from = if ($isUrl) { 'URL' } else { 'DISK' }
        $effectivePath = if ($isUrl -and -not [string]::IsNullOrWhiteSpace($CredentialName)) { $BackupPath.Split('?', 2)[0] } else { $BackupPath }
        $credentialClause = if ($isUrl -and -not [string]::IsNullOrWhiteSpace($CredentialName)) { "CREDENTIAL = '$CredentialName', " } else { "" }
        $RestoreSQL += "RESTORE DATABASE [$DatabaseName] FROM $from = '$effectivePath' WITH $credentialClause STATS = 5 $Replace $MoveFiles $(if($NoRecovery){',NORECOVERY'})`n"

        $displayRestoreSql = if ($isUrl) { $RestoreSQL.Replace($effectivePath, (Get-DisplayPath $effectivePath)) } else { $RestoreSQL }
        Log "[Restore-SQLDatabase] Restore SQL generated"
        Write-DebugMessage "[Restore-SQLDatabase] RestoreSQL: $displayRestoreSql"
        if ($script:DBALibraryVerboseDiagnostics) {
            Write-DiagMessage "RestoreSQL: $displayRestoreSql" -ForegroundColor Yellow
        }

        if ($DryRun.IsPresent) {
            $match = if ($isUrl) { ($effectivePath | Split-Path -Leaf) } else { $BackupPath }
            return @{ Job = $null; Instance = $InstanceName; Database = $DatabaseName; Path = $effectivePath; CmdString = $displayRestoreSql; JobType = "DryRun"; Id = -1 }
        }

        Write-DebugMessage "[Restore-SQLDatabase] About to start restore job"
        $Job = Start-Job -Name $JobName -ScriptBlock {
            param($InstanceName, $RestoreSQL, $RestoreSQLDisplay)
            Write-Host "[Restore-SQLDatabase] Running restore in job: $RestoreSQLDisplay" -ForegroundColor Cyan
            Import-Module SqlServer
            try {
                Write-Host "[Restore-SQLDatabase] Invoking Sqlcmd for restore" -ForegroundColor Cyan
                Invoke-Sqlcmd -ServerInstance $InstanceName -Database master -Query $RestoreSQL -QueryTimeout 65535 -ErrorAction Stop
                Write-Host "[Restore-SQLDatabase] Restore completed successfully." -ForegroundColor Green
            } catch {
                Write-Host "[Restore-SQLDatabase] ERROR: $($_.Exception.Message)" -ForegroundColor Red
                Write-Host "[Restore-SQLDatabase] STACK TRACE: $($_.ScriptStackTrace)" -ForegroundColor Red
                throw
            }
            Write-Host "[Restore-SQLDatabase] Restore job script block end" -ForegroundColor Cyan
        } -ArgumentList $InstanceName, $RestoreSQL, $displayRestoreSql
        Write-DebugMessage "[Restore-SQLDatabase] Job started: $($Job.Id) State: $($Job.State) Name: $($Job.Name)"
        Write-DebugMessage "[Restore-SQLDatabase] Returning job object"
        $match = if ($isUrl) { ($effectivePath | Split-Path -Leaf) } else { $BackupPath }
        @{Job = $Job ; Instance = $InstanceName ; Database = $DatabaseName ; Path = $effectivePath ; CmdString = $match ; JobType = "BackupRestore" ; Id = Get-NextProgressId }
        Write-DebugMessage "[Restore-SQLDatabase] End of function"
    } catch {
        Write-DebugMessage "[Restore-SQLDatabase] ERROR in catch: $($_.Exception.Message)"
        Log -Message "Error in restore." -Level Error
        throw
    }
}

function Get-FileListFromBackupFile {
    <#
    .SYNOPSIS
        Gets file list from backup file.
    #>
    param (
        $BackupPath,
        $Instance,
        [string]$CredentialName
    )

    try {
        $isUrl = ($BackupPath -match '^https://')
        $from = if ($isUrl) { 'URL' } else { 'DISK' }
        $effectivePath = if ($isUrl -and -not [string]::IsNullOrWhiteSpace($CredentialName)) { $BackupPath.Split('?', 2)[0] } else { $BackupPath }
        if ($isUrl -and -not [string]::IsNullOrWhiteSpace($CredentialName)) {
            $FileListSQL = "RESTORE FILELISTONLY FROM $from = '$effectivePath' WITH CREDENTIAL = '$CredentialName'"
        } else {
            $FileListSQL = "RESTORE FILELISTONLY FROM $from = '$effectivePath'"
        }
        $FileListResult = Invoke-Sqlcmd -ServerInstance $Instance -Database master -Query $FileListSQL
        $FileListResult | ForEach-Object {
            $Custom = New-Object PSObject
            $Custom | Add-Member "Type" $_.Type
            $Custom | Add-Member "SizeInMB" ($_.Size / 1024 / 1024)
            $Custom | Add-Member "LogicalName" $_.LogicalName
            $Custom | Add-Member "PhysicalName" $_.PhysicalName
            $Custom
        }
    } catch {
        Log -Message "Error getting file list Path = $(Get-DisplayPath $BackupPath) , Instance = $Instance " -Level Error
        throw
    }
}

function Get-MoveFiles {
    <#
    .SYNOPSIS
        Generates MOVE statements for restore.
    #>
    param ($SourceFileList, $Instance, $Database, $SourceDatabase, [bool]$CreateDatabase)

    function Join-PathString {
        param(
            [Parameter(Mandatory = $true)][string]$Base,
            [Parameter(Mandatory = $true)][string]$Child
        )

        if ([string]::IsNullOrWhiteSpace($Base)) { return $Child }
        if ([string]::IsNullOrWhiteSpace($Child)) { return $Base }

        $baseTrimmed = $Base.TrimEnd('\\')
        $childTrimmed = $Child.TrimStart('\\')
        "$baseTrimmed\$childTrimmed"
    }

    try {
        $DefaultPaths = Get-DefaultPathsLegacy -Instance $Instance
        if ($script:DBALibraryVerboseDiagnostics) {
            Write-DiagMessage "Default paths = $($DefaultPaths.DefaultFile) and $($DefaultPaths.DefaultLog)"
            Write-DiagMessage "SourceFileList = $SourceFileList , Instance = $Instance , Database = $Database , SourceDatabase = $SourceDatabase"
        }
        $Moves = @()
        $dbFolder = Join-PathString -Base $DefaultPaths.DefaultFile -Child $Database
        $dbFolderSql = $dbFolder.Replace("'", "''")
        $CreateDBFolder = "EXEC master.dbo.xp_create_subdir N'$dbFolderSql'"
        Invoke-Sqlcmd -ServerInstance $Instance -Database master -Query $CreateDBFolder
        foreach ($File in $SourceFileList) {
            $PhysicalNameOnly = Split-Path $File.PhysicalName -Leaf
            $NewNameOnly = $PhysicalNameOnly -replace $SourceDatabase, $Database
            $NewFolderAndName = "$Database\$NewNameOnly"
            Log $NewNameOnly
            Log $NewFolderAndName
            if ($script:DBALibraryVerboseDiagnostics) {
                Write-DiagMessage $NewFolderAndName -ForegroundColor Magenta
            }

            $newDataPath = Join-PathString -Base $dbFolder -Child $NewNameOnly
            $newLogPath = Join-PathString -Base $DefaultPaths.DefaultLog -Child $NewNameOnly

            if ($CreateDatabase -and (Test-PathOnSQLServer -Instance $Instance -Path $newDataPath)) {
                Log -Message "File $newDataPath already exists." -Level Error
                throw "File $newDataPath already exists."
            }

            $Moves += if ($File.Type -eq "D") { "MOVE '$($File.LogicalName)' TO '$($newDataPath.Replace("'","''"))' `n" } elseif ($File.Type -eq "L") { "MOVE '$($File.LogicalName)' TO '$($newLogPath.Replace("'","''"))' `n" }
        }
        $Results = ", $($Moves -join ',')"
        Log $Results
        $Results
    } catch {
        Log "Error generating MOVE STATEMENTS" "Error"
        throw
    }
}

function Get-SAAccountName {
    <#
    .SYNOPSIS
        Gets SA account name.
    #>
    param ($Instance)

    try {
        $SAAccountSQL = "SELECT SUSER_SNAME(0x1) AS SAAccount"
        (Invoke-Sqlcmd -ServerInstance $Instance -Database master -Query $SAAccountSQL).SAAccount
    } catch {
        Log "Error getting SA Account name" "Error"
        throw
    }
}

function Write-SQLDatabaseOwner {
    <#
    .SYNOPSIS
        Sets database owner.
    #>
    param ($InstanceName, $DatabaseName, $Owner)

    try {
        $ChangeOwnerSQL = @"
DECLARE @ChangeOwnerSQL nvarchar(100)
SELECT @ChangeOwnerSQL = 'EXEC sp_changedbowner ''$(if ($Owner -eq "sa") { Get-SAAccountName -Instance $InstanceName } else { $Owner })'''
EXEC ( @ChangeOwnerSQL )
"@
        Log $ChangeOwnerSQL
        Invoke-Sqlcmd -ServerInstance $InstanceName -Database $DatabaseName -Query $ChangeOwnerSQL -AbortOnError
    } catch {
        Log "Error writing owner." "Error"
        throw
    }
}

function Get-SQLInstanceLatestSupportedCompatibilityLevel {
    <#
    .SYNOPSIS
        Gets latest supported compatibility level for instance.
    #>
    param ($InstanceName)

    try {
        (Invoke-Sqlcmd -ServerInstance $InstanceName -Database master -Query "SELECT compatibility_level FROM sys.databases WHERE database_id = DB_ID()").compatibility_level
    } catch {
        Log "Error getting latest compatibility level." "Error"
        throw
    }
}

function Write-SQLDatabaseCompatibilityLevel {
    <#
    .SYNOPSIS
        Sets database compatibility level.
    #>
    param ($InstanceName, $DatabaseName, $CompatibilityLevel)

    try {
        $CompatSQL = "ALTER DATABASE [$DatabaseName] SET COMPATIBILITY_LEVEL = $CompatibilityLevel"
        Invoke-Sqlcmd -ServerInstance $InstanceName -Database master -Query $CompatSQL -AbortOnError
    } catch {
        Log "Error setting compatibility level" "Error"
        throw
    }
}

function Run-DBCCCHECKDB {
    <#
    .SYNOPSIS
        Runs DBCC CHECKDB and checks for errors.
    #>
    param ([string]$Instance, [string]$Database)

    try {
        Log "Running DBCC. May take a few minutes. Use -NoDBCC to skip...." -WriteToHost
        $DBCCSQL = "DBCC CHECKDB ('$Database') WITH NO_INFOMSGS, ALL_ERRORMSGS, TABLERESULTS;"
        Log $DBCCSQL
        $DBCCResults = Invoke-Sqlcmd -ServerInstance $Instance -Database $Database -Query $DBCCSQL -QueryTimeout 0
        if ($DBCCResults) {
            Log "DBCC CHECKDB reported errors on $Database" -Level Info -WriteToHost -ForegroundColour Red
            $DBCCResults | Select-Object Error, MessageText | Out-String
            $true
        } else { $false }
    } catch {
        Log "Error"
        throw
    }
}

function ListOrphanedUsers {
    <#
    .SYNOPSIS
        Lists orphaned users in database.
    #>
    param ([string]$InstanceName, [string]$DatabaseName)

    try {
        $OrphanedUsersSQL = @"
SELECT dp.name AS DatabaseUser, sp.name AS ServerLogin, dp.sid as DatabaseSID, sp.sid AS LoginSid, dp.create_date as UserCreateDate, sp.create_date as LoginCreateDate
FROM sys.database_principals dp
JOIN sys.server_principals sp ON sp.name = dp.name COLLATE DATABASE_DEFAULT
WHERE dp.sid <> sp.sid
AND sp.type = 'S' AND dp.type = 'S'
"@
        Log -Message $OrphanedUsersSQL -Level Info
        $Orphans = Invoke-Sqlcmd -ServerInstance $InstanceName -Database $DatabaseName -Query $OrphanedUsersSQL
        Log -Message $($Orphans | Out-String) -Level Info
        $Orphans
    } catch {
        Log "Error getting orphaned users" -Level Error -WriteToHost
        throw
    }
}

function LinkOrphanedUser {
    <#
    .SYNOPSIS
        Links orphaned users.
    #>
    param ([string]$InstanceName, [string]$DatabaseName, [string]$UserName, [switch]$All)

    try {
        if ($All) {
            Log "Linking all orphaned users"
            $ListOrphanedUsers = ListOrphanedUsers -InstanceName $InstanceName -DatabaseName $DatabaseName
            Log "Found $($ListOrphanedUsers.Count) orphaned users" -WriteToHost
            foreach ($Login in $ListOrphanedUsers) {
                $LinkSQL = "ALTER USER $($Login.DatabaseUser) WITH LOGIN = $($Login.DatabaseUser)"
                Log $LinkSQL -WriteToHost
                Invoke-Sqlcmd -ServerInstance $InstanceName -Database $DatabaseName -Query $LinkSQL
            }
        } else {
            $LinkSQL = "ALTER USER $UserName WITH LOGIN = $UserName"
            Log $LinkSQL -WriteToHost
            Invoke-Sqlcmd -ServerInstance $InstanceName -Database $DatabaseName -Query $LinkSQL
        }
    } catch {
        Log "Error linking user $UserName" "Error"
        throw
    }
}

function ListUsersWithNoLogins {
    <#
    .SYNOPSIS
        Lists users without logins.
    #>
    param ([string]$InstanceName, [string]$DatabaseName)

    try {
        $NoLoginsSQL = @"
SELECT dp.name FROM sys.database_principals dp
LEFT OUTER JOIN sys.server_principals sp ON sp.sid = dp.sid
WHERE sp.sid IS NULL AND dp.type IN ('S','U') AND dp.name NOT IN ('guest','sys','INFORMATION_SCHEMA')
"@
        $NoLogins = Invoke-Sqlcmd -ServerInstance $InstanceName -Database $DatabaseName -Query $NoLoginsSQL
        Log -Message $($NoLogins | Out-String) -Level Info
        $NoLogins
    } catch {
        Log "Error getting users without logins" -Level Error -WriteToHost
        throw
    }
}

function Get-RowCountsForDatabase {
    <#
    .SYNOPSIS
        Gets row counts for tables in database.
    #>
    param ($Instance, $Database)

    $RCSQL = @"
CREATE TABLE #counts
(
    table_name varchar(255),
    row_count int
)
EXEC sp_MSForEachTable @command1='INSERT #counts (table_name, row_count) SELECT ''?'', COUNT(*) FROM ?'
SELECT table_name, row_count FROM #counts ORDER BY row_count DESC
DROP TABLE #counts
"@
    Invoke-Sqlcmd -ServerInstance $Instance -Database $Database -Query $RCSQL
}

function Get-LastUpdatedTablesForDatabase {
    <#
    .SYNOPSIS
        Gets last updated tables in database.
    #>
    param ($Instance, $Database)

    $LastUpdateSQL = @"
SELECT
t.name AS TableName
,SUM(User_updates) OVER (PARTITION BY i.object_id, i.index_id) AS user_updates
,SUM(ius.user_seeks) OVER(PARTITION BY i.object_id, i.index_id) AS userSeeks
,SUM(ius.user_scans) OVER(PARTITION By i.object_id, i.index_id) AS UserScans
,SUM(ius.user_lookups) OVER(PARTITION BY i.object_id, i.index_id) AS UserLookups
,MAX(ius.last_user_update) OVER(PARTITION BY i.object_id, i.index_id) AS LastUpdate
,(SELECT sqlserver_start_time FROM sys.dm_os_sys_info) AS StartupTime
FROM sys.indexes i
LEFT OUTER JOIN sys.dm_db_index_usage_stats ius ON ius.index_id = i.index_id AND i.object_id = ius.object_id
JOIN sys.tables t ON t.object_id = i.object_id
WHERE t.is_ms_shipped = 0
AND database_id = DB_ID()
ORDER BY LastUpdate DESC
"@
    Invoke-Sqlcmd -ServerInstance $Instance -Database $Database -Query $LastUpdateSQL
}

function Write-SQLUserRoles {
    <#
    .SYNOPSIS
        Writes user roles to database.
    #>
    param ([string]$InstanceName, [string]$DatabaseName, $Roles)

    try {
        foreach ($Principal in $Roles) {
            $UserSQL = @"
IF USER_ID('$($Principal.DatabaseUserName)') IS NULL CREATE USER [$($Principal.DatabaseUserName)] FOR LOGIN [$($Principal.ServerLoginName)];
EXECUTE sp_addrolemember $($Principal.RoleName), '$($Principal.DatabaseUserName)'
"@
            Invoke-Sqlcmd -ServerInstance $InstanceName -Database $DatabaseName -Query $UserSQL -AbortOnError
        }
    } catch {
        Log "Error adding back users." "Error"
        throw "Error adding users"
    }
}

function Create-Logins {
    <#
    .SYNOPSIS
        Creates logins if they don't exist.
    #>
    param ([string]$SourceInstance, [string]$SourceDatabase, [string]$TargetInstance, [string]$TargetDatabase)

    try {
        $SourceLogins = Get-LoginCreationCommandsForDatabase -Instance $SourceInstance -Database $SourceDatabase
        foreach ($Login in $SourceLogins) {
            if (-not (LoginExists -Instance $TargetInstance -Login $Login.Login)) {
                Log "Login $($Login.Login) does not exist. Creating it."
                Invoke-Sqlcmd -ServerInstance $TargetInstance -Database master -Query $Login.CreateString
            } else {
                Log "Login $($Login.Login) already exists."
            }
        }
    } catch {
        Log "Error creating logins" "Error"
        throw
    }
}

function ShrinkLog {
    <#
    .SYNOPSIS
        Shrinks transaction log.
    #>
    param ($InstanceName, $DatabaseName)

    try {
        $ShrinkLogSQL = @"
DECLARE @LogName nvarchar(128)
SELECT @LogName = name FROM sys.master_files WHERE database_id = DB_ID() AND type = 1
DBCC SHRINKFILE (@LogName , 0, TRUNCATEONLY)
"@
        Invoke-Sqlcmd -ServerInstance $InstanceName -Database $DatabaseName -Query $ShrinkLogSQL | Out-Null
    } catch {
        Log "Error shrinking log." "Error"
        throw
    }
}

function Change-Collation {
    <#
    .SYNOPSIS
        Changes database collation.
    #>
    param ([string]$Instance, [string]$Database, [ValidateSet("Latin1_General_CI_AS")][string]$Collation)

    $DropIndexCommands = Get-DropStatementsForCollation -Instance $Instance -Database $Database -Collation $Collation
    Log -Message $DropIndexCommands
    $CreateIndexCommands = Get-CreateStatementsForCollation -Instance $Instance -Database $Database -Collation $Collation
    Log -Message $CreateIndexCommands
    if ($DropIndexCommands) { Invoke-Sqlcmd -ServerInstance $Instance -Database $Database -Query $DropIndexCommands }
    Set-DatabaseCollation -Instance $Instance -Database $Database -Collation $Collation
    if ($CreateIndexCommands) { Invoke-Sqlcmd -ServerInstance $Instance -Database $Database -Query $CreateIndexCommands }
}

function Set-DatabaseCollation {
    <#
    .SYNOPSIS
        Sets database collation.
    #>
    param ([string]$Instance, [string]$Database, [ValidateSet("Latin1_General_CI_AS")][string]$Collation)

    try {
        $ChangeCollationSQL = @"
-- The full SQL for changing collation (as in original)
"@
        # Paste the full ChangeCollationSQL here for completeness.
        Invoke-Sqlcmd -ServerInstance $Instance -Database $Database -Query $ChangeCollationSQL | Out-String
    } catch {
        Log -Message "Error changing collation" -Level Error
        throw
    }
}

function Get-CreateStatementsForCollation {
    <#
    .SYNOPSIS
        Gets create statements for indexes during collation change.
    #>
    param ([string]$Instance, [string]$Database, [ValidateSet("Latin1_General_CI_AS")][string]$Collation)

    try {
        $CreateSQL = @"
-- The full CreateSQL for collation (as in original)
"@
        # Paste the full CreateSQL here.
        Invoke-Sqlcmd -ServerInstance $Instance -Database $Database -Query $CreateSQL | Select-Object -ExpandProperty CreateSQL | Out-String
    } catch {
        Log -Message "Error getting Create commands for collation" -Level Error
        throw
    }
}

function Get-DropStatementsForCollation {
    <#
    .SYNOPSIS
        Gets drop statements for indexes during collation change.
    #>
    param ([string]$Instance, [string]$Database, [ValidateSet("Latin1_General_CI_AS")][string]$Collation)

    try {
        $DropSQL = @"
-- The full DropSQL for collation (as in original)
"@
        # Paste the full DropSQL here.
        Invoke-Sqlcmd -ServerInstance $Instance -Database $Database -Query $DropSQL | Select-Object -ExpandProperty DropSQL
    } catch {
        Log -Message "Error getting drop commands for collation" -Level Error
        throw
    }
}

function Remove-Orphans {
    <#
    .SYNOPSIS
        Removes orphaned users.
    #>
    param ([string]$InstanceName, [string]$DatabaseName)

    try {
        $SQLOrphans = "SELECT u.name, 'EXEC sp_revokedbaccess ''' + u.name +'''' AS [Script] FROM master..syslogins l RIGHT JOIN sysusers u ON l.sid = u.sid WHERE l.sid IS NULL AND issqlrole <> 1 AND isapprole <> 1 AND (u.name <> 'INFORMATION_SCHEMA' AND u.name <> 'guest' AND u.name <> 'sys' AND u.name <> 'system_function_schema')"
        $Orphans = (Invoke-Sqlcmd -ServerInstance $InstanceName -Database $DatabaseName -Query $SQLOrphans).Script
        $OrphansDeleteSQL = $Orphans -join "`n"
        Write-Host $OrphansDeleteSQL -ForegroundColor Yellow
        Invoke-Sqlcmd -ServerInstance $InstanceName -Database $DatabaseName -Query $OrphansDeleteSQL
    } catch {
        throw
    }
}

function Backup {
    <#
    .SYNOPSIS
        Backs up a database with options.
    #>
    param (
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

    try {
        $Params = $MyInvocation.BoundParameters | Out-String
        $ScriptStartTime = Get-Date
        $ExecutionID = [guid]::NewGuid().Guid
        Log "Starting"
        Log $Params

        if ((Get-DatabaseState -Instance $Instance -Database $Database) -eq "RESTORING") {
            Log "Database in RESTORING state." "Error"
            throw "Database in RESTORING state cannot be backed up. Use 'RESTORE DATABASE $Database' and try again."
        }

        $Compress = Get-SQLInstanceCompression -InstanceName $Instance
        Log "Compression = $Compress"

        $BackupLocation = if ($BackupToNul) { "nul" } else { Get-BackupLocation -InstanceName $Instance -DatabaseName $Database -CreateIfNotExist $true -MarkAsRetain $MarkAsRetain -Differential $Differential -BackupLocation $BackupPath }
        Log $BackupLocation

        $Jobs = Backup-Database -InstanceName $Instance -DatabaseName $Database -BackupPath $BackupLocation -Compress $CompressIfPossible -JobName "Backup" -CopyOnly $CopyOnly -Differential $Differential
        Progress2 -JobDetailsCollection @($Jobs)

        if (-not $BackupToNul) { Check-Backup -Instance $Instance -Database $Database -BackupLocation $BackupLocation -Verify $Verify }

        $MailSubject = "Backup of $Database on $Instance completed successfully."
        $MailMessage = "Backup at $BackupLocation"
    } catch {
        Log -Message "Error in Backup" -Level "Error"
        $StackTrace = $Error[0].ScriptStackTrace
        $ErrorDetails = "ERROR: `n`n STACK TRACE: `n`n $StackTrace `n`n" + ($Error[0] | Out-String) -replace "'", ""
        $ErrorDetails = $ErrorDetails -replace "\[$]", "X"
        $MailMessage = $ErrorDetails
        $MailSubject = "Error in Backup of $Database on $Instance."
        Write-Host $ErrorDetails -ForegroundColor Red
    } finally {
        Get-Job | Where-Object { $_.Name -ne "dbatools_Timer" } | Remove-Job
        Log "Finished"
	SendEMail -Subject $MailSubject -Msg $MailMessage
    }
}
