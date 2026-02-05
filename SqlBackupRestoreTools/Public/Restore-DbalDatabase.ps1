function Restore-DbalDatabase {
    <#
    .SYNOPSIS
        Restores a SQL Server database from a backup (standalone).

    .DESCRIPTION
        Restores a database from a backup file path (filesystem/UNC) or Azure URL.

        This cmdlet reuses the module's existing RESTORE implementation and adds the most common
        post-restore options: compatibility level, owner, preserve users/security, role memberships,
        DBCC, stats update, orphan cleanup, and optional post-restore script.

        Notes:
        - Some options (like -PreserveTargetSecurity and -CopyUserRoles) are only meaningful when overwriting an existing database.
        - If restoring to URL (Azure), you can supply either a SAS token in the URL or a SQL credential name via -CredentialName.

    .PARAMETER Instance
        Target SQL Server instance name (e.g. 'SERVER\INSTANCE').

    .PARAMETER Database
        Target database name.

    .PARAMETER BackupPath
        Backup file path or Azure URL to restore from.

    .PARAMETER CreateDatabase
        When $true (default), restores into a new database name and fails if it already exists.
        When $false, restores over an existing database name (destructive).

    .PARAMETER TakeInstanceOffline
        When $true, attempts to take the target database offline before restore (overwrite scenarios).

    .PARAMETER TakeInstanceOfflineMode
        Controls how the target database is taken offline (RollbackImmediate, NoWait, Wait).

    .PARAMETER AbortIfActiveSessions
        When $true and taking the target offline, aborts if active sessions are detected.

    .PARAMETER NoRecovery
        When $true, restores WITH NORECOVERY, leaving the target in RESTORING state.

    .PARAMETER Differential
        When $true, performs a differential restore workflow.

    .PARAMETER BackupSourceDatabase
        Name of the source database inside the backup.
        Used to generate nicer restored file names in WITH MOVE mapping when creating a new database.
        If omitted, defaults to the target -Database.

    .PARAMETER VerifyBackup
        Runs RESTORE VERIFYONLY against the backup before restoring.

    .PARAMETER CredentialName
        SQL credential name to use for Azure URL restores when the URL does not contain a SAS token.

    .PARAMETER CompatabilityLevel
        Compatibility level to apply after restore.
        - SetLatest: set to the instance maximum.
        - Retain: keep current (no change).
        - 100..170: explicit level.

    .PARAMETER Owner
        Database owner to set after restore (default: sa).

    .PARAMETER PreserveTargetSecurity
        When specified (overwrite scenarios), captures target users/permissions/role memberships before restore and reapplies after restore.

    .PARAMETER CopyUserRoles
        When $true (overwrite scenarios), captures role memberships before restore and reapplies after restore.
        If -PreserveTargetSecurity is set, role memberships are included and CopyUserRoles is skipped.

    .PARAMETER ShrinkLog
        When $true, attempts to shrink the target database log file after restore.

    .PARAMETER ChangeCollation
        When specified, changes the database collation after restore. Requires -Collation.

    .PARAMETER Collation
        Collation name to apply when using -ChangeCollation.

    .PARAMETER NoDBCC
        When $true, skips DBCC CHECKDB after restore.

    .PARAMETER UpdateStats
        When $true, runs sp_updatestats after restore.

    .PARAMETER DeleteOrphans
        When $true, removes orphaned users after restore.

    .PARAMETER ScriptToRunOnTarget
        Optional path to a .sql script file to execute against the target database after restore.

    .PARAMETER DryRun
        Generates and returns the RESTORE SQL without executing it.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Instance,
        [Parameter(Mandatory)][string]$Database,
        [Parameter(Mandatory)][string]$BackupPath,

        [Parameter()][bool]$CreateDatabase = $true,
        [Parameter()][bool]$TakeInstanceOffline = $false,
        [Parameter()][ValidateSet('RollbackImmediate', 'NoWait', 'Wait')][string]$TakeInstanceOfflineMode = 'RollbackImmediate',
        [Parameter()][bool]$AbortIfActiveSessions = $false,

        [Parameter()][bool]$NoRecovery = $false,
        [Parameter()][bool]$Differential = $false,

        [Parameter()][string]$BackupSourceDatabase,
        [Parameter()][switch]$VerifyBackup,
        [Parameter()][string]$CredentialName,

        [Parameter()][ValidateSet('SetLatest','Retain','100','110','120','130','140','150','160','170')][string]$CompatabilityLevel = 'SetLatest',
        [Parameter()][string]$Owner = 'sa',
        [Parameter()][switch]$PreserveTargetSecurity,
        [Parameter()][bool]$CopyUserRoles = $false,
        [Parameter()][bool]$ShrinkLog = $false,
        [Parameter()][switch]$ChangeCollation,
        [Parameter()][ValidateSet('Latin1_General_CI_AS')][string]$Collation,
        [Parameter()][bool]$NoDBCC = $false,
        [Parameter()][bool]$UpdateStats = $false,
        [Parameter()][bool]$DeleteOrphans = $false,
        [Parameter()][string]$ScriptToRunOnTarget,

        [Parameter()][switch]$DryRun,
        [Parameter()][switch]$VerboseDiagnostics
    )

    $script:ExecutionID = [guid]::NewGuid().Guid
    $script:DBALibraryVerboseDiagnostics = $VerboseDiagnostics.IsPresent

    $topProgressId = 0
    try { Write-Progress -Id $topProgressId -Activity "Restore $Database on $Instance" -Status 'Initializing...' -PercentComplete 0 } catch {}

    if ($ChangeCollation.IsPresent -and [string]::IsNullOrWhiteSpace($Collation)) {
        throw "-ChangeCollation requires -Collation."
    }

    if ($AbortIfActiveSessions -and $TakeInstanceOffline -and -not $CreateDatabase -and -not $Differential) {
        try {
            if (Test-DbalDatabaseHasActiveUserSessions -Instance $Instance -Database $Database) {
                throw "Active user sessions detected on [$Database] on [$Instance]; aborting due to -AbortIfActiveSessions."
            }
        } catch {
            $msg = "Failed checking active sessions on [$Database] on [$Instance]. $($_.Exception.Message)"
            throw (New-Object System.Exception($msg, $_.Exception))
        }
    }

    if ([string]::IsNullOrWhiteSpace($BackupSourceDatabase)) {
        $BackupSourceDatabase = $Database
    }

    if ($ScriptToRunOnTarget) {
        if (-not (Test-Path $ScriptToRunOnTarget)) {
            throw "-ScriptToRunOnTarget not accessible: $ScriptToRunOnTarget"
        }
    }

    $originalTargetRoles = $null
    if ($CopyUserRoles -and -not $CreateDatabase -and -not $Differential) {
        try {
            $originalTargetRoles = Get-SQLUserRoles -InstanceName $Instance -DatabaseName $Database
        } catch {
            $msg = "Failed capturing existing role memberships on [$Database] on [$Instance]. $($_.Exception.Message)"
            throw (New-Object System.Exception($msg, $_.Exception))
        }
    }

    $targetSecuritySnapshot = $null
    if ($PreserveTargetSecurity.IsPresent -and -not $CreateDatabase -and -not $Differential) {
        Log -Message "PreserveTargetSecurity specified: capturing target database security snapshot" -Level Info -WriteToHost
        try {
            $targetSecuritySnapshot = Get-DbalDatabaseSecuritySnapshot -Instance $Instance -Database $Database
        } catch {
            $msg = "Failed capturing security snapshot on [$Database] on [$Instance]. $($_.Exception.Message)"
            throw (New-Object System.Exception($msg, $_.Exception))
        }
        if ($targetSecuritySnapshot -and $targetSecuritySnapshot.Warnings -and $targetSecuritySnapshot.Warnings.Count -gt 0) {
            foreach ($w in $targetSecuritySnapshot.Warnings) {
                Log -Message $w -Level Warning -WriteToHost -ForegroundColour Yellow
            }
        }
    }

    if ($VerifyBackup.IsPresent) {
        try {
            $null = Invoke-DbalVerifyBackup -Instance $Instance -BackupPath $BackupPath -CredentialName $CredentialName -DryRun:$DryRun
        } catch {
            $msg = "Backup verification failed (RESTORE VERIFYONLY) on [$Instance] for path $(Get-DisplayPath $BackupPath). $($_.Exception.Message)"
            throw (New-Object System.Exception($msg, $_.Exception))
        }
    }

    $restoreParams = @{
        InstanceName            = $Instance
        DatabaseName            = $Database
        TakeInstanceOffline     = $TakeInstanceOffline
        TakeInstanceOfflineMode = $TakeInstanceOfflineMode
        BackupPath              = $BackupPath
        JobName                 = 'RestoreDatabase'
        NoRecovery              = $NoRecovery
        CreateDatabase          = $CreateDatabase
        SourceDatabase          = $BackupSourceDatabase
        Differential            = $Differential
        CredentialName          = $CredentialName
    }

    try {
        try { Write-Progress -Id $topProgressId -Activity "Restore $Database on $Instance" -Status 'Submitting restore...' -PercentComplete 3 } catch {}
        $restoreJob = Restore-SQLDatabase @restoreParams -DryRun:$DryRun
    } catch {
        $msg = "Failed starting restore for [$Database] on [$Instance]. $($_.Exception.Message)"
        throw (New-Object System.Exception($msg, $_.Exception))
    }

    if ($DryRun.IsPresent) {
        try { Write-Progress -Id $topProgressId -Activity "Restore $Database on $Instance" -Completed } catch {}
        return [pscustomobject]@{
            Instance   = $Instance
            Database   = $Database
            BackupPath = $BackupPath
            Sql        = $restoreJob.CmdString
        }
    }

    try {
        try { Write-Progress -Id $topProgressId -Activity "Restore $Database on $Instance" -Status 'Running...' -PercentComplete 5 } catch {}
        Progress2 -JobDetailsCollection @($restoreJob)
    } catch {
        $msg = "Restore failed for [$Database] on [$Instance]. $($_.Exception.Message)"
        throw (New-Object System.Exception($msg, $_.Exception))
    } finally {
        try { Write-Progress -Id $topProgressId -Activity "Restore $Database on $Instance" -Completed } catch {}
    }

    if (-not $NoRecovery) {
        if ($ChangeCollation) {
            Change-Collation -Instance $Instance -Database $Database -Collation $Collation
        }

        if ($PreserveTargetSecurity.IsPresent -and $null -ne $targetSecuritySnapshot) {
            Invoke-DbalDatabaseSecuritySnapshot -Instance $Instance -Database $Database -Snapshot $targetSecuritySnapshot
        }

        if ($ShrinkLog) {
            ShrinkLog -InstanceName $Instance -DatabaseName $Database
        }

        if ($CopyUserRoles) {
            if ($PreserveTargetSecurity.IsPresent) {
                Log "CopyUserRoles specified, but PreserveTargetSecurity is also set; skipping CopyUserRoles because role memberships are included in preserved security." -Level Warning -WriteToHost -ForegroundColour Yellow
            } elseif ($null -ne $originalTargetRoles) {
                Write-SQLUserRoles -InstanceName $Instance -DatabaseName $Database -Roles $originalTargetRoles
            }
        }

        if (-not [string]::IsNullOrWhiteSpace($Owner)) {
            Write-SQLDatabaseOwner -InstanceName $Instance -DatabaseName $Database -Owner $Owner
        }

        if ($CompatabilityLevel -eq 'SetLatest') {
            $targetLatestCompat = Get-SQLInstanceLatestSupportedCompatibilityLevel -InstanceName $Instance
            Write-SQLDatabaseCompatibilityLevel -InstanceName $Instance -DatabaseName $Database -CompatibilityLevel $targetLatestCompat
        } elseif ($CompatabilityLevel -ne 'Retain') {
            Write-SQLDatabaseCompatibilityLevel -InstanceName $Instance -DatabaseName $Database -CompatibilityLevel $CompatabilityLevel
        }

        if (-not $NoDBCC) {
            if (Run-DBCCCHECKDB -Instance $Instance -Database $Database) {
                throw "DBCC CHECKDB detected errors."
            }
        }

        if ($UpdateStats) {
            Invoke-Sqlcmd -ServerInstance $Instance -Database $Database -Query "EXEC sp_updatestats"
        }

        Invoke-Sqlcmd -ServerInstance $Instance -Database $Database -Query "ALTER DATABASE [$Database] SET PAGE_VERIFY CHECKSUM"

        $orph = ListOrphanedUsers -InstanceName $Instance -DatabaseName $Database
        if ($null -eq $orph) {
            Log -Message "No orphaned users on $Database on $Instance" -Level Info
        } else {
            Log -Message "Orphaned users found on $Database on $Instance :`n$($orph | Format-Table | Out-String)" -Level Warning -WriteToHost -ForegroundColour Yellow
        }

        $usersWithNoLogins = ListUsersWithNoLogins -InstanceName $Instance -DatabaseName $Database
        if ($null -eq $usersWithNoLogins) {
            Log -Message "No users without logins on $Database on $Instance" -Level Info
        } else {
            Log -Message "Users without logins found on $Database on $Instance :`n$($usersWithNoLogins | Format-Table | Out-String)" -Level Warning -WriteToHost -ForegroundColour Yellow
        }

        if ($DeleteOrphans) {
            Remove-Orphans -InstanceName $Instance -DatabaseName $Database
        }

        if ($ScriptToRunOnTarget) {
            Invoke-Sqlcmd -ServerInstance $Instance -Database $Database -InputFile $ScriptToRunOnTarget
        }
    }

    return [pscustomobject]@{
        Instance   = $Instance
        Database   = $Database
        BackupPath = $BackupPath
        NoRecovery = $NoRecovery
    }
}
