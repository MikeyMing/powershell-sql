function Set-DBALibraryConfig {
    <#
    .SYNOPSIS
        Sets SqlBackupRestoreTools session configuration.

    .DESCRIPTION
        Sets session-scoped configuration values used by BackupAndRestore.

        This module retains the legacy DBALibrary config cmdlet names for backward compatibility.
        Settings are not persisted across PowerShell sessions.

    .PARAMETER DBAInstance
        SQL Server instance hosting the logging database used when -EnableDbLogging is specified.

    .PARAMETER DBADatabase
        Database name used for SQL-backed logging (writes to dbo.Log). Defaults to 'DBA'.

    .PARAMETER SmtpServer
        SMTP relay host used for email notifications.

    .PARAMETER SMTPEnabled
        Enables/disables email sending.

    .PARAMETER DefaultBackupPath
        Default filesystem/UNC directory used when BackupAndRestore is called without -BackupPath or -AzureStorageBackupLocation.

    .PARAMETER DefaultAzureStorageBackupLocation
        Default Azure Blob container URL used when BackupAndRestore is called without -BackupPath or -AzureStorageBackupLocation.

    .PARAMETER Persist
        Persists the updated configuration to a per-user config file (JSON).

        On Windows, the file is stored under:
        - %APPDATA%\SqlBackupRestoreTools\config.json

    .EXAMPLE
        Set-DBALibraryConfig -DBAInstance 'SERVER\INSTANCE' -DBADatabase 'DBA'

        Sets the target instance/database for SQL-backed logging.

    .EXAMPLE
        Set-DBALibraryConfig -SMTPEnabled $true -SmtpServer 'smtp.yourdomain.local'

        Enables email and sets the SMTP relay.

    .EXAMPLE
        Set-DBALibraryConfig -DefaultBackupPath '\\fileserver\sqlbackups'

        Sets a default backup location for filesystem/UNC backups.

    .EXAMPLE
        Set-DBALibraryConfig -DefaultBackupPath '\\fileserver\\sqlbackups' -Persist

        Sets and persists a default backup location (per-user), so it auto-loads in future sessions.
    #>
    [CmdletBinding(SupportsShouldProcess)]
    param(
        [string]$DBAInstance,
        [string]$DBADatabase,
        [string]$SmtpServer,
        [Nullable[bool]]$SMTPEnabled,
        [string]$DefaultBackupPath,
        [string]$DefaultAzureStorageBackupLocation,
        [switch]$Persist
    )

    if ($PSCmdlet.ShouldProcess('SqlBackupRestoreTools configuration', 'Update')) {
        if ($PSBoundParameters.ContainsKey('DBAInstance')) { Set-Variable -Scope Script -Name DBAInstance -Value $DBAInstance }
        if ($PSBoundParameters.ContainsKey('DBADatabase')) { Set-Variable -Scope Script -Name DBADatabase -Value $DBADatabase }
        if ($PSBoundParameters.ContainsKey('SmtpServer')) { Set-Variable -Scope Script -Name smtpserver -Value $SmtpServer }
        if ($PSBoundParameters.ContainsKey('SMTPEnabled')) { Set-Variable -Scope Script -Name SMTPEnabled -Value $SMTPEnabled }

        if ($PSBoundParameters.ContainsKey('DefaultBackupPath')) {
            Set-Variable -Scope Script -Name DefaultBackupPath -Value $DefaultBackupPath
        }
        if ($PSBoundParameters.ContainsKey('DefaultAzureStorageBackupLocation')) {
            Set-Variable -Scope Script -Name DefaultAzureStorageBackupLocation -Value $DefaultAzureStorageBackupLocation
        }

        if ($Persist.IsPresent) {
            $configToPersist = [pscustomobject]@{
                DBAInstance = (Get-Variable -Name DBAInstance -Scope Script -ValueOnly -ErrorAction SilentlyContinue)
                DBADatabase = (Get-Variable -Name DBADatabase -Scope Script -ValueOnly -ErrorAction SilentlyContinue)
                SmtpServer  = (Get-Variable -Name smtpserver -Scope Script -ValueOnly -ErrorAction SilentlyContinue)
                SMTPEnabled = [bool](Get-Variable -Name SMTPEnabled -Scope Script -ValueOnly -ErrorAction SilentlyContinue)
                DefaultBackupPath = (Get-Variable -Name DefaultBackupPath -Scope Script -ValueOnly -ErrorAction SilentlyContinue)
                DefaultAzureStorageBackupLocation = (Get-Variable -Name DefaultAzureStorageBackupLocation -Scope Script -ValueOnly -ErrorAction SilentlyContinue)
            }

            $path = Save-SbrtPersistedConfig -Config $configToPersist
            Write-Verbose "Persisted config saved to: $path"
        }
    }
}
