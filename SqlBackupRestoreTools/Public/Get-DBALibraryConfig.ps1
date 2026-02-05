function Get-DBALibraryConfig {
    <#
    .SYNOPSIS
        Gets the current SqlBackupRestoreTools session configuration.

    .DESCRIPTION
        Returns the current in-session configuration values used by BackupAndRestore for optional
        database-backed logging and email delivery.

        This module retains the legacy DBALibrary config cmdlet names for backward compatibility.

    .OUTPUTS
        System.Management.Automation.PSCustomObject

        The returned object includes:
        - ConfigSource: Session or Persisted
        - ConfigPath: the per-user JSON config file location

    .EXAMPLE
        Get-DBALibraryConfig

        Shows the current DBA instance/database and SMTP settings.

    .EXAMPLE
        Get-DBALibraryConfig -Persisted

        Shows the per-user persisted configuration (if present).
    #>
    [CmdletBinding()]
    param(
        [switch]$Persisted
    )

    if ($Persisted.IsPresent) {
        $p = Get-SbrtPersistedConfig
        $path = Get-SbrtConfigPath
        if ($null -eq $p) {
            return [pscustomobject]@{
                ConfigSource = 'Persisted'
                ConfigPath   = $path
                Exists       = $false
            }
        }

        return [pscustomobject]@{
            ConfigSource = 'Persisted'
            ConfigPath   = $path
            Exists       = $true
            DBAInstance  = $p.DBAInstance
            DBADatabase  = $p.DBADatabase
            SmtpServer   = $p.SmtpServer
            SMTPEnabled  = [bool]$p.SMTPEnabled
            BackupPathDefault = $p.DefaultBackupPath
            AzureStorageBackupLocationDefault = $p.DefaultAzureStorageBackupLocation
            BlockSizeDefault = $p.DefaultBlockSize
            BufferCountDefault = $p.DefaultBufferCount
            MaxTransferSizeDefault = $p.DefaultMaxTransferSize
        }
    }

    [pscustomobject]@{
        ConfigSource = 'Session'
        ConfigPath   = (Get-SbrtConfigPath)
        DBAInstance  = (Get-Variable -Name DBAInstance -Scope Script -ValueOnly -ErrorAction SilentlyContinue)
        DBADatabase  = (Get-Variable -Name DBADatabase -Scope Script -ValueOnly -ErrorAction SilentlyContinue)
        SmtpServer   = (Get-Variable -Name smtpserver -Scope Script -ValueOnly -ErrorAction SilentlyContinue)
        SMTPEnabled  = [bool](Get-Variable -Name SMTPEnabled -Scope Script -ValueOnly -ErrorAction SilentlyContinue)
        BackupPathDefault = (Get-Variable -Name DefaultBackupPath -Scope Script -ValueOnly -ErrorAction SilentlyContinue)
        AzureStorageBackupLocationDefault = (Get-Variable -Name DefaultAzureStorageBackupLocation -Scope Script -ValueOnly -ErrorAction SilentlyContinue)
        BlockSizeDefault = (Get-Variable -Name DefaultBlockSize -Scope Script -ValueOnly -ErrorAction SilentlyContinue)
        BufferCountDefault = (Get-Variable -Name DefaultBufferCount -Scope Script -ValueOnly -ErrorAction SilentlyContinue)
        MaxTransferSizeDefault = (Get-Variable -Name DefaultMaxTransferSize -Scope Script -ValueOnly -ErrorAction SilentlyContinue)
    }
}
