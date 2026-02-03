function Clear-DBALibraryConfig {
    <#
    .SYNOPSIS
        Clears SqlBackupRestoreTools configuration.

    .DESCRIPTION
        Clears persisted configuration on disk (per-user), and optionally resets the current session configuration.

        This module retains the legacy DBALibrary config cmdlet names for backward compatibility.

    .PARAMETER Persisted
        Removes the persisted per-user configuration file.

        On Windows, the file is stored under:
        - %APPDATA%\SqlBackupRestoreTools\config.json

    .PARAMETER Session
        Resets current session configuration values back to defaults.

    .EXAMPLE
        Clear-DBALibraryConfig -Persisted

        Removes the per-user persisted config file.

    .EXAMPLE
        Clear-DBALibraryConfig -Session

        Resets the current session configuration.

    .EXAMPLE
        Clear-DBALibraryConfig -Persisted -Session

        Removes the persisted config file and resets the current session configuration.
    #>
    [CmdletBinding(SupportsShouldProcess)]
    param(
        [switch]$Persisted,
        [switch]$Session
    )

    if (-not $Persisted -and -not $Session) {
        throw 'Specify -Persisted and/or -Session.'
    }

    if ($Persisted) {
        [void](Remove-SbrtPersistedConfig)
    }

    if ($Session) {
        if ($PSCmdlet.ShouldProcess('SqlBackupRestoreTools configuration', 'Reset session defaults')) {
            Set-Variable -Scope Script -Name DBAInstance -Value $null
            Set-Variable -Scope Script -Name DBADatabase -Value 'DBA'
            Set-Variable -Scope Script -Name smtpserver -Value 'smtp'
            Set-Variable -Scope Script -Name SMTPEnabled -Value $false
            Set-Variable -Scope Script -Name DefaultBackupPath -Value $null
            Set-Variable -Scope Script -Name DefaultAzureStorageBackupLocation -Value $null
        }
    }
}
