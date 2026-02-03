Set-StrictMode -Version Latest

function Get-SbrtConfigPath {
    [CmdletBinding()]
    param()

    if ($IsWindows -and $env:APPDATA) {
        return (Join-Path $env:APPDATA 'SqlBackupRestoreTools\config.json')
    }

    $home = if ($env:HOME) { $env:HOME } else { $HOME }
    return (Join-Path $home '.config/SqlBackupRestoreTools/config.json')
}

function Get-SbrtPersistedConfig {
    [CmdletBinding()]
    param()

    $path = Get-SbrtConfigPath
    if (-not (Test-Path -LiteralPath $path)) {
        return $null
    }

    try {
        $json = Get-Content -LiteralPath $path -Raw -ErrorAction Stop
        if ([string]::IsNullOrWhiteSpace($json)) { return $null }
        return ($json | ConvertFrom-Json -ErrorAction Stop)
    } catch {
        return $null
    }
}

function Save-SbrtPersistedConfig {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [pscustomobject]$Config
    )

    $path = Get-SbrtConfigPath
    $dir = Split-Path -Parent $path
    if (-not (Test-Path -LiteralPath $dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
    }

    $Config | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $path -Encoding UTF8
    return $path
}

function Remove-SbrtPersistedConfig {
    [CmdletBinding(SupportsShouldProcess)]
    param()

    $path = Get-SbrtConfigPath
    if (-not (Test-Path -LiteralPath $path)) {
        return $false
    }

    if ($PSCmdlet.ShouldProcess($path, 'Remove persisted config')) {
        Remove-Item -LiteralPath $path -Force
        return $true
    }

    return $false
}

function Initialize-SbrtConfigFromDisk {
    [CmdletBinding()]
    param()

    # Opt-out: set env var to any non-empty value.
    if (-not [string]::IsNullOrWhiteSpace($env:SQLBACKUPRESTORETOOLS_DISABLE_PERSISTED_CONFIG)) {
        return
    }

    $persisted = Get-SbrtPersistedConfig
    if ($null -eq $persisted) {
        return
    }

    # Apply persisted values into module scope.
    if ($null -ne $persisted.DBAInstance) { Set-Variable -Scope Script -Name DBAInstance -Value ([string]$persisted.DBAInstance) }
    if ($null -ne $persisted.DBADatabase) { Set-Variable -Scope Script -Name DBADatabase -Value ([string]$persisted.DBADatabase) }
    if ($null -ne $persisted.SmtpServer) { Set-Variable -Scope Script -Name smtpserver -Value ([string]$persisted.SmtpServer) }
    if ($null -ne $persisted.SMTPEnabled) { Set-Variable -Scope Script -Name SMTPEnabled -Value ([bool]$persisted.SMTPEnabled) }

    if ($null -ne $persisted.DefaultBackupPath) { Set-Variable -Scope Script -Name DefaultBackupPath -Value ([string]$persisted.DefaultBackupPath) }
    if ($null -ne $persisted.DefaultAzureStorageBackupLocation) { Set-Variable -Scope Script -Name DefaultAzureStorageBackupLocation -Value ([string]$persisted.DefaultAzureStorageBackupLocation) }
}
