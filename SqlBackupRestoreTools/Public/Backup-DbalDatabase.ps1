function Backup-DbalDatabase {
    <#
    .SYNOPSIS
        Creates a SQL Server database backup (standalone).

    .DESCRIPTION
        Creates a FULL (default) or DIFFERENTIAL backup of a single database.

        This cmdlet reuses the module's existing BACKUP implementation and progress monitoring,
        but does not perform any restore steps.

        For filesystem/UNC backups, provide -BackupPath as a base folder (not a .bak file). The
        cmdlet will generate a timestamped backup file path underneath it.

        For Azure Blob Storage, provide -AzureStorageBackupLocation as a container URL.
        If the URL contains a SAS token, the backup will be written using the SAS.

    .PARAMETER Instance
        SQL Server instance name (e.g. 'SERVER\INSTANCE').

    .PARAMETER Database
        Database name.

    .PARAMETER BackupPath
        Base folder (filesystem or UNC share) where backups are written.

    .PARAMETER AzureStorageBackupLocation
        Azure Blob container URL (optionally with SAS token) used as the backup destination.

    .PARAMETER Differential
        When $true, creates a DIFFERENTIAL backup.

    .PARAMETER CopyOnly
        When $true (default), uses COPY_ONLY for FULL backups to avoid disrupting backup chains.

    .PARAMETER MarkAsRetain
        When $true, marks generated backup file names as retained (useful to avoid accidental cleanup).

    .PARAMETER BlockSize
        BACKUP tuning option BLOCKSIZE.

    .PARAMETER BufferCount
        BACKUP tuning option BUFFERCOUNT.

    .PARAMETER MaxTransferSize
        BACKUP tuning option MAXTRANSFERSIZE.

    .PARAMETER DryRun
        Generates and returns the BACKUP SQL without executing it.

    .OUTPUTS
        PSCustomObject with BackupPath and (when -DryRun) Sql.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Instance,
        [Parameter(Mandatory)][string]$Database,

        [Parameter()][string]$BackupPath,
        [Parameter()][string]$AzureStorageBackupLocation,

        [Parameter()][bool]$Differential = $false,
        [Parameter()][bool]$CopyOnly = $true,
        [Parameter()][bool]$MarkAsRetain = $false,

        [Parameter()][int]$BlockSize = 65536,
        [Parameter()][int]$BufferCount = 50,
        [Parameter()][int]$MaxTransferSize = 2097152,

        [Parameter()][switch]$DryRun,
        [Parameter()][switch]$VerboseDiagnostics
    )

    $script:ExecutionID = [guid]::NewGuid().Guid
    $script:DBALibraryVerboseDiagnostics = $VerboseDiagnostics.IsPresent

    # Apply configured defaults when neither backup location is explicitly provided.
    if ([string]::IsNullOrWhiteSpace($BackupPath) -and [string]::IsNullOrWhiteSpace($AzureStorageBackupLocation)) {
        if (-not [string]::IsNullOrWhiteSpace($script:DefaultAzureStorageBackupLocation)) {
            $AzureStorageBackupLocation = $script:DefaultAzureStorageBackupLocation
        } elseif (-not [string]::IsNullOrWhiteSpace($script:DefaultBackupPath)) {
            $BackupPath = $script:DefaultBackupPath
        }
    }

    if ([string]::IsNullOrWhiteSpace($BackupPath) -and [string]::IsNullOrWhiteSpace($AzureStorageBackupLocation)) {
        throw "Specify -BackupPath or -AzureStorageBackupLocation (or set a default via Set-DBALibraryConfig)."
    }

    if (-not [string]::IsNullOrWhiteSpace($BackupPath) -and -not [string]::IsNullOrWhiteSpace($AzureStorageBackupLocation)) {
        throw "Specify only one of -BackupPath or -AzureStorageBackupLocation."
    }

    # Apply configurable tuning defaults when not explicitly provided.
    $effectiveBlockSize = if ($PSBoundParameters.ContainsKey('BlockSize')) { $BlockSize } elseif ($null -ne $script:DefaultBlockSize) { [int]$script:DefaultBlockSize } else { $BlockSize }
    $effectiveBufferCount = if ($PSBoundParameters.ContainsKey('BufferCount')) { $BufferCount } elseif ($null -ne $script:DefaultBufferCount) { [int]$script:DefaultBufferCount } else { $BufferCount }
    $effectiveMaxTransferSize = if ($PSBoundParameters.ContainsKey('MaxTransferSize')) { $MaxTransferSize } elseif ($null -ne $script:DefaultMaxTransferSize) { [int]$script:DefaultMaxTransferSize } else { $MaxTransferSize }

    # Compression is instance-dependent (requires SQL connectivity). For -DryRun, avoid
    # any instance calls and default to no compression so SQL preview is always generated.
    $compress = if ($DryRun.IsPresent) { $false } else { Get-SQLInstanceCompression -InstanceName $Instance }

    $credentialNameForWithCredential = $null
    $computedBackupPath = $null

    if (-not [string]::IsNullOrWhiteSpace($AzureStorageBackupLocation)) {
        Test-AzureStorageBackupLocation -AzureStorageBackupLocation $AzureStorageBackupLocation
        $azureInfo = Get-AzureStorageSasInfo -AzureStorageBackupLocation $AzureStorageBackupLocation

        if (-not [string]::IsNullOrWhiteSpace($azureInfo.SasToken)) {
            # Create credential on the instance but omit WITH CREDENTIAL in BACKUP SQL.
            # For -DryRun, skip instance calls.
            if (-not $DryRun.IsPresent) {
                Ensure-SqlAzureBlobCredential -InstanceName $Instance -CredentialName $azureInfo.CredentialName -SasToken $azureInfo.SasToken
            }
            $credentialNameForWithCredential = $null
        } else {
            $credentialNameForWithCredential = $azureInfo.CredentialName
            Log -Message "AzureStorageBackupLocation has no SAS token; assuming a SQL credential named '$credentialNameForWithCredential' already exists on [$Instance]." -Level Warning -WriteToHost
        }

        $computedBackupPath = Get-AzureBackupLocation -AzureStorageBackupLocation $AzureStorageBackupLocation -DatabaseName $Database -MarkAsRetain $MarkAsRetain -Differential $Differential
    } else {
        $shouldCreateFolders = -not $DryRun.IsPresent
        $computedBackupPath = Get-BackupLocation -InstanceName $Instance -DatabaseName $Database -CreateIfNotExist $shouldCreateFolders -MarkAsRetain $MarkAsRetain -Differential $Differential -BackupLocation $BackupPath
        if (-not $DryRun.IsPresent) {
            if (-not (Test-PathOnSQLServer -Instance $Instance -Path $computedBackupPath -TestDirectoryOnly $true)) {
                throw "Backup location ($(Get-DisplayPath $computedBackupPath)) not accessible from [$Instance]."
            }
        }
    }

    $progressMatch = if ($computedBackupPath -match '^https://') { ($computedBackupPath.Split('?', 2)[0] | Split-Path -Leaf) } else { $computedBackupPath }

    $backupParams = @{
        InstanceName     = $Instance
        DatabaseName     = $Database
        BackupPath       = $computedBackupPath
        ProgressID       = 1
        Compress         = $compress
        JobName          = 'BackupDatabase'
        CopyOnly         = $CopyOnly
        Differential     = $Differential
        BlockSize        = $effectiveBlockSize
        BufferCount      = $effectiveBufferCount
        MaxTransferSize  = $effectiveMaxTransferSize
        ProgressMatch    = $progressMatch
        CredentialName   = $credentialNameForWithCredential
    }

    $jobDetails = Backup-Database @backupParams -DryRun:$DryRun

    if ($DryRun.IsPresent) {
        return [pscustomobject]@{
            Instance   = $Instance
            Database   = $Database
            BackupPath = $computedBackupPath
            Sql        = $jobDetails.CmdString
        }
    }

    Progress2 -JobDetailsCollection @($jobDetails)

    return [pscustomobject]@{
        Instance   = $Instance
        Database   = $Database
        BackupPath = $computedBackupPath
    }
}
