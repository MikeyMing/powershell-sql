@{
    RootModule        = 'SqlBackupRestoreTools.psm1'
    ModuleVersion     = '2.0.36'
    GUID              = '78575d6b-ce20-4124-92bc-3396f257eb29'
    Author            = 'Mike Fleming (@BelugaMike)'
    CompanyName       = ''
    Copyright         = '(c) 2026 Mike Fleming'
    Description       = 'SQL Server database backup/restore helper module with optional SQL-backed logging and email notifications.'
    PowerShellVersion = '5.1'

    RequiredModules   = @('SqlServer')

    FunctionsToExport = @(
        'BackupAndRestore',
        'Backup-DbalDatabase',
        'Clear-DBALibraryConfig',
        'Get-DBALibraryConfig',
        'Restore-DbalDatabase',
        'Set-DBALibraryConfig'
    )
    CmdletsToExport   = @()
    VariablesToExport = @()
    AliasesToExport   = @()

    PrivateData = @{
        PSData = @{
            Tags         = @('SQLServer','DBA','Backup','Restore')
            ProjectUri   = 'https://github.com/MikeyMing/powershell-sql'
            LicenseUri   = 'https://github.com/MikeyMing/powershell-sql/blob/main/LICENSE'
            ReleaseNotes = '2.0.36: Fix $S undefined variable when -DontCheckSpace is true. Thread -TrustServerCertificate through all background backup/restore jobs (Backup-Database, Backup-TransactionLog, Restore-SQLTransactionLog, Restore-SQLDatabase). See CHANGELOG.md.'
        }
    }
}
