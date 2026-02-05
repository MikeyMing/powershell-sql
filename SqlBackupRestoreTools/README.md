# SqlBackupRestoreTools

A PowerShell module for copying SQL Server databases via backup/restore, with optional SQL-backed logging and email notifications.

> This module wraps the existing script implementation in `SqlBackupRestoreTools.ps1` for backwards compatibility.

SqlBackupRestoreTools is for reliably cloning or refreshing SQL Server databases using a repeatable backup/restore workflow. It’s designed for “start it and trust it” operations: it runs a set of preflight checks up front to catch common failure conditions early (permissions, paths, connectivity, prerequisites), so you can kick off a refresh and expect it to complete as quickly as possible without surprises.

Why use it instead of doing it manually?
- Speed: one command instead of a long checklist of SSMS steps.
- Fewer failures: preflight validation reduces mid-run blowups and wasted time.
- Operational visibility: optional SQL-backed logging and email notifications.
- One-line control: set key behavior/properties of the target database (safe clone vs overwrite, offline/restore options, verify-only, dry run, etc.) in a single command.

Why not just use dbatools?
dbatools is excellent and very broad. SqlBackupRestoreTools is intentionally narrower and more opinionated: it standardizes a single “clone/refresh database” procedure with guardrails, logging, notifications, and a consistent interface for the options your team uses most.

## PowerShell Gallery

- Package page: https://www.powershellgallery.com/packages/SqlBackupRestoreTools

Discover from PowerShell:

```powershell
Find-Module SqlBackupRestoreTools -Repository PSGallery
Find-Module SqlBackupRestoreTools -AllVersions
```

## Install / Import

From the PowerShell Gallery:

```powershell
# First time only: you may be prompted to install the NuGet provider and/or trust PSGallery.
Install-Module SqlBackupRestoreTools -Scope CurrentUser

# Optional: trust PSGallery to avoid prompts
# Set-PSRepository -Name PSGallery -InstallationPolicy Trusted

Import-Module SqlBackupRestoreTools
Get-Command -Module SqlBackupRestoreTools

# Update later
# Update-Module SqlBackupRestoreTools

# Remove
# Uninstall-Module SqlBackupRestoreTools -AllVersions
```

From a local checkout (this repo):

```powershell
Import-Module .\SqlBackupRestoreTools.psd1 -Force
Get-Command -Module SqlBackupRestoreTools
```

## Primary command

- `BackupAndRestore`

## BackupAndRestore parameters (summary)

Use `Get-Help BackupAndRestore -Full` for full details and examples. This is a quick discovery index.

- Core: `-SourceInstance`, `-SourceDatabase`, `-TargetInstance`, `-TargetDatabase`
- Backup location (pick one): `-BackupPath`, `-AzureStorageBackupLocation`
- Safety/flow: `-CreateDatabase`, `-TakeTargetOffline`, `-TakeTargetOfflineMode`, `-AbortIfActiveSessions`, `-VerifyBackup`, `-PreflightOnly`, `-DryRun`
- Automation/diagnostics: `-BatchMode`, `-VerboseDiagnostics`, `-ResumeFromLatestBackup`
- Advanced restore workflows: `-Differential`, `-NoRecovery`, `-WaitforManualRestore`, `-IntermediateInstance`, `-RollForwardTransactionLogs`, `-LogBackupIntervalSeconds`, `-MaxLogBackupCycles`
- Target DB post-restore actions: `-RecoveryModel`, `-CompatabilityLevel`, `-ChangeCollation`, `-Collation`, `-ShrinkLog`, `-UpdateStats`, `-NoDBCC`, `-DeleteOrphans`, `-ScriptToRunOnTarget`
- Security/users: `-CopyUserRoles`, `-CreateLoginsIfTheyDontExist`, `-PreserveTargetSecurity`, `-RetainOwnerName`
- Backup retention/cleanup: `-NumberOfBackupsToRetain`, `-RetainByAgeDays`, `-MarkAsRetain`
- Performance tuning: `-BlockSize`, `-BufferCount`, `-MaxTransferSize`, `-CopyOnly`
- Logging/notifications: `-EnableDbLogging`, `-EmailAddress`, `-FromAddress`
- Misc/legacy: `-DontCheckSpace`, `-DontBackupTarget`, `-OverwriteTarget` (legacy/no-op)

## Getting help

```powershell
Get-Help about_SqlBackupRestoreTools
Get-Help BackupAndRestore -Full
Get-Help Get-DBALibraryConfig -Full
Get-Help Set-DBALibraryConfig -Full
```

## Configuration

Defaults are intentionally generic for public distribution. Configure once per session:

```powershell
Set-DBALibraryConfig -DBAInstance 'SERVER\INSTANCE' -DBADatabase 'DBA' -SMTPEnabled $true -SmtpServer 'smtp.yourdomain.local'
Get-DBALibraryConfig
```

Set a default backup location (used when you omit both `-BackupPath` and `-AzureStorageBackupLocation`):

```powershell
Set-DBALibraryConfig -DefaultBackupPath '\\fileserver\sqlbackups'
```

Set default BACKUP/RESTORE tuning values (used when you omit these parameters on `BackupAndRestore`):

```powershell
Set-DBALibraryConfig -DefaultBlockSize 65536 -DefaultBufferCount 50 -DefaultMaxTransferSize 2097152
```

Persist config per-user (so you don't have to set it every session):

```powershell
Set-DBALibraryConfig -DefaultBackupPath '\\fileserver\sqlbackups' -Persist
Get-DBALibraryConfig -Persisted
```

Clear persisted and/or session config:

```powershell
Clear-DBALibraryConfig -Persisted -Session
```

On Windows, the persisted config file is stored under:
- `%APPDATA%\SqlBackupRestoreTools\config.json`

## Prerequisites

- **SQL operations**: Requires the `SqlServer` PowerShell module (`Invoke-Sqlcmd`).
- **Email autodiscovery (optional)**: If you omit `-EmailAddress`, the script attempts to resolve the current user’s email from AD via `Get-ADUser`. If the current user has no email in AD, you must supply `-EmailAddress`.

## Common examples

## Common scenarios

For `-BackupPath`, prefer a UNC path (network share) that the SQL Server service account(s) can access (source/target). A local drive like `E:\Temp` is resolved on the SQL Server host and usually won’t work across different servers.

Clone a database to a new target DB (safe default):

```powershell
BackupAndRestore `
  -SourceInstance 'SERVER\INSTANCE' -SourceDatabase 'MyDb' `
  -TargetInstance 'SERVER\INSTANCE' -TargetDatabase 'MyDb_Copy' `
  -BackupPath '\\fileserver\sqlbackups\staging' `
  -BatchMode
```

Overwrite an existing target DB (destructive):

```powershell
BackupAndRestore `
  -SourceInstance 'SERVER\INSTANCE' -SourceDatabase 'MyDb' `
  -TargetInstance 'SERVER\INSTANCE' -TargetDatabase 'MyDb' `
  -BackupPath '\\fileserver\sqlbackups\staging' `
  -CreateDatabase $false -TakeTargetOffline $true `
  -BatchMode
```

Azure Blob Storage (container URL with SAS):

```powershell
$containerUrlWithSas = 'https://<account>.blob.core.windows.net/<container>?sv=...&sig=...'
BackupAndRestore `
  -SourceInstance 'SERVER\INSTANCE' -SourceDatabase 'MyDb' `
  -TargetInstance 'SERVER\INSTANCE' -TargetDatabase 'MyDb_Copy' `
  -AzureStorageBackupLocation $containerUrlWithSas `
  -BatchMode
```

Enable SQL-backed logging:

```powershell
Set-DBALibraryConfig -DBAInstance 'SERVER\INSTANCE' -DBADatabase 'DBA'
BackupAndRestore `
  -SourceInstance 'SERVER\INSTANCE' -SourceDatabase 'MyDb' `
  -TargetInstance 'SERVER\INSTANCE' -TargetDatabase 'MyDb_Copy' `
  -BackupPath '\\fileserver\sqlbackups\staging' `
  -EnableDbLogging -BatchMode
```

Validate without executing (preflight / dry run):

```powershell
BackupAndRestore `
  -SourceInstance 'SERVER\INSTANCE' -SourceDatabase 'MyDb' `
  -TargetInstance 'SERVER\INSTANCE' -TargetDatabase 'MyDb_Copy' `
  -BackupPath '\\fileserver\sqlbackups\staging' `
  -PreflightOnly -BatchMode

BackupAndRestore `
  -SourceInstance 'SERVER\INSTANCE' -SourceDatabase 'MyDb' `
  -TargetInstance 'SERVER\INSTANCE' -TargetDatabase 'MyDb_Copy' `
  -BackupPath '\\fileserver\sqlbackups\staging' `
  -DryRun -BatchMode
```

Create a new database from a source backup/restore:

```powershell
BackupAndRestore `
  -SourceInstance 'SERVER\INSTANCE' -SourceDatabase 'MyDb' `
  -TargetInstance 'SERVER\INSTANCE' -TargetDatabase 'MyDb_Copy' `
  -BackupPath '\\fileserver\sqlbackups\staging' `
  -BatchMode $true
```

Enable SQL-backed logging (writes to `dbo.Log` in the configured DBA database):

```powershell
BackupAndRestore `
  -SourceInstance 'SERVER\INSTANCE' -SourceDatabase 'MyDb' `
  -TargetInstance 'SERVER\INSTANCE' -TargetDatabase 'MyDb_Copy' `
  -BackupPath '\\fileserver\sqlbackups\staging' `
  -BatchMode $true `
  -EnableDbLogging
```

Send email explicitly (recommended if AD has no `mail` value for the current user):

```powershell
BackupAndRestore `
  -SourceInstance 'SERVER\INSTANCE' -SourceDatabase 'MyDb' `
  -TargetInstance 'SERVER\INSTANCE' -TargetDatabase 'MyDb_Copy' `
  -BackupPath '\\fileserver\sqlbackups\staging' `
  -BatchMode $true `
  -EmailAddress 'you@domain.com' -FromAddress 'you@domain.com'
```

Use Azure Blob Storage as the backup/restore location:

```powershell
$containerUrlWithSas = 'https://<account>.blob.core.windows.net/<container>?sv=...&sig=...'
BackupAndRestore `
  -SourceInstance 'SERVER\\INSTANCE' -SourceDatabase 'MyDb' `
  -TargetInstance 'SERVER\\INSTANCE' -TargetDatabase 'MyDb_Copy' `
  -AzureStorageBackupLocation $containerUrlWithSas `
  -BatchMode $true
```

Overwrite an existing target database (destructive):

```powershell
BackupAndRestore `
  -SourceInstance 'SERVER\INSTANCE' -SourceDatabase 'MyDb' `
  -TargetInstance 'SERVER\INSTANCE' -TargetDatabase 'MyDb' `
  -BackupPath '\\fileserver\sqlbackups\staging' `
  -CreateDatabase $false -BatchMode $true `
  -TakeTargetOffline $true
```

## Notes

- `Send-MailMessage` is deprecated in PowerShell; this module currently uses it for compatibility.
- SMTP delivery depends on your organization’s mail relay policy (allowed From/To, external relay, quarantine).

## License

MIT. See [LICENSE](LICENSE).
