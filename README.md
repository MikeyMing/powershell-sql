# DBALibrary

A PowerShell module for copying SQL Server databases via backup/restore, with optional SQL-backed logging and email notifications.

> This repo currently wraps the existing script implementation in `DBALibrary.ps1` for backwards compatibility.

## Install / Import

From this folder:

```powershell
Import-Module .\DBALibrary.psd1 -Force
Get-Command -Module DBALibrary
```

## Primary command

- `BackupAndRestore`

## Configuration

Defaults are intentionally generic for public distribution. Configure once per session:

```powershell
Set-DBALibraryConfig -DBAInstance 'SERVER\INSTANCE' -DBADatabase 'DBA' -SMTPEnabled $true -SmtpServer 'smtp.yourdomain.local'
Get-DBALibraryConfig
```

## Prerequisites

- **SQL operations**: Requires the `SqlServer` PowerShell module (`Invoke-Sqlcmd`).
- **Email autodiscovery (optional)**: If you omit `-EmailAddress`, the script attempts to resolve the current user’s email from AD via `Get-ADUser`. If the current user has no email in AD, you must supply `-EmailAddress`.

## Common examples

Create a new database from a source backup/restore:

```powershell
BackupAndRestore \
  -SourceInstance 'SERVER\INSTANCE' -SourceDatabase 'MyDb' \
  -TargetInstance 'SERVER\INSTANCE' -TargetDatabase 'MyDb_Copy' \
  -BackupPath 'E:\Temp' \
  -BatchMode $true
```

Enable SQL-backed logging (writes to `dbo.Log` in the configured DBA database):

```powershell
BackupAndRestore \
  -SourceInstance 'SERVER\INSTANCE' -SourceDatabase 'MyDb' \
  -TargetInstance 'SERVER\INSTANCE' -TargetDatabase 'MyDb_Copy' \
  -BackupPath 'E:\Temp' \
  -BatchMode $true \
  -EnableDbLogging
```

Send email explicitly (recommended if AD has no `mail` value for the current user):

```powershell
BackupAndRestore \
  -SourceInstance 'SERVER\INSTANCE' -SourceDatabase 'MyDb' \
  -TargetInstance 'SERVER\INSTANCE' -TargetDatabase 'MyDb_Copy' \
  -BackupPath 'E:\Temp' \
  -BatchMode $true \
  -EmailAddress 'you@domain.com' -FromAddress 'you@domain.com'
```

Use Azure Blob Storage as the backup/restore location:

```powershell
$containerUrlWithSas = 'https://<account>.blob.core.windows.net/<container>?sv=...&sig=...'
BackupAndRestore \
  -SourceInstance 'SERVER\\INSTANCE' -SourceDatabase 'MyDb' \
  -TargetInstance 'SERVER\\INSTANCE' -TargetDatabase 'MyDb_Copy' \
  -AzureStorageBackupLocation $containerUrlWithSas \
  -BatchMode $true

Overwrite an existing target database (destructive):

```powershell
BackupAndRestore \
  -SourceInstance 'SERVER\INSTANCE' -SourceDatabase 'MyDb' \
  -TargetInstance 'SERVER\INSTANCE' -TargetDatabase 'MyDb' \
  -BackupPath 'E:\Temp' \
  -CreateDatabase $false -BatchMode $true \
  -TakeTargetOffline $true
```

Notes:
- `CreateDatabase` now defaults to `$true` (safer default). Use `-CreateDatabase $false` when you intend to overwrite an existing target DB.
```

Notes:
- `-BackupPath` and `-AzureStorageBackupLocation` are mutually exclusive.
- The Azure access check validates the SAS token from the machine running PowerShell.
- SQL Server BACKUP/RESTORE to URL uses a SQL credential; the module will auto-create a credential (requires `ALTER ANY CREDENTIAL`) when a SAS is provided.
- If you provide an Azure URL without a SAS token, a matching credential must already exist on the SQL instance(s).

## Notes

- `Send-MailMessage` is deprecated in PowerShell; this module currently uses it for compatibility.
- SMTP delivery depends on your organization’s mail relay policy (allowed From/To, external relay, quarantine).

## Tests

Run unit tests:

```powershell
Invoke-Pester -Path .\Tests -Verbose
```

Run the Azure Storage integration test (requires a container SAS URL):

```powershell
\Tests\Run-IntegrationTests.ps1 -AzureContainerSasUrl 'https://<account>.blob.core.windows.net/<container>?sv=...&sig=...'
```

The Azure integration test is skipped unless `DBALIBRARY_TEST_AZURE_CONTAINER_SAS_URL` is set (either via the helper script or directly in the same process running Pester).

If you have multiple integration profiles in `\Tests\IntegrationSettings.local.ps1`, you can run them all in one go:

```powershell
\Tests\Run-IntegrationTests.ps1 -AllProfiles
```

Or select a specific profile:

```powershell
$env:DBALIBRARY_TEST_PROFILE = 'CrossInstance'
Invoke-Pester -Path .\Tests\DBALibrary.Integration.Tests.ps1 -Verbose
```

To run the full `BackupAndRestore` integration tests, create a local settings file (kept out of source control):

- Copy `\Tests\IntegrationSettings.example.ps1` to `\Tests\IntegrationSettings.local.ps1`
- Set `AllowDestructive = $true` and fill in `SourceInstance` / `BackupPath`
- Optional: set `TargetInstance` to test cross-instance restores. For filesystem restores, `BackupPath` must be a UNC share accessible to both SQL Server service accounts.
- Run: `Invoke-Pester -Path .\Tests -Verbose`

## License

MIT. See [LICENSE](LICENSE).
