# Changelog

All notable changes to this project will be documented in this file.

## [2.0.38] - 2026-04-07

### Added
- `BackupAndRestore` now accepts `-WaitForActiveQueries`, `-QueryWaitMaxSeconds` (default 300), and `-QueryWaitPollSeconds` (default 10) parameters. When `-WaitForActiveQueries $true`, the function polls for active user sessions on the source database before beginning the backup, logging elapsed wait time and warning if the timeout is reached.
- `BackupAndRestore` now returns a `PSCustomObject` with fields: `Status`, `SourceInstance`, `SourceDatabase`, `TargetInstance`, `TargetDatabase`, `StartTime`, `EndTime`, `TotalDuration`, `TotalSeconds`, `QueryWaitSeconds`, `BackupStartTime`, `BackupEndTime`, `BackupDuration`, `BackupFilePath`, `BackupFileSizeBytes`, `RestoreStartTime`, `RestoreEndTime`, `RestoreDuration`, `UsersWithNoLogins`, `ErrorMessage`. Both success and failure paths populate this object.

## [2.0.37] - 2026-04-02

### Changed
- Completed `TrustServerCertificate` propagation in remaining execution paths (`Get-DatabaseState`, backup wrapper paths).
- Hardened `Progress2` progress parsing/output to avoid terminal conversion failures during long-running jobs.
- Removed stale post-success `$MailMessage` concatenation that could throw after successful backup/restore.
- Added explicit heartbeat output around backup/restore phases and job starts for better live visibility.

## [2.0.36] - 2026-04-02

### Changed
- Fixed undefined `$S` when `-DontCheckSpace` is used.
- Threaded `-TrustServerCertificate` through background backup/restore job functions.

## [2.0.5] - 2026-02-05

### Added
- Configurable default backup tuning values (`BlockSize`, `BufferCount`, `MaxTransferSize`) via `Set-DBALibraryConfig` (optionally persisted).

## [2.0.6] - 2026-02-05

### Added
- Standalone cmdlets for running one half of the workflow:
	- `Backup-DbalDatabase`
	- `Restore-DbalDatabase`

## [2.0.7] - 2026-02-05

### Changed
- Improved progress visibility for backup/restore operations in VS Code terminals.
- Friendlier error messages when an instance is unreachable (includes operation context and preserves original exception details).

## [2.0.8] - 2026-02-05

### Changed
- Added preflight validation for common failure cases:
	- Backup: clearer error when the source database doesn’t exist or isn’t accessible.
	- Restore: clearer error when the target database already exists but `-CreateDatabase` is `$true`, or when it doesn’t exist but `-CreateDatabase` is `$false`.

## [2.0.9] - 2026-02-05

### Changed
- Added actionable error hints for common SQL failures (permissions, login failure, path access, and connectivity).
- Preserved underlying job failure exceptions so callers see better root-cause details.

## [2.0.4] - 2026-02-02

### Changed
- Clarified README installation instructions for PowerShell Gallery (install/update/uninstall + PSGallery trust note).

## [2.0.3] - 2026-02-02

### Changed
- Expanded `BackupAndRestore` comment-based help with a “Common scenarios” section.
- Updated READMEs to include scenario-based recipes (clone, overwrite, Azure, logging, preflight, dry run).

## [2.0.2] - 2026-02-02

### Added
- Optional per-user persisted configuration (JSON under the user profile) with auto-load on module import.
- `Clear-DBALibraryConfig` to reset session config and/or delete persisted config.

### Changed
- `Set-DBALibraryConfig` can now persist updates via `-Persist`.
- `Get-DBALibraryConfig` can show persisted config via `-Persisted`.

## [2.0.1] - 2026-02-02

### Added
- Comment-based help for exported cmdlets and an `about_SqlBackupRestoreTools` topic.

### Changed
- Updated documentation to reflect the published module name `SqlBackupRestoreTools`.

## [2.0.0] - 2026-02-02

### Added
- PowerShell Gallery-ready module folder layout.
- Public configuration cmdlets: `Get-DBALibraryConfig`, `Set-DBALibraryConfig`.

### Changed
- Module entry point now loads `Public/` and `Private/` scripts.

