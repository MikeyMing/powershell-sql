# Changelog

All notable changes to this project will be documented in this file.

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

