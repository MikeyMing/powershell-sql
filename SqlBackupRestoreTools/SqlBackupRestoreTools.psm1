# Module entry point
# Publish-ready structure (Public/ + Private/) while keeping backwards compatibility.

$moduleRoot = Split-Path -Parent $PSCommandPath

$privatePath = Join-Path $moduleRoot 'Private'
if (Test-Path $privatePath) {
    Get-ChildItem -Path $privatePath -Filter '*.ps1' -File | Sort-Object Name | ForEach-Object {
        . $_.FullName
    }
}

$publicPath = Join-Path $moduleRoot 'Public'
if (Test-Path $publicPath) {
    Get-ChildItem -Path $publicPath -Filter '*.ps1' -File | Sort-Object Name | ForEach-Object {
        . $_.FullName
    }
}

# Load per-user persisted configuration (opt-out via env var SQLBACKUPRESTORETOOLS_DISABLE_PERSISTED_CONFIG).
if (Get-Command Initialize-SbrtConfigFromDisk -ErrorAction SilentlyContinue) {
    Initialize-SbrtConfigFromDisk
}

# Export the stable public surface area.
Export-ModuleMember -Function @(
    'BackupAndRestore',
    'Clear-DBALibraryConfig',
    'Get-DBALibraryConfig',
    'Set-DBALibraryConfig'
)
