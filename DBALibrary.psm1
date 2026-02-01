# Module entry point
# Publish-ready structure (Public/ + Private/) while keeping backwards compatibility.

$moduleRoot = Split-Path -Parent $PSCommandPath

$privatePath = Join-Path $moduleRoot 'Private'
if (Test-Path $privatePath) {
    Get-ChildItem -Path $privatePath -Filter '*.ps1' -File | ForEach-Object {
        . $_.FullName
    }
}

$publicPath = Join-Path $moduleRoot 'Public'
if (Test-Path $publicPath) {
    Get-ChildItem -Path $publicPath -Filter '*.ps1' -File | ForEach-Object {
        . $_.FullName
    }
}

# Export the stable public surface area.
Export-ModuleMember -Function @(
    'BackupAndRestore',
    'Get-DBALibraryConfig',
    'Set-DBALibraryConfig'
)
