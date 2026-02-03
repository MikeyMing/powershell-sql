[CmdletBinding()]
param(
    [switch]$WhatIf,
    [string]$ModulePath = '.\SqlBackupRestoreTools'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# Allow local override via PublishSettings.local.ps1 (gitignored)
$settingsPath = Join-Path -Path $PSScriptRoot -ChildPath 'PublishSettings.local.ps1'
if (Test-Path $settingsPath) {
    . $settingsPath
    if ($PSBoundParameters.ContainsKey('ModulePath') -and (Get-Variable ModulePath -ErrorAction SilentlyContinue)) {
        # explicit param wins
    } elseif (Get-Variable ModulePath -ErrorAction SilentlyContinue) {
        $ModulePath = $ModulePath
    }
}

# Prefer environment variable if present (useful in CI)
$apiKey = $env:PSGALLERY_API_KEY
if ([string]::IsNullOrWhiteSpace($apiKey) -and (Get-Variable PSGalleryApiKey -ErrorAction SilentlyContinue)) {
    $apiKey = $PSGalleryApiKey
}

if ([string]::IsNullOrWhiteSpace($apiKey)) {
    throw "Missing PSGallery API key. Set env var PSGALLERY_API_KEY or create PublishSettings.local.ps1 (see PublishSettings.example.ps1)."
}

if ($apiKey -match '^<.*>$' -or $apiKey -eq '<YOUR_PSGALLERY_API_KEY>') {
    throw "PSGallery API key is still a placeholder. Edit PublishSettings.local.ps1 (gitignored) and set $PSGalleryApiKey to your real key, or set env var PSGALLERY_API_KEY."
}

$manifest = Join-Path -Path $ModulePath -ChildPath 'SqlBackupRestoreTools.psd1'
if (-not (Test-Path $manifest)) {
    throw "Module manifest not found at: $manifest"
}

Write-Host "Testing manifest: $manifest"
Test-ModuleManifest -Path $manifest | Out-Null

Write-Host "Publishing module from: $ModulePath"
if ($WhatIf) {
    Publish-Module -Path $ModulePath -NuGetApiKey $apiKey -WhatIf
} else {
    Publish-Module -Path $ModulePath -NuGetApiKey $apiKey
}
