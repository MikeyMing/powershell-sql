# Example publish settings for SqlBackupRestoreTools
#
# IMPORTANT:
# - Do NOT put real secrets in this file.
# - Copy this file to PublishSettings.local.ps1 (gitignored) and put your real key there.
# - Alternatively set $env:PSGALLERY_API_KEY.

# This script is dot-sourced by Publish.ps1 and should define $PSGalleryApiKey.
$PSGalleryApiKey = '<YOUR_PSGALLERY_API_KEY>'
