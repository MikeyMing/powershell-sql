function Get-DBALibraryConfig {
    [CmdletBinding()]
    param()

    [pscustomobject]@{
        DBAInstance  = $script:DBAInstance
        DBADatabase  = $script:DBADatabase
        SmtpServer   = $script:smtpserver
        SMTPEnabled  = [bool]$script:SMTPEnabled
    }
}
