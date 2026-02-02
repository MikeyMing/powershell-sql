function Set-DBALibraryConfig {
    [CmdletBinding(SupportsShouldProcess)]
    param(
        [string]$DBAInstance,
        [string]$DBADatabase,
        [string]$SmtpServer,
        [Nullable[bool]]$SMTPEnabled
    )

    if ($PSCmdlet.ShouldProcess('DBALibrary configuration', 'Update')) {
        if ($PSBoundParameters.ContainsKey('DBAInstance')) { $script:DBAInstance = $DBAInstance }
        if ($PSBoundParameters.ContainsKey('DBADatabase')) { $script:DBADatabase = $DBADatabase }
        if ($PSBoundParameters.ContainsKey('SmtpServer')) { $script:smtpserver = $SmtpServer }
        if ($PSBoundParameters.ContainsKey('SMTPEnabled')) { $script:SMTPEnabled = $SMTPEnabled }
    }
}
