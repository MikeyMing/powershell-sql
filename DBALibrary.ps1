function log ([string]$message, [ValidateSet("Info","Error","Warning")][string]$Level = "Info", [switch]$WriteToHost, [ConsoleColor]$ForegroundColour = "White", [string]$Note)
{
Try
    {
    If ($WriteToHost)
        {
        Write-Host $message -ForegroundColor $ForegroundColour
        }
    $message = $message -replace "`'" , ""
    If ($Level-eq "Error") { $errorDetails = $($Error[0] | Out-String) -replace "`'", ""}
    $errordetails = $errorDetails -replace "`[$]", "X"
    $errorLine = $($Error[0].InvocationInfo.Line | Out-String) -replace "`'", ""
    $errorLine = $errorLine -replace "`[$]", "X"
    $ErrorLineNumber = $($Error[0].InvocationInfo.ScriptLineNumber)
    $FunctionName = $($(Get-Variable MyInvocation -Scope 1).Value.MyCommand.Name)
    $scriptName = $MyInvocation.ScriptName
    if ($Level -eq "Error")
        {
        [string]$logQuery = "EXEC sp_log @Message = '" + $message + "', @ScriptName = '" + $scriptName + "', @FunctionName = '" + $FunctionName + "', @Level = '" + "Error" + "', @ExecutionId = '" + $ExecutionID + "', @Errordetails = '" + $errorDetails + "', @ErrorLineNumber = '" + $ErrorLineNumber + "', @ErrorLine = '" + $errorLine + "', @ScriptLineNumber ='" + $MyInvocation.ScriptLineNumber + "' , @Note = '" + $note + "'"
        }
    else 
        {
        [string]$logQuery = "EXEC sp_log @Message = '" + $message + "', @ScriptName = '" + $MyInvocation.ScriptName + "', @FunctionName = '" + $($(Get-Variable MyInvocation -Scope 1).Value.MyCommand.Name) + "', @Level = '" + "Info" + "', @ExecutionId = '" + $ExecutionID + "', @ScriptLineNumber ='" + $MyInvocation.ScriptLineNumber + "' , @Note = '" + $Note + "'"
        }
    Invoke-Sqlcmd -ServerInstance $dbainstance -Database $dbadatabase -Query $logQuery 
    } Catch {Write-Host "Error in log" -ForegroundColor Red ; Throw }
}
