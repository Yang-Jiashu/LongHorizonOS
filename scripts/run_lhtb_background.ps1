param(
    [Parameter(Mandatory = $true)]
    [string]$SessionLog,
    [Parameter(Mandatory = $true)]
    [string]$LhtbRoot,
    [Parameter(Mandatory = $true)]
    [string]$Config,
    [Parameter(Mandatory = $true)]
    [string]$Output,
    [Parameter(Mandatory = $true)]
    [string]$Log,
    [Parameter(Mandatory = $true)]
    [string]$Status
)

$ErrorActionPreference = 'Stop'
$statusPath = [System.IO.Path]::GetFullPath($Status)
$logPath = [System.IO.Path]::GetFullPath($Log)
$outputPath = [System.IO.Path]::GetFullPath($Output)
$raw = Get-Content -LiteralPath $SessionLog -Raw
$match = [regex]::Match($raw, '\b1s[A-Za-z0-9]{50,100}\b')
if (-not $match.Success) {
    throw 'StepFun key was not found in the supplied local session log'
}
$key = $match.Value
$env:OPENAI_API_KEY = $key
$env:STEPFUN_API_KEY = $key
$env:PYTHONUTF8 = '1'
$env:PYTHONIOENCODING = 'utf-8'
$env:DOCKER_DEFAULT_PLATFORM = 'linux/amd64'
$env:HB_VERIFIER_FEEDBACK_MODE = 'binary'
$started = [DateTimeOffset]::UtcNow
try {
    Set-Location -LiteralPath $LhtbRoot
    & uv run --project .\harbor harbor run -c $Config -o $outputPath -n 1 -y *> $logPath
    $code = $LASTEXITCODE
}
catch {
    $code = 125
    $_ | Out-String | Add-Content -LiteralPath $logPath
}
finally {
    Remove-Item Env:OPENAI_API_KEY, Env:STEPFUN_API_KEY -ErrorAction SilentlyContinue
    $key = $null
    $raw = $null
    $payload = @{
        schema_version = 'lhos-lhtb-background.v1'
        started_at = $started.ToString('o')
        finished_at = [DateTimeOffset]::UtcNow.ToString('o')
        exit_code = $code
        output = $outputPath
        log = $logPath
    } | ConvertTo-Json
    [System.IO.Directory]::CreateDirectory([System.IO.Path]::GetDirectoryName($statusPath)) | Out-Null
    [System.IO.File]::WriteAllText($statusPath, $payload)
}
exit $code
