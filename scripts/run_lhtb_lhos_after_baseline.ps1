param(
    [Parameter(Mandatory = $true)]
    [string]$SessionLog,
    [Parameter(Mandatory = $true)]
    [string]$BaselineStatus,
    [Parameter(Mandatory = $true)]
    [string]$RepoRoot,
    [Parameter(Mandatory = $true)]
    [string]$HarborProject,
    [Parameter(Mandatory = $true)]
    [string]$Tasks,
    [Parameter(Mandatory = $true)]
    [string]$JobsDir,
    [Parameter(Mandatory = $true)]
    [string]$Output,
    [Parameter(Mandatory = $true)]
    [string]$Log,
    [Parameter(Mandatory = $true)]
    [string]$Status
)

$ErrorActionPreference = 'Stop'
while (-not (Test-Path -LiteralPath $BaselineStatus)) {
    Start-Sleep -Seconds 30
}
$raw = Get-Content -LiteralPath $SessionLog -Raw
$match = [regex]::Match($raw, '\b1s[A-Za-z0-9]{50,100}\b')
if (-not $match.Success) {
    throw 'StepFun key was not found in the supplied local session log'
}
$key = $match.Value
$env:OPENAI_API_KEY = $key
$env:STEPFUN_API_KEY = $key
$env:PYTHONPATH = (Resolve-Path (Join-Path $RepoRoot 'src')).Path
$started = [DateTimeOffset]::UtcNow
try {
    Set-Location -LiteralPath $RepoRoot
    & python scripts\run_lhtb_lhos_batch.py `
        --harbor-project $HarborProject `
        --tasks $Tasks `
        --jobs-dir $JobsDir `
        --output $Output `
        --agent-timeout-seconds 900 `
        --worker-timeout-seconds 3600 `
        --max-concurrency 2 *> $Log
    $code = $LASTEXITCODE
}
catch {
    $code = 125
    $_ | Out-String | Add-Content -LiteralPath $Log
}
finally {
    Remove-Item Env:OPENAI_API_KEY, Env:STEPFUN_API_KEY -ErrorAction SilentlyContinue
    $key = $null
    $raw = $null
    $payload = @{
        schema_version = 'lhos-lhtb-lhos-background.v1'
        started_at = $started.ToString('o')
        finished_at = [DateTimeOffset]::UtcNow.ToString('o')
        exit_code = $code
        output = [System.IO.Path]::GetFullPath($Output)
        log = [System.IO.Path]::GetFullPath($Log)
    } | ConvertTo-Json
    [System.IO.Directory]::CreateDirectory(
        [System.IO.Path]::GetDirectoryName([System.IO.Path]::GetFullPath($Status))
    ) | Out-Null
    [System.IO.File]::WriteAllText(
        [System.IO.Path]::GetFullPath($Status),
        $payload
    )
}
exit $code
