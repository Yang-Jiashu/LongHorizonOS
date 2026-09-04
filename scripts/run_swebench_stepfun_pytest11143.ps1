param(
    [Parameter(Mandatory = $true)]
    [string]$SourceRepo,

    [string]$Node = "C:\Users\yangjiashu\.meituan-catpaw\runtimes\node\versions\24.18.0\node.exe",
    [string]$Dsh = "C:\Users\yangjiashu\Temp\dsh-runtime-rc8-pnpm\node_modules\@deepseek-ai\dsh\lib\bin.js",
    [string]$Python = "python",
    [string]$EvalPython = "",
    [string]$OutputDir = "",
    [double]$TimeoutSeconds = 900
)

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$expectedCommit = "6995257cf470d2143ad1683824962de4071c0eb7"
$testPatch = Join-Path $repoRoot "artifacts\swebench-official-pytest11143-20260820\test.patch"
$providerPatch = Join-Path $repoRoot "benchmarks\real_dsh_dynamic_coding\stepfun-3.7-pi-ai.cordis.patch.yml"

if ([string]::IsNullOrWhiteSpace($OutputDir)) {
    $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $OutputDir = Join-Path $repoRoot "artifacts\swe-stepfun-pytest11143-$stamp"
}

foreach ($path in @($SourceRepo, $Node, $Dsh, $testPatch, $providerPatch)) {
    if (-not (Test-Path -LiteralPath $path)) {
        throw "Required path does not exist: $path"
    }
}
if (Test-Path -LiteralPath $OutputDir) {
    throw "Output directory already exists: $OutputDir"
}

$head = (& git -C $SourceRepo rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0 -or $head -ne $expectedCommit) {
    throw "SourceRepo must be checked out at $expectedCommit; found $head"
}
$status = & git -C $SourceRepo status --porcelain
if ($LASTEXITCODE -ne 0 -or $status) {
    throw "SourceRepo must have a clean tracked worktree."
}

$nodeVersion = (& $Node --version).Trim()
if ($LASTEXITCODE -ne 0 -or $nodeVersion -notmatch "^v(2[4-9]|[3-9][0-9])\.") {
    throw "DeepSeek Harness rc.8 requires Node 24 or newer; found $nodeVersion"
}

$secureKey = Read-Host "STEPFUN_API_KEY" -AsSecureString
$plainKey = [System.Net.NetworkCredential]::new("", $secureKey).Password
$oldStepfunKey = $env:STEPFUN_API_KEY
$oldPythonPath = $env:PYTHONPATH
$oldKeyPool = $env:LHOS_DSH_API_KEYS

try {
    $env:STEPFUN_API_KEY = $plainKey
    $env:PYTHONPATH = Join-Path $repoRoot "src"
    Remove-Item Env:LHOS_DSH_API_KEYS -ErrorAction SilentlyContinue
    $runnerArgs = @(
        "-m", "lhos.benchmarks.swe_host_native",
        "--source-repo", (Resolve-Path -LiteralPath $SourceRepo).Path,
        "--test-patch", $testPatch,
        "--node", (Resolve-Path -LiteralPath $Node).Path,
        "--dsh", (Resolve-Path -LiteralPath $Dsh).Path,
        "--patch", $providerPatch,
        "--output-dir", $OutputDir,
        "--credential-env", "STEPFUN_API_KEY",
        "--timeout-seconds", $TimeoutSeconds
    )
    if (-not [string]::IsNullOrWhiteSpace($EvalPython)) {
        $runnerArgs += @("--eval-python", (Resolve-Path -LiteralPath $EvalPython).Path)
    }

    & $Python @runnerArgs
    if ($LASTEXITCODE -ne 0) {
        throw "SWE host-native pair failed with exit code $LASTEXITCODE"
    }

    $result = Join-Path $OutputDir "result.json"
    & $Python (Join-Path $repoRoot "scripts\export_swe_predictions.py") $result `
        --static-model-name "step-3.7-flash-medium+dsh-static" `
        --lhos-model-name "step-3.7-flash-medium+dsh+longhorizonos"
    if ($LASTEXITCODE -ne 0) {
        throw "Prediction export failed with exit code $LASTEXITCODE"
    }
    & $Python (Join-Path $repoRoot "scripts\profile_swe_case.py") $result `
        --model "step-3.7-flash" `
        --reasoning "medium" `
        --harness "DeepSeek Harness 0.1.0-rc.8"
    if ($LASTEXITCODE -ne 0) {
        throw "Case profiling failed with exit code $LASTEXITCODE"
    }
}
finally {
    $plainKey = $null
    if ($null -eq $oldStepfunKey) {
        Remove-Item Env:STEPFUN_API_KEY -ErrorAction SilentlyContinue
    }
    else {
        $env:STEPFUN_API_KEY = $oldStepfunKey
    }
    if ($null -eq $oldPythonPath) {
        Remove-Item Env:PYTHONPATH -ErrorAction SilentlyContinue
    }
    else {
        $env:PYTHONPATH = $oldPythonPath
    }
    if ($null -eq $oldKeyPool) {
        Remove-Item Env:LHOS_DSH_API_KEYS -ErrorAction SilentlyContinue
    }
    else {
        $env:LHOS_DSH_API_KEYS = $oldKeyPool
    }
}

Write-Output "Result: $(Join-Path $OutputDir 'result.json')"
Write-Output "Profile: $(Join-Path $OutputDir 'CASE-PROFILE.zh-CN.md')"
Write-Output "LHOS prediction: $(Join-Path $OutputDir 'predictions.json')"
