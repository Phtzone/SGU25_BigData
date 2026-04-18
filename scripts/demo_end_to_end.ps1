Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot

if (-not (Get-Command wsl -ErrorAction SilentlyContinue)) {
    throw "WSL is required. Please install WSL and run again."
}

$wslRepoRoot = (& wsl wslpath -a "$repoRoot").Trim()
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($wslRepoRoot)) {
    throw "Cannot resolve repository path in WSL."
}

$bashCommand = "if [ -f ~/venvs/sgu25_bigdata/bin/activate ]; then source ~/venvs/sgu25_bigdata/bin/activate; fi; cd `"$wslRepoRoot`" && bash scripts/demo_end_to_end.sh"
& wsl bash -lc $bashCommand
exit $LASTEXITCODE
