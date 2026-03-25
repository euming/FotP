[CmdletBinding()]
param(
    [string]$Configuration = "Debug"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$testsDir = Split-Path -Parent $scriptDir
$memoryCtlDir = Split-Path -Parent $testsDir
$repoRoot = Split-Path -Parent (Split-Path -Parent $memoryCtlDir)

$fixturesDir = Join-Path $testsDir "fixtures"
$goldenDir = Join-Path $testsDir "golden"
$tempDir = Join-Path $testsDir ".characterization_tmp"

if (Test-Path $tempDir) {
    Remove-Item -Recurse -Force $tempDir
}
New-Item -ItemType Directory -Path $tempDir | Out-Null

$cases = @(
    @{
        Name = "mixed-card-states.validate"
        Command = @("validate", "--db", "tests/fixtures/mixed-card-states.memory.jsonl")
        Golden = "mixed-card-states.validate.txt"
    },
    @{
        Name = "mixed-card-states.query-explain"
        Command = @("query", "--db", "tests/fixtures/mixed-card-states.memory.jsonl", "--q", "cache workaround", "--top", "10", "--explain")
        Golden = "mixed-card-states.query-explain.txt"
    },
    @{
        Name = "mixed-card-states.prompt"
        Command = @("prompt", "--db", "tests/fixtures/mixed-card-states.memory.jsonl", "--q", "cache workaround", "--top", "10")
        Golden = "mixed-card-states.prompt.txt"
    },
    @{
        Name = "duplicate-link-attempts.validate"
        Command = @("validate", "--db", "tests/fixtures/duplicate-link-attempts.memory.jsonl")
        Golden = "duplicate-link-attempts.validate.txt"
    },
    @{
        Name = "duplicate-link-attempts.list-memanchors"
        Command = @("list-memanchors", "--db", "tests/fixtures/duplicate-link-attempts.memory.jsonl")
        Golden = "duplicate-link-attempts.list-memanchors.txt"
    },
    @{
        Name = "missing-optional-payloads.validate"
        Command = @("validate", "--db", "tests/fixtures/missing-optional-payloads.memory.jsonl")
        Golden = "missing-optional-payloads.validate.txt"
    },
    @{
        Name = "missing-optional-payloads.query-explain"
        Command = @("query", "--db", "tests/fixtures/missing-optional-payloads.memory.jsonl", "--q", "payload", "--top", "10", "--explain")
        Golden = "missing-optional-payloads.query-explain.txt"
    },
    @{
        Name = "missing-optional-payloads.prompt"
        Command = @("prompt", "--db", "tests/fixtures/missing-optional-payloads.memory.jsonl", "--q", "payload", "--top", "10")
        Golden = "missing-optional-payloads.prompt.txt"
    }
)

Push-Location $memoryCtlDir
try {
    Write-Host "[build] memoryctl ($Configuration)"
    & dotnet build --configuration $Configuration | Out-Host
    if ($LASTEXITCODE -ne 0) {
        throw "dotnet build failed with exit code $LASTEXITCODE"
    }

    foreach ($case in $cases) {
        $actualPath = Join-Path $tempDir $case.Golden
        $actualDir = Split-Path -Parent $actualPath
        if (-not (Test-Path $actualDir)) {
            New-Item -ItemType Directory -Path $actualDir | Out-Null
        }

        Write-Host "[run] $($case.Name)"
        $output = & dotnet run --configuration $Configuration --no-build -- @($case.Command) 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-Host $output
            throw "Command failed for case '$($case.Name)' with exit code $LASTEXITCODE"
        }

        [System.IO.File]::WriteAllText($actualPath, ($output -join [Environment]::NewLine) + [Environment]::NewLine, (New-Object System.Text.UTF8Encoding($false)))

        $goldenPath = Join-Path $goldenDir $case.Golden
        & git --no-pager diff --no-index --exit-code -- $goldenPath $actualPath
        if ($LASTEXITCODE -ne 0) {
            throw "Behavior drift detected for '$($case.Name)'. See diff above."
        }
    }

    $exportActualPath = Join-Path $tempDir "duplicate-link-attempts.export-graph.json"
    Write-Host "[run] duplicate-link-attempts.export-graph"
    & dotnet run --configuration $Configuration --no-build -- export-graph --db tests/fixtures/duplicate-link-attempts.memory.jsonl --out $exportActualPath
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed for case 'duplicate-link-attempts.export-graph' with exit code $LASTEXITCODE"
    }

    $exportGoldenPath = Join-Path $goldenDir "duplicate-link-attempts.export-graph.json"
    & git --no-pager diff --no-index --exit-code -- $exportGoldenPath $exportActualPath
    if ($LASTEXITCODE -ne 0) {
        throw "Behavior drift detected for 'duplicate-link-attempts.export-graph'. See diff above."
    }

    Write-Host "Characterization checks passed."
}
finally {
    Pop-Location
    if (Test-Path $tempDir) {
        Remove-Item -Recurse -Force $tempDir
    }
}
