param(
  [string]$BaselinePath = "tools/memoryctl/tests/performance/command-performance-baseline.json"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptDir "..\..\..\..")
$repoRootPath = $repoRoot.Path
$projectPath = Join-Path $repoRootPath "tools\memoryctl\MemoryCtl.csproj"
$resolvedBaselinePath = Join-Path $repoRootPath $BaselinePath

if (-not (Test-Path $resolvedBaselinePath)) {
  throw "Baseline file not found: $resolvedBaselinePath"
}

$baseline = Get-Content -Raw $resolvedBaselinePath | ConvertFrom-Json

Write-Host "Building memoryctl (Release)..."
& dotnet build $projectPath -c Release | Out-Host
if ($LASTEXITCODE -ne 0) {
  throw "dotnet build failed with exit code $LASTEXITCODE"
}

function Invoke-MemoryCtlCommand {
  param(
    [string[]]$Arguments,
    [string]$WorkingDirectory
  )

  $sw = [System.Diagnostics.Stopwatch]::StartNew()
  & dotnet run --project $projectPath -c Release --no-build -- @Arguments 1>$null
  $exitCode = $LASTEXITCODE
  $sw.Stop()

  return [pscustomobject]@{
    ExitCode = $exitCode
    ElapsedMs = [math]::Round($sw.Elapsed.TotalMilliseconds, 2)
  }
}

function Get-Median {
  param([double[]]$Values)

  $sorted = $Values | Sort-Object
  $count = $sorted.Count
  if ($count -eq 0) { return 0 }

  if ($count % 2 -eq 1) {
    return [double]$sorted[[int][math]::Floor($count / 2)]
  }

  $upper = [double]$sorted[$count / 2]
  $lower = [double]$sorted[($count / 2) - 1]
  return ($lower + $upper) / 2
}

$failures = New-Object System.Collections.Generic.List[string]
$results = New-Object System.Collections.Generic.List[object]

foreach ($commandName in @('validate','query','prompt','add')) {
  if (-not $baseline.commands.PSObject.Properties.Name.Contains($commandName)) {
    $failures.Add("Missing baseline entry for command '$commandName'.")
    continue
  }

  $entry = $baseline.commands.$commandName
  $fixtureRelative = [string]$entry.fixture
  $fixturePath = Join-Path $repoRootPath $fixtureRelative

  if (-not (Test-Path $fixturePath)) {
    $failures.Add("Fixture does not exist for '$commandName': $fixtureRelative")
    continue
  }

  $measurements = New-Object System.Collections.Generic.List[double]
  $iterations = 3

  for ($i = 1; $i -le $iterations; $i++) {
    $tempDbPath = Join-Path $env:TEMP ("memoryctl-perf-{0}-{1}-{2}.memory.jsonl" -f $commandName, [guid]::NewGuid().ToString('N'), $i)
    Copy-Item -Path $fixturePath -Destination $tempDbPath -Force

    try {
      switch ($commandName) {
        'validate' {
          $args = @('validate', '--db', $tempDbPath)
        }
        'query' {
          $args = @('query', '--db', $tempDbPath, '--q', 'memory graph refactor', '--top', '10', '--explain')
        }
        'prompt' {
          $args = @('prompt', '--db', $tempDbPath, '--q', 'memory graph refactor', '--top', '12')
        }
        'add' {
          $args = @('add', '--db', $tempDbPath, '--title', "Perf Guardrail Card $i", '--text', 'Performance regression guardrail write test payload.', '--memAnchor', 'Topic: Perf Guardrails', '--source', 'perf-check')
        }
        default {
          throw "Unsupported command '$commandName'"
        }
      }

      if ($i -eq 1) {
        $warmup = Invoke-MemoryCtlCommand -Arguments $args -WorkingDirectory $repoRootPath
        if ($warmup.ExitCode -ne 0) {
          $failures.Add("Warmup run failed for '$commandName' with exit code $($warmup.ExitCode).")
          continue
        }
      }

      $run = Invoke-MemoryCtlCommand -Arguments $args -WorkingDirectory $repoRootPath
      if ($run.ExitCode -ne 0) {
        $failures.Add("Measured run failed for '$commandName' with exit code $($run.ExitCode).")
        continue
      }

      $measurements.Add([double]$run.ElapsedMs)
    }
    finally {
      if (Test-Path $tempDbPath) {
        Remove-Item $tempDbPath -Force -ErrorAction SilentlyContinue
      }
    }
  }

  if ($measurements.Count -eq 0) {
    $failures.Add("No measurements captured for '$commandName'.")
    continue
  }

  $median = [math]::Round((Get-Median -Values $measurements.ToArray()), 2)
  $maxAllowed = [double]$entry.maxAllowedMedianMs
  $baselineMedian = [double]$entry.baselineMedianMs

  $results.Add([pscustomobject]@{
    Command = $commandName
    BaselineMedianMs = $baselineMedian
    MaxAllowedMedianMs = $maxAllowed
    MeasuredMedianMs = $median
    SamplesMs = ($measurements | ForEach-Object { [math]::Round($_, 2) }) -join ', '
  })

  if ($median -gt $maxAllowed) {
    $failures.Add("$commandName median ${median}ms exceeded max allowed ${maxAllowed}ms (baseline ${baselineMedian}ms).")
  }
}

Write-Host ""
Write-Host "Command performance regression summary"
$results | Sort-Object Command | Format-Table -AutoSize | Out-Host

if ($failures.Count -gt 0) {
  Write-Host ""
  Write-Host "Performance regression check failures:" -ForegroundColor Red
  foreach ($failure in $failures) {
    Write-Host "- $failure" -ForegroundColor Red
  }
  exit 1
}

Write-Host ""
Write-Host "All command-level performance checks passed." -ForegroundColor Green
exit 0
