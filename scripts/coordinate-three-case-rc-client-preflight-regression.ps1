$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'coordinate-three-case-rc-client-preflight.ps1')

$passed = 0
$failed = 0
$results = [Collections.Generic.List[object]]::new()
function Assert-Case([string] $name, [scriptblock] $body) {
  try {
    & $body
    $script:passed += 1
    $script:results.Add([ordered]@{ name = $name; status = 'PASS' })
  } catch {
    $script:failed += 1
    $script:results.Add([ordered]@{ name = $name; status = 'FAIL'; code = [string]$_.Exception.Message })
  }
}
function Require([bool] $condition, [string] $code) {
  if (-not $condition) { throw $code }
}

$tempRoot = Join-Path ([IO.Path]::GetTempPath()) ("coordinate-three-case-client-preflight-" + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $tempRoot | Out-Null
try {
  $paths = @(
    (Join-Path $tempRoot 'first.bin'),
    (Join-Path $tempRoot 'second.bin'),
    (Join-Path $tempRoot 'third.bin')
  )
  [IO.File]::WriteAllBytes($paths[0], [byte[]](1, 2, 3))
  [IO.File]::WriteAllBytes($paths[1], [byte[]](4, 5, 6))
  [IO.File]::WriteAllBytes($paths[2], [byte[]](7, 8, 9))
  $hashes = @($paths | ForEach-Object { (Get-FileHash -LiteralPath $_ -Algorithm SHA256).Hash.ToLowerInvariant() })

  Assert-Case 'prompted Indonesia path is returned on the original ordered dictionary' {
    $definitions = @(
      [ordered]@{ caseId = 'indonesia'; imageSha256 = $hashes[0]; path = '' },
      [ordered]@{ caseId = 'mgrs'; imageSha256 = $hashes[1]; path = $paths[1] },
      [ordered]@{ caseId = 'bftm'; imageSha256 = $hashes[2]; path = $paths[2] }
    )
    $promptCounter = [pscustomobject]@{ Count = 0 }
    $resolved = @(Resolve-CoordinateThreeCaseLocalInputs -Definitions $definitions -PromptForIndonesiaPath {
      $promptCounter.Count += 1
      return $paths[0]
    })
    Require ($promptCounter.Count -eq 1) 'PROMPT_COUNT_MISMATCH'
    Require ($resolved.Count -eq 3) 'RESOLVED_COUNT_MISMATCH'
    Require ($definitions[0].path -eq (Resolve-Path -LiteralPath $paths[0]).Path) 'INDONESIA_PATH_NOT_WRITTEN_BACK'
    Require ($definitions[1].path -eq (Resolve-Path -LiteralPath $paths[1]).Path) 'MGRS_PATH_NOT_NORMALIZED'
    Require ($definitions[2].path -eq (Resolve-Path -LiteralPath $paths[2]).Path) 'BFTM_PATH_NOT_NORMALIZED'
  }

  Assert-Case 'failure in the second input stops before the third input is mutated' {
    $thirdOriginal = '.\third.bin'
    $definitions = @(
      [ordered]@{ caseId = 'indonesia'; imageSha256 = $hashes[0]; path = $paths[0] },
      [ordered]@{ caseId = 'mgrs'; imageSha256 = $hashes[1]; path = (Join-Path $tempRoot 'missing.bin') },
      [ordered]@{ caseId = 'bftm'; imageSha256 = $hashes[2]; path = $thirdOriginal }
    )
    $caught = ''
    try { Resolve-CoordinateThreeCaseLocalInputs -Definitions $definitions | Out-Null }
    catch { $caught = [string]$_.Exception.Message }
    Require ($caught -eq 'CASE_IMAGE_PATH_UNREADABLE_MGRS') 'SECOND_FAILURE_CODE_MISMATCH'
    Require ($definitions[2].path -eq $thirdOriginal) 'THIRD_INPUT_WAS_TOUCHED'
  }

  $clientPath = Join-Path $PSScriptRoot 'coordinate-three-case-rc-client.ps1'
  $clientText = Get-Content -LiteralPath $clientPath -Raw
  Assert-Case 'all local inputs are resolved before the password prompt' {
    $preflightIndex = $clientText.IndexOf('Resolve-CoordinateThreeCaseLocalInputs')
    $passwordIndex = $clientText.IndexOf("Read-Host '请输入 RC 管理员密码（不会显示）'")
    Require ($preflightIndex -ge 0 -and $passwordIndex -gt $preflightIndex) 'PASSWORD_PRECEDES_LOCAL_PREFLIGHT'
  }
  Assert-Case 'case failure is recorded and stops later cases' {
    $definitions = @(
      [ordered]@{ caseId = 'indonesia' },
      [ordered]@{ caseId = 'mgrs' },
      [ordered]@{ caseId = 'bftm' }
    )
    $counters = [pscustomobject]@{ Invoked = 0; Closed = 0; Evidence = 0 }
    $result = Invoke-CoordinateThreeCaseControlledRun -Definitions $definitions `
      -BeforeCases { } `
      -InvokeCase { param($definition) $counters.Invoked += 1; throw 'SIMULATED_CASE_FAILURE' } `
      -ConvertReceipt { throw 'UNREACHABLE_RECEIPT_CONVERSION' } `
      -CloseRun { $counters.Closed += 1; return [ordered]@{ closed = $true } } `
      -WriteEvidence { param($r, $c, $f, $ready) $counters.Evidence += 1; return [ordered]@{ runClosed = $c.closed; failure = $f } }
    Require ($counters.Invoked -eq 1) 'THROW_PATH_INVOKED_LATER_CASE'
    Require ($counters.Closed -eq 1) 'THROW_PATH_DID_NOT_CLOSE_ONCE'
    Require ($counters.Evidence -eq 1) 'THROW_PATH_DID_NOT_WRITE_EVIDENCE_ONCE'
    Require ($result.receipts.Count -eq 1) 'THROW_PATH_RECEIPT_COUNT_MISMATCH'
    Require ($result.failureCode -eq 'SIMULATED_CASE_FAILURE') 'THROW_PATH_FAILURE_CODE_MISMATCH'
  }
  Assert-Case 'a returned FAILED job blocks the next case without relying on an exception' {
    $definitions = @(
      [ordered]@{ caseId = 'indonesia' },
      [ordered]@{ caseId = 'mgrs' },
      [ordered]@{ caseId = 'bftm' }
    )
    $counters = [pscustomobject]@{ Invoked = 0; Closed = 0; Evidence = 0 }
    $result = Invoke-CoordinateThreeCaseControlledRun -Definitions $definitions `
      -BeforeCases { } `
      -InvokeCase { param($definition) $counters.Invoked += 1; return [ordered]@{ status = 'FAILED' } } `
      -ConvertReceipt { param($definition, $snapshot) return [ordered]@{ caseId = $definition.caseId; terminalStatus = 'FAILED'; success = $false } } `
      -CloseRun { $counters.Closed += 1; return [ordered]@{ closed = $true } } `
      -WriteEvidence { param($r, $c, $f, $ready) $counters.Evidence += 1; return [ordered]@{ runClosed = $c.closed; failure = $f } }
    Require ($counters.Invoked -eq 1) 'FAILED_RETURN_INVOKED_LATER_CASE'
    Require ($counters.Closed -eq 1) 'FAILED_RETURN_DID_NOT_CLOSE_ONCE'
    Require ($counters.Evidence -eq 1) 'FAILED_RETURN_DID_NOT_WRITE_EVIDENCE_ONCE'
    Require ($result.receipts.Count -eq 1) 'FAILED_RETURN_RECEIPT_COUNT_MISMATCH'
    Require ($result.failureCode -eq 'CASE_TERMINAL_FAILURE') 'FAILED_RETURN_FAILURE_CODE_MISMATCH'
  }
  Assert-Case 'close and evidence settlement remain after execution' {
    Require ($clientText.Contains('/api/admin/coordinate-products/three-case-rc/close')) 'CLOSE_ROUTE_MISSING'
    Require ($clientText.Contains('runClosed = $sequenceCloseReceipt.closed -eq $true')) 'CLOSE_EVIDENCE_MISSING'
    Require ($clientText.Contains('Set-Content -LiteralPath $resolvedEvidencePath')) 'EVIDENCE_WRITE_MISSING'
  }
} finally {
  $resolvedTempRoot = (Resolve-Path -LiteralPath $tempRoot -ErrorAction Stop).Path
  $expectedTempRoot = [IO.Path]::GetFullPath([IO.Path]::GetTempPath()).TrimEnd('\') + '\'
  if (-not $resolvedTempRoot.StartsWith($expectedTempRoot, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'TEMP_CLEANUP_PATH_REJECTED'
  }
  Remove-Item -LiteralPath $resolvedTempRoot -Recurse -Force
}

$summary = [ordered]@{
  suite = 'COORDINATE_THREE_CASE_RC_CLIENT_PREFLIGHT'
  passed = $passed
  failed = $failed
  total = $passed + $failed
  providerCalls = 0
  httpRequests = 0
  cases = $results
}
$summary | ConvertTo-Json -Depth 6
if ($failed -gt 0) { exit 1 }
