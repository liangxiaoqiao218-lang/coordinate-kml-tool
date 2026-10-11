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
  Assert-Case 'actual client helpers bind the same controlled visitor to claim header and multipart body' {
    $visitorId = 'controlled-rc-visitor-offline'
    $definition = [ordered]@{ caseId = 'mgrs'; imageSha256 = ('a' * 64) }
    $claimBody = New-CoordinateThreeCaseClaimBody -RunId 'run-offline' -BatchId 'batch-offline' `
      -Definition $definition -RecognitionRequestId '11111111-1111-4111-8111-111111111111' -VisitorId $visitorId
    Require ($claimBody.visitorId -eq $visitorId) 'CLAIM_VISITOR_NOT_BOUND'
    $httpClient = [Net.Http.HttpClient]::new()
    $form = [Net.Http.MultipartFormDataContent]::new()
    try {
      $bound = Add-CoordinateThreeCaseJobVisitorIdentity -Client $httpClient -Form $form -VisitorId $visitorId
      Require ($bound -eq $visitorId) 'JOB_VISITOR_BINDING_RESULT_MISMATCH'
      Require (($httpClient.DefaultRequestHeaders.GetValues('x-visitor-id') | Select-Object -First 1) -eq $visitorId) 'JOB_VISITOR_HEADER_MISSING'
      $multipartText = $form.ReadAsStringAsync().GetAwaiter().GetResult()
      Require ($multipartText.Contains('name=visitorId')) 'JOB_VISITOR_FORM_FIELD_MISSING'
      Require ($multipartText.Contains($visitorId)) 'JOB_VISITOR_FORM_VALUE_MISSING'
    } finally {
      $form.Dispose()
      $httpClient.Dispose()
    }
  }
  Assert-Case 'actual client helper rejects an empty controlled visitor before network' {
    $caught = ''
    try { Resolve-CoordinateThreeCaseControlledVisitorId -VisitorId '   ' | Out-Null }
    catch { $caught = [string]$_.Exception.Message }
    Require ($caught -eq 'CONTROLLED_VISITOR_ID_INVALID') 'EMPTY_VISITOR_NOT_REJECTED'
  }
  Assert-Case 'browser consumer evidence uses the actual strict client whitelist writer' {
    $evidencePath = Join-Path $tempRoot 'browser-consumer.json'
    $evidence = [pscustomobject][ordered]@{
      schemaVersion = 'coordinate_three_case_rc_r7_browser_evidence_v1'
      runId = 'coordinate-three-case-rc-20261011-r7'
      caseId = 'indonesia'
      bindingDigest = ('a' * 64)
      resultInstalled = $true
      resultIdentityMatch = $true
      mapState = 'READY'
      mapProvider = 'OPENFREEMAP'
      mapIdentityMatch = $true
      authorityMutationCount = 0
      kmlDownloadTriggered = $true
      kmlIdentityMatch = $true
      downloadFileName = 'coordinates.kml'
    }
    $written = Write-CoordinateThreeCaseBrowserConsumerEvidenceArtifact -Evidence $evidence `
      -Path $evidencePath -ExpectedRunId $evidence.runId -ExpectedCaseId $evidence.caseId
    Require (Test-Path -LiteralPath $written.path -PathType Leaf) 'BROWSER_EVIDENCE_NOT_WRITTEN'
    Require ([string]$written.sha256 -match '^[a-f0-9]{64}$') 'BROWSER_EVIDENCE_HASH_MISSING'
    $roundTrip = Get-Content -LiteralPath $written.path -Raw | ConvertFrom-Json
    Require ($roundTrip.bindingDigest -eq $evidence.bindingDigest) 'BROWSER_EVIDENCE_ROUND_TRIP_MISMATCH'

    $evidence | Add-Member -NotePropertyName rawProviderResponse -NotePropertyValue 'forbidden'
    $caught = ''
    try {
      Write-CoordinateThreeCaseBrowserConsumerEvidenceArtifact -Evidence $evidence `
        -Path (Join-Path $tempRoot 'rejected.json') -ExpectedRunId $evidence.runId -ExpectedCaseId $evidence.caseId | Out-Null
    } catch { $caught = [string]$_.Exception.Message }
    Require ($caught -eq 'R7_BROWSER_CONSUMER_EVIDENCE_SCHEMA_REJECTED') 'EXTRA_BROWSER_EVIDENCE_FIELD_NOT_REJECTED'
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
  Assert-Case 'Indonesia consumer gate must pass before MGRS begins' {
    $definitions = @(
      [ordered]@{ caseId = 'indonesia' },
      [ordered]@{ caseId = 'mgrs' },
      [ordered]@{ caseId = 'bftm' }
    )
    $counters = [pscustomobject]@{ Invoked = 0; Consumer = 0; Closed = 0 }
    $result = Invoke-CoordinateThreeCaseControlledRun -Definitions $definitions `
      -BeforeCases { } `
      -InvokeCase { param($definition) $counters.Invoked += 1; return [ordered]@{ status = 'SUCCEEDED' } } `
      -ConvertReceipt { param($definition, $snapshot) return [ordered]@{ caseId = $definition.caseId; terminalStatus = 'SUCCEEDED'; success = $true } } `
      -AfterCase { param($definition, $snapshot, $receipt) $counters.Consumer += 1; return $false } `
      -CloseRun { $counters.Closed += 1; return [ordered]@{ closed = $true } } `
      -WriteEvidence { param($r, $c, $f, $ready) return [ordered]@{ runClosed = $c.closed; failure = $f } }
    Require ($counters.Invoked -eq 1) 'CONSUMER_FAILURE_INVOKED_MGRS'
    Require ($counters.Consumer -eq 1) 'CONSUMER_GATE_COUNT_MISMATCH'
    Require ($counters.Closed -eq 1) 'CONSUMER_FAILURE_DID_NOT_CLOSE_ONCE'
    Require ($result.failureCode -eq 'CASE_CONSUMER_GATE_FAILED') 'CONSUMER_FAILURE_CODE_MISMATCH'
  }
  Assert-Case 'close and evidence settlement remain after execution' {
    Require ($clientText.Contains('/api/admin/coordinate-products/three-case-rc/close')) 'CLOSE_ROUTE_MISSING'
    Require ($clientText.Contains('runClosed = $sequenceCloseReceipt.closed -eq $true')) 'CLOSE_EVIDENCE_MISSING'
    Require ($clientText.Contains('Set-Content -LiteralPath $resolvedEvidencePath')) 'EVIDENCE_WRITE_MISSING'
    Require ($clientText.Contains('/api/admin/coordinate-products/three-case-rc/browser-handoff/register')) 'R7_HANDOFF_REGISTRATION_MISSING'
    Require ($clientText.Contains('/api/admin/coordinate-products/three-case-rc/browser-evidence')) 'R7_BROWSER_EVIDENCE_POLL_MISSING'
    Require ($clientText.Contains('Write-CoordinateThreeCaseBrowserConsumerEvidenceArtifact')) 'R7_BROWSER_EVIDENCE_WHITELIST_WRITER_MISSING'
    Require ($clientText.Contains('coordinate-three-case-rc-r7-browser-consumer-gate.ps1')) 'R7_BROWSER_CONSUMER_GATE_MISSING'
    Require ($clientText.Contains('[DateTimeOffset]::TryParse($handoffExpiresAt')) 'R7_HANDOFF_RETURNED_DEADLINE_NOT_PARSED'
    Require ($clientText.Contains('-TimeoutMilliseconds ([Math]::Min(30000, $remainingRequestMilliseconds))')) 'R7_BROWSER_POLL_NOT_REQUEST_BOUNDED'
    Require ($clientText.Contains('-HandoffExpiresAt $handoffExpiresAt')) 'R7_GATE_NOT_BOUND_TO_HANDOFF_DEADLINE'
  }
  Assert-Case 'fresh browser download directory is created and disclosed before the password prompt' {
    $directoryIndex = $clientText.IndexOf("New-Item -ItemType Directory -Path `$browserDownloadDirectory")
    $disclosureIndex = $clientText.IndexOf('R7_BROWSER_DOWNLOAD_DIRECTORY=')
    $passwordIndex = $clientText.IndexOf("Read-Host '请输入 RC 管理员密码（不会显示）'")
    Require ($directoryIndex -ge 0 -and $disclosureIndex -gt $directoryIndex -and $passwordIndex -gt $disclosureIndex) `
      'R7_DOWNLOAD_DIRECTORY_NOT_READY_BEFORE_PASSWORD'
    Require ($clientText.Contains("(Test-Path -LiteralPath `$browserDownloadDirectory)")) 'R7_DOWNLOAD_DIRECTORY_STALE_GUARD_MISSING'
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
