param([switch] $R8Incremental, [string] $R8CaseFilter='')
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'coordinate-three-case-rc-client-preflight.ps1')

$passed = 0
$failed = 0
$results = [Collections.Generic.List[object]]::new()
function Assert-Case([string] $name, [scriptblock] $body) {
  if ($R8Incremental -and $R8CaseFilter -and $name -notmatch $R8CaseFilter) { return }
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

if ($R8Incremental) {
  . (Join-Path $PSScriptRoot 'coordinate-three-case-rc-client.ps1') -LibraryOnly
  $fixedNow=[DateTimeOffset]::UtcNow
  $commit='a'*40
  $testRunId=$script:ReplayRunId
  function Invoke-R8EntryCase([string] $FailureAt='') {
    $trace=[Collections.Generic.List[string]]::new()
    $context=New-ReplayClientStateContext 'unused-offline' $commit 999 $fixedNow ('b'*32)
    $wait={ $trace.Add('release'); if ($FailureAt -eq 'release') { throw 'OFFLINE_DENIED' } }.GetNewClosure()
    $version={
      $trace.Add('version'); if ($FailureAt -eq 'version') { throw 'OFFLINE_DENIED' }
      return @{runtimeIdentity=@{commit=$commit;coordinateThreeCaseRc=@{enabled=$true;runId=$testRunId};coordinateConsumerReplay=@{enabled=$false}}}
    }.GetNewClosure()
    $read={ $trace.Add('password'); if ($FailureAt -eq 'password') { throw 'OFFLINE_DENIED' }; ConvertTo-SecureString 'offline-only' -AsPlainText -Force }.GetNewClosure()
    $protocol={ param($secure,$receipt,$deadline,$transition)
      $trace.Add('protocol'); & $transition 'CLOSING'; $receipt.runClosed=$true; $receipt.success=$true
    }.GetNewClosure()
    $receipt=Invoke-R8ClientEntrypoint -StateContext $context -Commit $commit -WaitForRelease $wait -ReadVersion $version -FlushInput {$trace.Add('flush')}.GetNewClosure() -ReadPassword $read -RunProtocol $protocol -PersistState {param($p,$v,$n)} -Now { $fixedNow }.GetNewClosure()
    return @{receipt=$receipt;trace=@($trace);context=$context}
  }
  Assert-Case 'R8 entry orders release runtime flush password protocol with original 15m deadline' {
    $r=Invoke-R8EntryCase
    Require ($r.receipt.success -and (($r.trace -join ',') -eq 'release,version,flush,password,protocol')) 'R8_ORDER_FAILED'
    Require ($r.context.writeCount -eq 7 -and $r.context.deadline -eq $fixedNow.AddMinutes(15)) 'R8_DEADLINE_CHANGED'
  }
  Assert-Case 'R8 release failure cannot read runtime password or execute' {
    $r=Invoke-R8EntryCase 'release'
    Require (-not $r.receipt.success -and (($r.trace -join ',') -eq 'release') -and $r.context.phase -eq 'FAILED') 'R8_RELEASE_FAILED_OPEN'
  }
  Assert-Case 'R8 runtime failure cannot request password or execute' {
    $r=Invoke-R8EntryCase 'version'
    Require (-not $r.receipt.success -and (($r.trace -join ',') -eq 'release,version')) 'R8_RUNTIME_FAILED_OPEN'
  }
  Assert-Case 'R8 password failure cannot release upload' {
    $r=Invoke-R8EntryCase 'password'
    Require (-not $r.receipt.success -and $r.trace -notcontains 'protocol') 'R8_PASSWORD_FAILED_OPEN'
  }
  Assert-Case 'R8 readiness failure closes once with no upload release' {
    $receipt=[ordered]@{uploadReleaseCount=0;closeRequestCount=0;runClosed=$false;success=$false}
    $trace=[Collections.Generic.List[string]]::new()
    $request={param($method,$path,$body,$timeout)
      $trace.Add($path)
      if ($path.EndsWith('/browser-readiness')) { return @{browserSessionReady=$false} }
      if ($path.EndsWith('/close')) {return @{closed=$true}}
      throw 'UNEXPECTED_REQUEST'
    }.GetNewClosure()
    try { Invoke-R8FreshConsumerProtocol -Request $request -Receipt $receipt -Deadline $fixedNow.AddMinutes(10) -Transition {} -Now {$fixedNow}.GetNewClosure() } catch {}
    Require ($receipt.uploadReleaseCount -eq 0 -and $receipt.closeRequestCount -eq 1 -and $receipt.runClosed -and $trace.Count -eq 2) 'R8_CLOSE_COUNT_FAILED'
  }
  Assert-Case 'R8 failed fresh recognition stops before consumers and closes without retry' {
    $receipt=[ordered]@{uploadReleaseCount=0;closeRequestCount=0;runClosed=$false;success=$false}
    $trace=[Collections.Generic.List[string]]::new()
    $request={param($method,$path,$body,$timeout)
      $trace.Add($path)
      if ($path.EndsWith('/browser-readiness')) {return @{browserSessionReady=$true}}
      if ($path.EndsWith('/release-upload')) {return @{released=$true;runId=$testRunId}}
      if ($path.EndsWith('/fresh-result')) {return @{ready=$false;stage='FAILED'}}
      if ($path.EndsWith('/close')) {throw 'OFFLINE_CLOSE_FAILURE'}
      throw 'UNEXPECTED_REQUEST'
    }.GetNewClosure()
    try { Invoke-R8FreshConsumerProtocol -Request $request -Receipt $receipt -Deadline $fixedNow.AddMinutes(10) -Transition {} -Now {$fixedNow}.GetNewClosure() } catch {}
    Require ($receipt.uploadReleaseCount -eq 1 -and $receipt.closeRequestCount -eq 1 -and -not $receipt.runClosed -and $trace.Count -eq 4) 'R8_FAILURE_RETRIED'
    Require ($receipt.failureStage -eq 'FRESH_RECOGNITION') 'R8_CLOSE_MASKED_ORIGINAL_FAILURE'
  }
  Assert-Case 'R8 new protocol retains its own result then actual physical gate and closes once' {
    $tempRoot=Join-Path ([IO.Path]::GetTempPath()) ('r8-protocol-'+[guid]::NewGuid().ToString('N'))
    New-Item -ItemType Directory -Path $tempRoot | Out-Null
    try {
      $resultPath=Join-Path $tempRoot 'result.json'; $browserPath=Join-Path $tempRoot 'browser.json'; $downloadPath=Join-Path $tempRoot 'coordinates.kml'
      $requestId='11111111-1111-4111-8111-111111111111'
      $finalized=@{schemaVersion='finalized_coordinate_result_v1';resultId='offline-r8-fresh';resultRevision=1;geometryHash=('sha256:'+('a'*64));
        technicalKmlReady=$true;crs=@{id='EPSG:4326';axisOrder='longitude_latitude'};
        geometry=@{type='Polygon';coordinates=@(,@(@(1,2),@(3,2),@(3,4),@(1,2)))}}
      $capabilities=@{resultId=$finalized.resultId;resultRevision=1;geometryHash=$finalized.geometryHash;technicallyGeneratable=$true;mapReady=$true;kmlReady=$true}
      $snapshot=@{requestId=$requestId;status='SUCCEEDED';httpStatus=200;result=@{success=$true;providerModel='qwen3.8-flash';model='qwen3.8-flash';providerCallCount=1;recognitionUsageConsumed=$true;finalizedCoordinateResult=$finalized;outputCapabilities=$capabilities}}
      $evidence=@{schemaVersion='coordinate_three_case_rc_r7_browser_evidence_v1';runId=$testRunId;caseId='indonesia';
        bindingDigest=(Get-R7IdentityBindingDigest $testRunId 'indonesia' $requestId $finalized);
        resultInstalled=$true;resultIdentityMatch=$true;mapState='READY';mapProvider='OPENFREEMAP';mapIdentityMatch=$true;
        authorityMutationCount=0;kmlDownloadTriggered=$true;kmlIdentityMatch=$true;downloadFileName='coordinates.kml'}
      # Explicit synthetic download fixture, never a real browser-download claim.
      [IO.File]::WriteAllText($downloadPath,'<kml xmlns="http://www.opengis.net/kml/2.2"><Document><Placemark><Polygon><outerBoundaryIs><LinearRing><coordinates>1,2,0 3,2,0 3,4,0 1,2,0</coordinates></LinearRing></outerBoundaryIs></Polygon></Placemark></Document></kml>')
      $trace=[Collections.Generic.List[string]]::new()
      $receipt=[ordered]@{uploadReleaseCount=0;consumerGateCount=0;closeRequestCount=0;runClosed=$false;success=$false;failureStage=''}
      $request={param($method,$path,$body,$timeout)
        $trace.Add($path)
        if($path.EndsWith('/browser-readiness')){return @{browserSessionReady=$true}}
        if($path.EndsWith('/release-upload')){return @{released=$true;runId=$testRunId}}
        if($path.EndsWith('/fresh-result')){return @{ready=$true;expiresAt=$fixedNow.AddMinutes(5).ToString('O');snapshot=$snapshot}}
        if($path.EndsWith('/browser-evidence')){return @{ready=$true;evidence=$evidence}}
        if($path.EndsWith('/close')){return @{closed=$true}}
        throw 'UNEXPECTED_REQUEST'
      }.GetNewClosure()
      Invoke-R8FreshConsumerProtocol -Request $request -Receipt $receipt -Deadline $fixedNow.AddMinutes(10) -Transition {} -Commit $commit -ResultPath $resultPath -BrowserPath $browserPath -DownloadPath $downloadPath
      Require ($receipt.success -and $receipt.runClosed -and $receipt.consumerGateCount -eq 1 -and $receipt.closeRequestCount -eq 1 -and $trace.Count -eq 5) 'R8_PROTOCOL_NOT_COMPLETE'
      $stored=Get-Content -LiteralPath $resultPath -Raw | ConvertFrom-Json
      Require ($stored.runId -eq $testRunId -and $stored.cases[0].recognitionRequestId -eq $requestId -and $stored.cases[0].finalizedResult.consumerIdentityMatch) 'R8_FRESH_IDENTITY_LOST'
      Require ($receipt.consumer.kmlGeometryMatch -and $receipt.consumer.kmlSha256 -match '^[a-f0-9]{64}$') 'R8_PHYSICAL_CHECK_MISSING'
    } finally {
      $resolvedTemp=[IO.Path]::GetFullPath($tempRoot)
      if(-not $resolvedTemp.StartsWith([IO.Path]::GetFullPath([IO.Path]::GetTempPath()),[StringComparison]::OrdinalIgnoreCase)){throw 'TEMP_PATH_REJECTED'}
      Remove-Item -LiteralPath $resolvedTemp -Recurse -Force
    }
  }
  [ordered]@{suite='R8 incremental client entry';passed=$passed;failed=$failed;cases=@($results);networkCalls=0;realModelCalls=0;clientLaunches=0} | ConvertTo-Json -Depth 8
  if ($failed) {exit 1}; exit 0
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
