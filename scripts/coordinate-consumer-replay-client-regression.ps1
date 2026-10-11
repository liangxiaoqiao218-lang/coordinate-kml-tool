param(
  [Parameter(Mandatory = $true)] [string] $HistoricalArtifactPath,
  [switch] $R2Incremental,
  [string] $CaseFilter
)

$ErrorActionPreference = 'Stop'
$regressionHistoricalArtifactPath = $HistoricalArtifactPath
. (Join-Path $PSScriptRoot 'coordinate-consumer-replay-client.ps1') -LibraryOnly
$HistoricalArtifactPath = $regressionHistoricalArtifactPath
$cases = [Collections.Generic.List[object]]::new()
function Add-Case([string] $Name, [scriptblock] $Body) {
  if ($R2Incremental -and -not $Name.StartsWith('R2 ')) { return }
  if ($CaseFilter -and $Name -notmatch $CaseFilter) { return }
  try { & $Body; $cases.Add([ordered]@{ name = $Name; pass = $true }) }
  catch { $cases.Add([ordered]@{ name = $Name; pass = $false; code = [string]$_.Exception.Message }) }
}

$root = Join-Path ([IO.Path]::GetTempPath()) ("consumer-replay-regression-" + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $root | Out-Null
try {
  $source = Test-ReplaySourceArtifact -Path $HistoricalArtifactPath
  $artifact = Get-Content -LiteralPath $source.Path -Raw | ConvertFrom-Json
  $finalized = @($artifact.cases | Where-Object { $_.caseId -eq 'indonesia' })[0].finalizedResult
  $coordinates = @($finalized.geometry.coordinates[0] | ForEach-Object {
    ([double]$_[0]).ToString('R', [Globalization.CultureInfo]::InvariantCulture) + ',' +
      ([double]$_[1]).ToString('R', [Globalization.CultureInfo]::InvariantCulture)
  }) -join ' '
  $kmlPath = Join-Path $root 'coordinates.kml'
  $kml = '<?xml version="1.0" encoding="UTF-8"?><kml xmlns="http://www.opengis.net/kml/2.2"><Document><Placemark><Polygon><outerBoundaryIs><LinearRing><coordinates>' +
    $coordinates + '</coordinates></LinearRing></outerBoundaryIs></Polygon></Placemark></Document></kml>'
  Set-Content -LiteralPath $kmlPath -Encoding UTF8 -Value $kml

  $bindingDigest = 'a' * 64
  $evidence = [pscustomobject]@{
    schemaVersion = 'coordinate_consumer_replay_browser_evidence_v1'
    runId = $script:ReplayRunId
    batchId = $script:ReplayBatchId
    caseId = 'indonesia'
    bindingDigest = $bindingDigest
    replayOnly = $true
    businessQuotaBypassed = $true
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
  $testExpiresAt = [DateTimeOffset]::UtcNow.AddMinutes(5).ToString('O')

  Add-Case 'exact source artifact preflight' {
    if ($source.Sha256 -ne $script:ReplayArtifactSha256 -or $source.Bytes.Length -le 0) { throw 'ARTIFACT_PREFLIGHT_FAILED' }
  }
  Add-Case 'wrong artifact hash fails closed' {
    $wrong = Join-Path $root 'wrong.json'
    Set-Content -LiteralPath $wrong -Encoding UTF8 -Value '{}'
    $caught = ''
    try { Test-ReplaySourceArtifact -Path $wrong | Out-Null } catch { $caught = [string]$_.Exception.Message }
    if ($caught -ne 'CONSUMER_REPLAY_SOURCE_ARTIFACT_HASH_MISMATCH') { throw 'WRONG_HASH_NOT_REJECTED' }
  }
  Add-Case 'physical KML parser matches preserved geometry' {
    $gate = Invoke-CoordinateConsumerReplayGate -EvidenceObject $evidence -ArtifactPath $source.Path `
      -DownloadPath $kmlPath -RunId $script:ReplayRunId -BatchId $script:ReplayBatchId `
      -ArtifactSha256 $script:ReplayArtifactSha256 -ExpectedBindingDigest $bindingDigest -ExpiresAt $testExpiresAt
    if ($gate.success -ne $true -or $gate.kmlGeometryMatch -ne $true -or $gate.providerCalls -ne 0 -or
        $gate.databaseWrites -ne 0 -or $gate.businessQuotaDeductions -ne 0) { throw 'KML_GATE_FAILED' }
  }
  Add-Case 'browser evidence must match the exact registration binding' {
    $caught = ''
    try {
      Invoke-CoordinateConsumerReplayGate -EvidenceObject $evidence -ArtifactPath $source.Path `
        -DownloadPath $kmlPath -RunId $script:ReplayRunId -BatchId $script:ReplayBatchId `
        -ArtifactSha256 $script:ReplayArtifactSha256 -ExpectedBindingDigest ('b' * 64) `
        -ExpiresAt $testExpiresAt | Out-Null
    } catch { $caught = [string]$_.Exception.Message }
    if ($caught -ne 'CONSUMER_REPLAY_BROWSER_EVIDENCE_REJECTED') { throw 'BINDING_MISMATCH_NOT_REJECTED' }
  }
  Add-Case 'actual client protocol completes once and closes once' {
    $requestCounts = @{}
    $expectedRunId = $script:ReplayRunId
    $expectedBatchId = $script:ReplayBatchId
    $expectedArtifactSha = $script:ReplayArtifactSha256
    $request = {
      param($method, $path, $body, $timeout)
      $requestCounts[$path] = 1 + [int]($requestCounts[$path])
      switch ($path) {
        '/api/version' { return [pscustomobject]@{ runtimeIdentity = [pscustomobject]@{
          commit = 'candidate'; coordinateConsumerReplay = [pscustomobject]@{ enabled = $true; runId = $expectedRunId } } } }
        '/api/admin/coordinate-consumer-replay/browser-readiness' { return [pscustomobject]@{ browserSessionReady = $true } }
        '/api/admin/coordinate-consumer-replay/register' { return [pscustomobject]@{
          registered = $true; runId = $expectedRunId; batchId = $expectedBatchId;
          caseId = 'indonesia'; expiresAt = $testExpiresAt; bindingDigest = $bindingDigest } }
        '/api/admin/coordinate-consumer-replay/evidence' { return [pscustomobject]@{
          ready = $true; lastStage = 'EVIDENCE_ACCEPTED'; evidence = $evidence } }
        '/api/admin/coordinate-consumer-replay/close' { return [pscustomobject]@{ closed = $true } }
        default { throw 'UNEXPECTED_REQUEST' }
      }
    }.GetNewClosure()
    $runGate = {
      param($browserEvidence, $artifactPath, $downloadPath, $deadline, $expectedBindingDigest)
      return Invoke-CoordinateConsumerReplayGate -EvidenceObject $browserEvidence -ArtifactPath $artifactPath `
        -DownloadPath $downloadPath -RunId $expectedRunId -BatchId $expectedBatchId `
        -ArtifactSha256 $expectedArtifactSha -ExpectedBindingDigest $expectedBindingDigest -ExpiresAt $deadline
    }.GetNewClosure()
    $receipt = Invoke-CoordinateConsumerReplayProtocol -Request $request -RunGate $runGate `
      -SourceArtifact $source -Deadline ([DateTimeOffset]$testExpiresAt) -KmlPath $kmlPath
    if ($receipt.success -ne $true -or $receipt.registrationCount -ne 1 -or
        $receipt.replayCloseRequestCount -ne 1 -or $receipt.mapOpenCount -ne 1 -or
        $receipt.kmlDownloadCount -ne 1 -or $receipt.providerCalls -ne 0 -or
        $receipt.databaseWrites -ne 0 -or $receipt.businessQuotaDeductions -ne 0) {
      throw ('CLIENT_PROTOCOL_FAILED_' + ($receipt | ConvertTo-Json -Depth 5 -Compress))
    }
    foreach ($path in @('/api/admin/coordinate-consumer-replay/register', '/api/admin/coordinate-consumer-replay/close')) {
      if ([int]$requestCounts[$path] -ne 1) { throw 'CLIENT_REQUEST_COUNT_INVALID' }
    }
  }
  Add-Case 'client source contains no recognition provider or database path' {
    $sourceText = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'coordinate-consumer-replay-client.ps1') -Raw
    if ($sourceText -match '/api/recognize-coordinates|three-case-rc/claims|supabase') { throw 'PROHIBITED_CLIENT_PATH_PRESENT' }
    if ($sourceText -notmatch 'Read-ReplayPassword -Deadline' -or $sourceText -notmatch 'automaticRetries = 0') {
      throw 'CLIENT_BOUNDARY_MISSING'
    }
  }
  Add-Case 'terminal browser failure stops polling and closes once' {
    $requestCounts = @{}
    $expectedRunId = $script:ReplayRunId
    $expectedBatchId = $script:ReplayBatchId
    $request = {
      param($method, $path, $body, $timeout)
      $requestCounts[$path] = 1 + [int]$requestCounts[$path]
      switch ($path) {
        '/api/version' { return [pscustomobject]@{ runtimeIdentity = [pscustomobject]@{
          commit = 'candidate'; coordinateConsumerReplay = [pscustomobject]@{ enabled = $true; runId = $expectedRunId } } } }
        '/api/admin/coordinate-consumer-replay/browser-readiness' { return [pscustomobject]@{ browserSessionReady = $true } }
        '/api/admin/coordinate-consumer-replay/register' { return [pscustomobject]@{
          registered = $true; runId = $expectedRunId; batchId = $expectedBatchId; caseId = 'indonesia';
          expiresAt = $testExpiresAt; bindingDigest = $bindingDigest } }
        '/api/admin/coordinate-consumer-replay/evidence' { return [pscustomobject]@{
          ready = $false; lastStage = 'MAP_FAILED'; failureCode = 'MAP_PRODUCT_PATH_FAILED' } }
        '/api/admin/coordinate-consumer-replay/close' { return [pscustomobject]@{ closed = $true } }
        default { throw 'PROHIBITED_OR_UNEXPECTED_REQUEST' }
      }
    }.GetNewClosure()
    $neverGate = { throw 'GATE_MUST_NOT_RUN_AFTER_TERMINAL_FAILURE' }
    $receipt = Invoke-CoordinateConsumerReplayProtocol -Request $request -RunGate $neverGate `
      -SourceArtifact $source -Deadline ([DateTimeOffset]$testExpiresAt) -KmlPath $kmlPath
    if ($receipt.success -ne $false -or $receipt.failureCode -ne 'CONSUMER_REPLAY_BROWSER_TERMINAL_FAILURE' -or
        [int]$requestCounts['/api/admin/coordinate-consumer-replay/evidence'] -ne 1 -or
        [int]$requestCounts['/api/admin/coordinate-consumer-replay/close'] -ne 1) {
      throw 'TERMINAL_FAILURE_DID_NOT_STOP_ON_FIRST_RECEIPT'
    }
  }
  Add-Case 'registration attempt failure still performs the single close request' {
    $requestCounts = @{}
    $expectedRunId = $script:ReplayRunId
    $request = {
      param($method, $path, $body, $timeout)
      $requestCounts[$path] = 1 + [int]$requestCounts[$path]
      switch ($path) {
        '/api/version' { return [pscustomobject]@{ runtimeIdentity = [pscustomobject]@{
          commit = 'candidate'; coordinateConsumerReplay = [pscustomobject]@{ enabled = $true; runId = $expectedRunId } } } }
        '/api/admin/coordinate-consumer-replay/browser-readiness' { return [pscustomobject]@{ browserSessionReady = $true } }
        '/api/admin/coordinate-consumer-replay/register' { return [pscustomobject]@{ registered = $false } }
        '/api/admin/coordinate-consumer-replay/close' { return [pscustomobject]@{ closed = $true } }
        default { throw 'PROHIBITED_OR_UNEXPECTED_REQUEST' }
      }
    }.GetNewClosure()
    $receipt = Invoke-CoordinateConsumerReplayProtocol -Request $request -RunGate { throw 'GATE_MUST_NOT_RUN' } `
      -SourceArtifact $source -Deadline ([DateTimeOffset]$testExpiresAt) -KmlPath $kmlPath
    if ($receipt.registrationAttemptCount -ne 1 -or $receipt.registrationCount -ne 0 -or
        $receipt.replayCloseRequestCount -ne 1 -or [int]$requestCounts['/api/admin/coordinate-consumer-replay/close'] -ne 1) {
      throw 'REGISTRATION_ATTEMPT_NOT_CLOSED'
    }
  }
  Add-Case 'library imports preserve the caller artifact parameter' {
    if ($HistoricalArtifactPath -ne $regressionHistoricalArtifactPath -or
        -not (Get-Command Invoke-CoordinateConsumerReplayProtocol -ErrorAction SilentlyContinue)) {
      throw 'LIBRARY_PARAMETER_POLLUTION_DETECTED'
    }
  }
  function Invoke-R2Scenario([string] $Mode = 'success') {
    $boundRun = $script:ReplayRunId
    $boundBatch = $script:ReplayBatchId
    $boundArtifactSha = $script:ReplayArtifactSha256
    $directory = Join-Path $root ([guid]::NewGuid().ToString('N'))
    New-Item -ItemType Directory -Path $directory | Out-Null
    $clockState = @{ value = [DateTimeOffset]::UtcNow; keyIndex = 0; keyReads = 0; flushes = 0
      requestPaths = [Collections.Generic.List[string]]::new(); runtimeReads = 0
      phases = [Collections.Generic.List[string]]::new(); receiptWrites = 0; delays = 0 }
    $now = { $clockState.value }.GetNewClosure()
    $started = $clockState.value.AddMinutes(-1)
    $nonce = 'b' * 32
    $commit = 'a' * 40
    $statePath = Join-Path $directory 'state.json'
    $receiptPath = Join-Path $directory 'receipt.json'
    $releasePath = Join-Path $directory 'release.json'
    $context = New-ReplayClientStateContext $statePath $commit 42424 $started $nonce
    $manifest = [ordered]@{
      schemaVersion = 'coordinate_consumer_replay_release_v1'
      runId = $script:ReplayRunId; batchId = $script:ReplayBatchId; expectedCommit = $commit
      clientInstanceNonce = $nonce; clientProcessId = 42424
      clientProcessStartedAt = $started.ToUniversalTime().ToString('O')
      enableDeployId = 'dep-offlinetest'; releaseAuthorized = $true
      releasedAt = $clockState.value.ToString('O')
    }
    switch ($Mode) {
      'wrongNonce' { $manifest.clientInstanceNonce = 'c' * 32 }
      'wrongPid' { $manifest.clientProcessId = 42425 }
      'wrongStart' { $manifest.clientProcessStartedAt = $started.AddSeconds(1).ToString('O') }
      'wrongCommit' { $manifest.expectedCommit = 'c' * 40 }
      'wrongRun' { $manifest.runId = 'coordinate-indonesia-consumer-replay-20261012-r1' }
      'wrongBatch' { $manifest.batchId = 'coordinate-indonesia-consumer-replay-20261012-v1' }
      'stringBoolean' { $manifest.releaseAuthorized = 'true' }
      'pastRelease' { $manifest.releasedAt = $started.AddSeconds(-1).ToString('O') }
      'futureRelease' { $manifest.releasedAt = $clockState.value.AddSeconds(1).ToString('O') }
      'alreadyExpired' { $clockState.value = $context.deadline }
    }
    if ($Mode -ne 'missingRelease') { Write-ReplayAtomicJson $releasePath $manifest $nonce }
    $wait = {
      Wait-ReplayReleaseManifest -Path $releasePath -ExpectedCommit $commit -InstanceNonce $nonce `
        -ProcessId 42424 -ProcessStartedAt $started.ToUniversalTime().ToString('O') `
        -PreparationExpiresAt $context.deadline -PreparationClock ([Diagnostics.Stopwatch]::StartNew()) `
        -Now $now -PollLimit 3 -Delay { param($ms) $clockState.delays += 1; $clockState.value = $clockState.value.AddMilliseconds($ms) }
    }.GetNewClosure()
    $persistState = {
      param($path, $value, $instance)
      $clockState.phases.Add($value.phase)
      if (($Mode -eq 'closingStateFailure' -and $value.phase -eq 'CLOSING') -or
          ($Mode -eq 'terminalStateFailure' -and $value.phase -eq 'COMPLETED') -or
          ($Mode -eq 'waitingStateFailure' -and $value.phase -eq 'WAITING_RELEASE')) {
        throw 'SYNTHETIC_SENSITIVE_STATE_ERROR'
      }
      Write-ReplayAtomicJson $path $value $instance
    }.GetNewClosure()
    $runtime = {
      param($timeout)
      $clockState.runtimeReads += 1
      if ($timeout -gt 30000 -or $timeout -le 0) { throw 'INVALID_RUNTIME_TIMEOUT' }
      if ($Mode -eq 'runtimeExpired') { $clockState.value = $context.deadline }
      $version = [pscustomobject]@{ runtimeIdentity = [pscustomobject]@{
        commit = $commit; coordinateConsumerReplay = [pscustomobject]@{ enabled = $true; runId = $boundRun } } }
      if ($Mode -eq 'runtimeMismatch') { $version.runtimeIdentity.commit = 'old' }
      if ($Mode -eq 'runtimeBoolean') { $version.runtimeIdentity.coordinateConsumerReplay.enabled = 'true' }
      return $version
    }.GetNewClosure()
    $flush = {
      $clockState.flushes += 1
      if ($Mode -eq 'flushFailure') { throw 'CONSUMER_REPLAY_CONSOLE_INPUT_FLUSH_UNAVAILABLE' }
      # An early queued Enter is discarded, not passed to the password reader.
      $clockState.keyIndex = 0
    }.GetNewClosure()
    $readPassword = {
      param($deadline)
      $inputState = $clockState
      $inputMode = $Mode
      $available = { $inputMode -ne 'passwordTimeout' }.GetNewClosure()
      $readKey = {
        $inputState.keyReads += 1
        $inputState.keyIndex += 1
        if ($inputMode -eq 'emptyPassword' -or $inputState.keyIndex -gt 1) {
          return [ConsoleKeyInfo]::new([char]13, [ConsoleKey]::Enter, $false, $false, $false)
        }
        return [ConsoleKeyInfo]::new([char]'x', [ConsoleKey]::X, $false, $false, $false)
      }.GetNewClosure()
      $delay = { param($ms) $inputState.value = $deadline }.GetNewClosure()
      try { Read-ReplayPassword -Deadline $deadline -Now $now -KeyAvailable $available -ReadKey $readKey -Render {} -Delay $delay }
      catch { $clockState.testError = $_.Exception.Message; throw }
    }.GetNewClosure()
    $request = {
      param($method, $path, $body, $timeout)
      $clockState.requestPaths.Add($path)
      if ($timeout -le 0 -or $timeout -gt 30000) { throw 'INVALID_REQUEST_TIMEOUT' }
      switch ($path) {
        '/api/admin/coordinate-consumer-replay/browser-readiness' { return [pscustomobject]@{ browserSessionReady = $true } }
        '/api/admin/coordinate-consumer-replay/register' {
          if ($Mode -eq 'registrationFailure') { throw 'SYNTHETIC_SENSITIVE_HTTP_ERROR' }
          if ($Mode -eq 'registrationTimeout') { $clockState.value = $context.deadline }
          return [pscustomobject]@{ registered = $true; runId = $boundRun; batchId = $boundBatch
            caseId = 'indonesia'; expiresAt = $clockState.value.AddMinutes(8).ToString('O'); bindingDigest = $bindingDigest }
        }
        '/api/admin/coordinate-consumer-replay/evidence' {
          if ($Mode -eq 'evidenceTimeout') { $clockState.value = $context.deadline }
          if ($Mode -eq 'browserFailure') { return [pscustomobject]@{
            ready = $false; lastStage = 'MAP_FAILED'; failureCode = 'SYNTHETIC_SENSITIVE_BROWSER_ERROR' } }
          return [pscustomobject]@{ ready = $true; lastStage = 'EVIDENCE_ACCEPTED'; evidence = $evidence }
        }
        '/api/admin/coordinate-consumer-replay/close' {
          if ($Mode -eq 'closeFailure') { throw 'SYNTHETIC_SENSITIVE_CLOSE_ERROR' }
          return [pscustomobject]@{ closed = $true }
        }
        default { throw 'UNEXPECTED_EXTERNAL_BOUNDARY' }
      }
    }.GetNewClosure()
    $runGate = {
      param($browserEvidence, $artifactPath, $downloadPath, $deadline, $digest)
      if ($Mode -eq 'gateTimeout') { $clockState.value = [DateTimeOffset]$deadline }
      if ($Mode -eq 'gateFailure') { throw 'SYNTHETIC_SENSITIVE_XML_ERROR' }
      if ($Mode -eq 'success') {
        return Invoke-CoordinateConsumerReplayGate -EvidenceObject $browserEvidence -ArtifactPath $artifactPath `
          -DownloadPath $downloadPath -RunId $boundRun -BatchId $boundBatch `
          -ArtifactSha256 $boundArtifactSha -ExpectedBindingDigest $digest -ExpiresAt $deadline -Now $now
      }
      # Failure injection cases isolate the changed orchestration; do not repeat geometry regression.
      return [pscustomobject]@{ success = $true; mapReady = $true; kmlParsed = $true; kmlGeometryMatch = $true; kmlSha256 = 'd' * 64 }
    }.GetNewClosure()
    $protocol = {
      param($secure, $transition, $receipt, $deadline)
      Invoke-CoordinateConsumerReplayProtocol -SourceArtifact $source -KmlPath $kmlPath `
        -Request $request -RunGate $runGate -StateTransition $transition -Receipt $receipt -Deadline $deadline -Now $now
    }.GetNewClosure()
    $persistReceipt = {
      param($receipt)
      $clockState.receiptWrites += 1
      if ($Mode -eq 'receiptFailure') { throw 'SYNTHETIC_SENSITIVE_FILE_ERROR' }
      Write-ReplayAtomicJson $receiptPath $receipt $nonce
    }.GetNewClosure()
    $receipt = Invoke-CoordinateConsumerReplayClientEntrypoint -SourceArtifact $source -ExpectedCommit $commit `
      -KmlPath $kmlPath -StateContext $context -WaitForRelease $wait -ReadRuntimeVersion $runtime `
      -FlushInputBuffer $flush -ReadPassword $readPassword -RunProtocol $protocol -PersistState $persistState `
      -PersistReceipt $persistReceipt -Now $now
    if ($context.writeCount -gt 7 -or $clockState.receiptWrites -ne 1 -or
        $receipt.registrationAttemptCount -gt 1 -or $receipt.replayCloseRequestCount -gt 1) { throw 'R2_OPERATION_BOUND_EXCEEDED' }
    if (($receipt | ConvertTo-Json -Depth 8 -Compress) -match 'SYNTHETIC_SENSITIVE') { throw 'R2_RAW_ERROR_LEAK' }
    return @{ receipt = $receipt; io = $clockState; context = $context; statePath = $statePath; receiptPath = $receiptPath }
  }

  Add-Case 'R2 actual entry release runtime secure input protocol physical parser close and receipt' {
    $r = Invoke-R2Scenario
    if (-not $r.receipt.success -or $r.receipt.registrationCount -ne 1 -or
        $r.receipt.replayCloseRequestCount -ne 1 -or -not $r.receipt.kmlGeometryMatch -or
        $r.io.keyReads -ne 2 -or $r.io.flushes -ne 1 -or $r.io.runtimeReads -ne 1 -or
        ($r.io.phases -join ',') -ne 'PREPARING,WAITING_RELEASE,RUNTIME_CHECK,WAITING_PASSWORD,RUNNING,CLOSING,COMPLETED') {
      throw ('R2_ENTRY_CHAIN_FAILED:' + $r.io.testError + ':' + ($r.receipt | ConvertTo-Json -Depth 5 -Compress))
    }
    $saved = Get-Content -LiteralPath $r.receiptPath -Raw | ConvertFrom-Json
    if ($saved.registrationCount -ne 1 -or $saved.finalReceiptWriteCount -ne 1) { throw 'R2_RECEIPT_NOT_PRESERVED' }
  }
  foreach ($mode in @('missingRelease', 'wrongNonce', 'wrongPid', 'wrongStart', 'wrongCommit',
      'wrongRun', 'wrongBatch', 'stringBoolean', 'pastRelease', 'futureRelease', 'alreadyExpired', 'waitingStateFailure')) {
    Add-Case "R2 $mode blocks before public request input or registration" {
      $r = Invoke-R2Scenario $mode
      if ($r.receipt.success -or $r.io.runtimeReads -ne 0 -or $r.io.keyReads -ne 0 -or
          $r.io.flushes -ne 0 -or $r.io.requestPaths.Count -ne 0 -or $r.receipt.registrationAttemptCount -ne 0) {
        throw 'R2_PRE_RELEASE_SIDE_EFFECT'
      }
      if ($mode -eq 'missingRelease' -and $r.io.delays -ne 3) { throw 'R2_RELEASE_POLL_NOT_BOUNDED' }
      $expectedCode = switch ($mode) {
        'missingRelease' { 'CONSUMER_REPLAY_PREPARATION_TIMEOUT' }
        'alreadyExpired' { 'CONSUMER_REPLAY_TOTAL_DEADLINE_EXPIRED' }
        'waitingStateFailure' { 'CONSUMER_REPLAY_STATE_PERSISTENCE_FAILED' }
        default { 'CONSUMER_REPLAY_RELEASE_MANIFEST_REJECTED' }
      }
      if ($r.receipt.failureCode -cne $expectedCode) { throw 'R2_WRONG_PRE_RELEASE_FAILURE' }
    }
  }
  foreach ($mode in @('runtimeMismatch', 'runtimeBoolean', 'runtimeExpired', 'flushFailure')) {
    Add-Case "R2 $mode prevents password and authenticated requests" {
      $r = Invoke-R2Scenario $mode
      if ($r.receipt.success -or $r.io.runtimeReads -ne 1 -or $r.io.keyReads -ne 0 -or
          $r.io.requestPaths.Count -ne 0) { throw 'R2_PRE_PASSWORD_GATE_BYPASSED' }
    }
  }
  foreach ($mode in @('emptyPassword', 'passwordTimeout')) {
    Add-Case "R2 $mode fails locally without registration" {
      $r = Invoke-R2Scenario $mode
      if ($r.receipt.success -or $r.io.flushes -ne 1 -or $r.io.requestPaths.Count -ne 0 -or
          $r.receipt.replayCloseRequestCount -ne 0) { throw 'R2_PASSWORD_GATE_BYPASSED' }
    }
  }
  foreach ($mode in @('registrationFailure', 'registrationTimeout', 'evidenceTimeout', 'gateTimeout',
      'browserFailure', 'closingStateFailure', 'terminalStateFailure', 'receiptFailure', 'closeFailure', 'gateFailure')) {
    Add-Case "R2 $mode preserves attempts and single cleanup even on state failure" {
      $r = Invoke-R2Scenario $mode
      $closes = @($r.io.requestPaths | Where-Object { $_ -eq '/api/admin/coordinate-consumer-replay/close' }).Count
      if ($r.receipt.success -or $r.receipt.registrationAttemptCount -ne 1 -or
          $r.receipt.replayCloseRequestCount -ne 1 -or $closes -ne 1 -or
          ($mode -ne 'registrationFailure' -and $r.receipt.registrationCount -ne 1)) { throw 'R2_LOST_ACTION_COUNTS_OR_CLEANUP' }
      if ($mode -eq 'registrationTimeout' -and $r.io.requestPaths.Count -ne 3) { throw 'R2_REQUEST_AFTER_DEADLINE' }
      if ($mode -eq 'receiptFailure' -and $r.receipt.finalReceiptPersisted) { throw 'R2_FALSE_RECEIPT_PERSISTENCE' }
      if ($mode -eq 'gateFailure' -and $r.receipt.failureCode -cne 'CONSUMER_REPLAY_GATE_FAILED') { throw 'R2_GATE_DIAGNOSTIC_LOST' }
    }
  }
  Add-Case 'R2 gate rejects expiry after physical XML and hash work' {
    $gateClock = @{ calls = 0 }
    $gateNow = {
      $gateClock.calls += 1
      if ($gateClock.calls -ge 3) { [DateTimeOffset]$testExpiresAt } else { [DateTimeOffset]::UtcNow }
    }.GetNewClosure()
    $caught = ''
    try {
      Invoke-CoordinateConsumerReplayGate -EvidenceObject $evidence -ArtifactPath $source.Path `
        -DownloadPath $kmlPath -RunId $script:ReplayRunId -BatchId $script:ReplayBatchId `
        -ArtifactSha256 $script:ReplayArtifactSha256 -ExpectedBindingDigest $bindingDigest `
        -ExpiresAt $testExpiresAt -Now $gateNow | Out-Null
    } catch { $caught = $_.Exception.Message }
    if ($caught -ne 'CONSUMER_REPLAY_HANDOFF_EXPIRED') { throw 'R2_LATE_GATE_SUCCESS' }
  }
  Add-Case 'R2 executor read-only liveness gate rejects exit PID reuse failure stale state and final receipt' {
    $directory = Join-Path $root 'liveness'
    New-Item -ItemType Directory -Path $directory | Out-Null
    $path = Join-Path $directory 'state.json'; $receiptFile = Join-Path $directory 'receipt.json'
    $processInfo = Get-Process -Id $PID
    $started = [DateTimeOffset]$processInfo.StartTime.ToUniversalTime()
    $context = New-ReplayClientStateContext $path ('a' * 40) $PID $started ('b' * 32)
    Set-ReplayClientState $context 'PREPARING'
    Set-ReplayClientState $context 'WAITING_RELEASE'
    $args = @{ StatePath = $path; ReceiptPath = $receiptFile; ExpectedCommit = 'a' * 40
      ProcessId = $PID; ProcessStartedAt = $started; InstanceNonce = 'b' * 32 }
    if (-not (Test-ReplayWaitingClient @args)) { throw 'R2_LIVE_PROCESS_REJECTED' }
    if (Test-ReplayWaitingClient @args -ReadProcess { param($id) throw 'EXITED' }) { throw 'R2_EXIT_NOT_DETECTED' }
    $reused = { param($id) [pscustomobject]@{ Id = $id; HasExited = $false; StartTime = $started.AddSeconds(1).UtcDateTime } }.GetNewClosure()
    if (Test-ReplayWaitingClient @args -ReadProcess $reused) { throw 'R2_PID_REUSE_ACCEPTED' }
    $staleNow = { $started.AddMinutes(15) }.GetNewClosure()
    if (Test-ReplayWaitingClient @args -Now $staleNow) { throw 'R2_EXPIRED_STATE_ACCEPTED' }
    Write-ReplayAtomicJson $receiptFile @{ success = $false } ('b' * 32)
    if (Test-ReplayWaitingClient @args) { throw 'R2_FINAL_RECEIPT_IGNORED' }
    Set-ReplayClientState $context 'FAILED' 'CONSUMER_REPLAY_EXECUTION_FAILED'
    $args.ReceiptPath = Join-Path $directory 'nonexistent.json'
    if (Test-ReplayWaitingClient @args) { throw 'R2_TERMINAL_STATE_ACCEPTED' }
  }
} finally {
  $resolvedTemp = [IO.Path]::GetFullPath([IO.Path]::GetTempPath()).TrimEnd('\')
  $resolvedRoot = [IO.Path]::GetFullPath($root)
  if ((Split-Path -Parent $resolvedRoot) -ne $resolvedTemp -or
      (Split-Path -Leaf $resolvedRoot) -notmatch '^consumer-replay-regression-[a-f0-9]{32}$') { throw 'UNSAFE_TEST_CLEANUP_PATH' }
  if (Test-Path -LiteralPath $resolvedRoot) { Remove-Item -LiteralPath $resolvedRoot -Recurse -Force }
}

$passed = @($cases | Where-Object { $_.pass }).Count
[ordered]@{
  suite = 'COORDINATE_CONSUMER_REPLAY_CLIENT'
  passed = $passed
  total = $cases.Count
  evidenceClass = 'OFFLINE_STUBBED_EXTERNAL_BOUNDARIES'
  providerCalls = 0
  modelCalls = 0
  databaseWrites = 0
  businessQuotaDeductions = 0
  cases = $cases
} | ConvertTo-Json -Depth 6 -Compress
if ($passed -ne $cases.Count) { exit 1 }
