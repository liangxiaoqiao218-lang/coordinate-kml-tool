param(
  [Parameter(Mandatory = $true)] [string] $HistoricalArtifactPath
)

$ErrorActionPreference = 'Stop'
$regressionHistoricalArtifactPath = $HistoricalArtifactPath
. (Join-Path $PSScriptRoot 'coordinate-consumer-replay-client.ps1') -LibraryOnly
$HistoricalArtifactPath = $regressionHistoricalArtifactPath
$cases = [Collections.Generic.List[object]]::new()
function Add-Case([string] $Name, [scriptblock] $Body) {
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
      -SourceArtifact $source -RuntimeCommit 'candidate' -KmlPath $kmlPath
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
    if ($sourceText -notmatch 'Read-Host.+AsSecureString' -or $sourceText -notmatch 'automaticRetries = 0') {
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
      -SourceArtifact $source -RuntimeCommit 'candidate' -KmlPath $kmlPath
    if ($receipt.success -ne $false -or $receipt.failureCode -ne 'CONSUMER_REPLAY_TERMINAL_MAP_PRODUCT_PATH_FAILED' -or
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
      -SourceArtifact $source -RuntimeCommit 'candidate' -KmlPath $kmlPath
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
} finally {
  if (Test-Path -LiteralPath $root) { Remove-Item -LiteralPath $root -Recurse -Force }
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
