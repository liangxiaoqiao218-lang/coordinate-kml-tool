param(
  [string] $BrowserEvidencePath,
  [string] $HistoricalArtifactPath,
  [string] $KmlDownloadPath,
  [string] $ExpectedRunId = 'coordinate-indonesia-consumer-replay-20261012-r2',
  [string] $ExpectedBatchId = 'coordinate-indonesia-consumer-replay-20261012-v2',
  [string] $ExpectedArtifactSha256 = '0de3ed5542962d8203e15e654bc2e908dc659745b72b55c5c1518075f94f3522',
  [string] $ExpectedBindingDigest,
  [string] $HandoffExpiresAt,
  [int] $TimeoutSeconds = 480,
  [switch] $LibraryOnly
)

$ErrorActionPreference = 'Stop'
$replayGateParameters = @{
  BrowserEvidencePath = $BrowserEvidencePath
  HistoricalArtifactPath = $HistoricalArtifactPath
  KmlDownloadPath = $KmlDownloadPath
  ExpectedRunId = $ExpectedRunId
  ExpectedBatchId = $ExpectedBatchId
  ExpectedArtifactSha256 = $ExpectedArtifactSha256
  ExpectedBindingDigest = $ExpectedBindingDigest
  HandoffExpiresAt = $HandoffExpiresAt
  TimeoutSeconds = $TimeoutSeconds
  LibraryOnly = $LibraryOnly
}
. (Join-Path $PSScriptRoot 'coordinate-three-case-rc-r7-browser-consumer-gate.ps1') -LibraryOnly
$BrowserEvidencePath = $replayGateParameters.BrowserEvidencePath
$HistoricalArtifactPath = $replayGateParameters.HistoricalArtifactPath
$KmlDownloadPath = $replayGateParameters.KmlDownloadPath
$ExpectedRunId = $replayGateParameters.ExpectedRunId
$ExpectedBatchId = $replayGateParameters.ExpectedBatchId
$ExpectedArtifactSha256 = $replayGateParameters.ExpectedArtifactSha256
$ExpectedBindingDigest = $replayGateParameters.ExpectedBindingDigest
$HandoffExpiresAt = $replayGateParameters.HandoffExpiresAt
$TimeoutSeconds = $replayGateParameters.TimeoutSeconds
$LibraryOnly = $replayGateParameters.LibraryOnly

function Invoke-CoordinateConsumerReplayGate {
  param(
    [string] $EvidencePath,
    [object] $EvidenceObject,
    [Parameter(Mandatory = $true)] [string] $ArtifactPath,
    [Parameter(Mandatory = $true)] [string] $DownloadPath,
    [Parameter(Mandatory = $true)] [string] $RunId,
    [Parameter(Mandatory = $true)] [string] $BatchId,
    [Parameter(Mandatory = $true)] [string] $ArtifactSha256,
    [Parameter(Mandatory = $true)] [string] $ExpectedBindingDigest,
    [Parameter(Mandatory = $true)] [string] $ExpiresAt,
    [int] $WaitSeconds = 480,
    [scriptblock] $Now = { [DateTimeOffset]::UtcNow }
  )
  foreach ($candidate in @($ArtifactPath, $DownloadPath)) {
    if (-not [IO.Path]::IsPathFullyQualified($candidate)) { throw 'CONSUMER_REPLAY_ABSOLUTE_PATH_REQUIRED' }
  }
  if ($ExpectedBindingDigest -cnotmatch '^[0-9a-f]{64}$') { throw 'CONSUMER_REPLAY_EXPECTED_BINDING_INVALID' }
  if ($null -eq $EvidenceObject -and (-not [IO.Path]::IsPathFullyQualified($EvidencePath))) {
    throw 'CONSUMER_REPLAY_ABSOLUTE_PATH_REQUIRED'
  }
  if (-not (Test-Path -LiteralPath $ArtifactPath -PathType Leaf)) { throw 'CONSUMER_REPLAY_SOURCE_ARTIFACT_MISSING' }
  $actualArtifactSha = (Get-FileHash -LiteralPath $ArtifactPath -Algorithm SHA256).Hash.ToLowerInvariant()
  if ($actualArtifactSha -ne $ArtifactSha256) { throw 'CONSUMER_REPLAY_SOURCE_ARTIFACT_HASH_MISMATCH' }

  $deadline = [DateTimeOffset]::MinValue
  if (-not [DateTimeOffset]::TryParse($ExpiresAt, [Globalization.CultureInfo]::InvariantCulture,
      [Globalization.DateTimeStyles]::AssumeUniversal, [ref]$deadline)) {
    throw 'CONSUMER_REPLAY_HANDOFF_DEADLINE_INVALID'
  }
  $remainingMilliseconds = [int64][Math]::Floor(($deadline.ToUniversalTime() - (& $Now)).TotalMilliseconds)
  if ($remainingMilliseconds -le 0) { throw 'CONSUMER_REPLAY_HANDOFF_EXPIRED' }
  $timeoutMilliseconds = [int][Math]::Min([int64]($WaitSeconds * 1000), $remainingMilliseconds)
  $clock = [Diagnostics.Stopwatch]::StartNew()
  if ($null -eq $EvidenceObject) {
    if (-not (Wait-R7File -Path $EvidencePath -Clock $clock -TimeoutMilliseconds $timeoutMilliseconds)) {
      throw 'CONSUMER_REPLAY_BROWSER_EVIDENCE_TIMEOUT'
    }
  }
  if (-not (Wait-R7StableFile -Path $DownloadPath -Clock $clock -TimeoutMilliseconds $timeoutMilliseconds)) {
    throw 'CONSUMER_REPLAY_KML_DOWNLOAD_TIMEOUT'
  }
  if ((& $Now) -ge $deadline.ToUniversalTime()) { throw 'CONSUMER_REPLAY_HANDOFF_EXPIRED' }

  $browser = if ($null -ne $EvidenceObject) { $EvidenceObject } else { Get-Content -LiteralPath $EvidencePath -Raw | ConvertFrom-Json }
  $artifact = Get-Content -LiteralPath $ArtifactPath -Raw | ConvertFrom-Json
  $cases = @($artifact.cases | Where-Object { $_.caseId -eq 'indonesia' })
  if ($artifact.schemaVersion -ne 'coordinate_three_case_validated_results_v1' -or
      $artifact.runId -ne 'coordinate-three-case-rc-20261011-r7' -or
      $artifact.batchId -ne 'coordinate-three-case-rc-20261011-v7' -or
      $artifact.expectedCommit -ne '279f7bee577152a83efe6a53a5cdb44d6e18cebc' -or $cases.Count -ne 1) {
    throw 'CONSUMER_REPLAY_SOURCE_ARTIFACT_CONTRACT_MISMATCH'
  }
  $record = $cases[0]
  $finalized = $record.finalizedResult
  if ($record.imageSha256 -ne '41f2b2117667fb92f6a4eb703822b1893e29c985be2e14f7b20fbda103b66cf2' -or
      $record.terminalStatus -ne 'SUCCEEDED' -or $record.success -ne $true -or
      $finalized.identityPresent -ne $true -or $finalized.currentRevisionMatch -ne $true -or
      $finalized.crs.id -ne 'EPSG:4326' -or $finalized.crs.axisOrder -ne 'longitude_latitude') {
    throw 'CONSUMER_REPLAY_SOURCE_RESULT_BINDING_INVALID'
  }
  $browserContract = $browser.schemaVersion -eq 'coordinate_consumer_replay_browser_evidence_v1' -and
    $browser.runId -eq $RunId -and $browser.batchId -eq $BatchId -and $browser.caseId -eq 'indonesia' -and
    ([string]$browser.bindingDigest -match '^[0-9a-f]{64}$') -and
    ([string]$browser.bindingDigest -ceq $ExpectedBindingDigest) -and
    $browser.replayOnly -eq $true -and $browser.businessQuotaBypassed -eq $true -and
    $browser.resultInstalled -eq $true -and $browser.resultIdentityMatch -eq $true -and
    $browser.mapState -eq 'READY' -and $browser.mapProvider -eq 'OPENFREEMAP' -and
    $browser.mapIdentityMatch -eq $true -and [int]$browser.authorityMutationCount -eq 0 -and
    $browser.kmlDownloadTriggered -eq $true -and $browser.kmlIdentityMatch -eq $true -and
    $browser.downloadFileName -eq 'coordinates.kml'
  if (-not $browserContract) { throw 'CONSUMER_REPLAY_BROWSER_EVIDENCE_REJECTED' }

  $expectedGroups = @(ConvertTo-R7CoordinateGroups -Geometry $finalized.geometry)
  $actualKml = Read-R7KmlCoordinateGroups -Path $DownloadPath
  if ([string]$actualKml.geometryType -ne [string]$finalized.geometry.type) {
    throw 'CONSUMER_REPLAY_KML_GEOMETRY_TYPE_MISMATCH'
  }
  if (-not (Test-R7CoordinateGroupsExact -Expected $expectedGroups -Actual @($actualKml.groups))) {
    throw 'CONSUMER_REPLAY_KML_GEOMETRY_IDENTITY_MISMATCH'
  }
  $kmlSha256 = (Get-FileHash -LiteralPath $DownloadPath -Algorithm SHA256).Hash.ToLowerInvariant()
  if ((& $Now) -ge $deadline.ToUniversalTime()) { throw 'CONSUMER_REPLAY_HANDOFF_EXPIRED' }
  return [ordered]@{
    success = $true
    evidenceClass = 'HISTORICAL_RESULT_CONSUMER_REPLAY_ONLY'
    artifactHashMatch = $true
    browserEvidenceAccepted = $true
    replayOnly = $true
    businessQuotaBypassed = $true
    mapReady = $true
    mapProvider = 'OPENFREEMAP'
    authorityMutationCount = 0
    kmlParsed = $true
    kmlGeometryType = [string]$actualKml.geometryType
    kmlGeometryMatch = $true
    kmlSha256 = $kmlSha256
    providerCalls = 0
    recognitionClaims = 0
    recognitionDispatches = 0
    databaseWrites = 0
    businessQuotaDeductions = 0
  }
}

if ($LibraryOnly) { return }
try {
  $result = Invoke-CoordinateConsumerReplayGate -EvidencePath $BrowserEvidencePath `
    -ArtifactPath $HistoricalArtifactPath -DownloadPath $KmlDownloadPath `
    -RunId $ExpectedRunId -BatchId $ExpectedBatchId -ArtifactSha256 $ExpectedArtifactSha256 `
    -ExpectedBindingDigest $ExpectedBindingDigest `
    -ExpiresAt $HandoffExpiresAt -WaitSeconds $TimeoutSeconds
  $result | ConvertTo-Json -Depth 6 -Compress
  exit 0
} catch {
  [ordered]@{ success = $false; code = 'CONSUMER_REPLAY_GATE_FAILED'; evidenceClass = 'HISTORICAL_RESULT_CONSUMER_REPLAY_ONLY' } | ConvertTo-Json -Compress
  exit 7
}
