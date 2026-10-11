param(
  [string] $RcBaseUrl,
  [string] $ExpectedCommit,
  [string] $HistoricalArtifactPath,
  [string] $EvidencePath,
  [switch] $LibraryOnly
)

$ErrorActionPreference = 'Stop'
$script:ReplayRunId = 'coordinate-indonesia-consumer-replay-20261012-r1'
$script:ReplayBatchId = 'coordinate-indonesia-consumer-replay-20261012-v1'
$script:ReplayArtifactSha256 = '0de3ed5542962d8203e15e654bc2e908dc659745b72b55c5c1518075f94f3522'
$replayClientParameters = @{
  RcBaseUrl = $RcBaseUrl
  ExpectedCommit = $ExpectedCommit
  HistoricalArtifactPath = $HistoricalArtifactPath
  EvidencePath = $EvidencePath
  LibraryOnly = $LibraryOnly
}
. (Join-Path $PSScriptRoot 'coordinate-consumer-replay-gate.ps1') -LibraryOnly
$RcBaseUrl = $replayClientParameters.RcBaseUrl
$ExpectedCommit = $replayClientParameters.ExpectedCommit
$HistoricalArtifactPath = $replayClientParameters.HistoricalArtifactPath
$EvidencePath = $replayClientParameters.EvidencePath
$LibraryOnly = $replayClientParameters.LibraryOnly

function ConvertFrom-ReplaySecureString([Security.SecureString] $Secure) {
  $pointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Secure)
  try { return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($pointer) }
  finally { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($pointer) }
}

function Invoke-ReplayJsonRequest {
  param([Net.Http.HttpClient] $Client, [string] $Method, [string] $Url, [object] $Body = $null, [int] $TimeoutMilliseconds = 30000)
  $request = [Net.Http.HttpRequestMessage]::new([Net.Http.HttpMethod]::new($Method), $Url)
  $cancellation = [Threading.CancellationTokenSource]::new($TimeoutMilliseconds)
  try {
    if ($null -ne $Body) {
      $json = $Body | ConvertTo-Json -Depth 10 -Compress
      $request.Content = [Net.Http.StringContent]::new($json, [Text.Encoding]::UTF8, 'application/json')
    }
    $response = $Client.SendAsync($request, $cancellation.Token).GetAwaiter().GetResult()
    $text = $response.Content.ReadAsStringAsync().GetAwaiter().GetResult()
    $payload = if ([string]::IsNullOrWhiteSpace($text)) { $null } else { $text | ConvertFrom-Json }
    if (-not $response.IsSuccessStatusCode) {
      $code = if ($payload.code) { [string]$payload.code } else { 'HTTP_FAILURE' }
      throw "HTTP_$([int]$response.StatusCode)_$code"
    }
    return $payload
  } finally {
    $cancellation.Dispose()
    $request.Dispose()
  }
}

function Test-ReplaySourceArtifact {
  param([Parameter(Mandatory = $true)] [string] $Path)
  $resolved = [IO.Path]::GetFullPath($Path)
  if (-not (Test-Path -LiteralPath $resolved -PathType Leaf)) { throw 'CONSUMER_REPLAY_SOURCE_ARTIFACT_MISSING' }
  $sha = (Get-FileHash -LiteralPath $resolved -Algorithm SHA256).Hash.ToLowerInvariant()
  if ($sha -ne $script:ReplayArtifactSha256) { throw 'CONSUMER_REPLAY_SOURCE_ARTIFACT_HASH_MISMATCH' }
  $artifact = Get-Content -LiteralPath $resolved -Raw | ConvertFrom-Json
  $cases = @($artifact.cases | Where-Object { $_.caseId -eq 'indonesia' })
  if ($artifact.schemaVersion -ne 'coordinate_three_case_validated_results_v1' -or
      $artifact.runId -ne 'coordinate-three-case-rc-20261011-r7' -or
      $artifact.batchId -ne 'coordinate-three-case-rc-20261011-v7' -or
      $artifact.expectedCommit -ne '279f7bee577152a83efe6a53a5cdb44d6e18cebc' -or $cases.Count -ne 1 -or
      $cases[0].imageSha256 -ne '41f2b2117667fb92f6a4eb703822b1893e29c985be2e14f7b20fbda103b66cf2' -or
      $cases[0].terminalStatus -ne 'SUCCEEDED' -or $cases[0].success -ne $true -or
      $cases[0].finalizedResult.identityPresent -ne $true -or
      $cases[0].finalizedResult.currentRevisionMatch -ne $true -or
      $cases[0].finalizedResult.crs.id -ne 'EPSG:4326' -or
      $cases[0].finalizedResult.crs.axisOrder -ne 'longitude_latitude') {
    throw 'CONSUMER_REPLAY_SOURCE_ARTIFACT_CONTRACT_MISMATCH'
  }
  return [pscustomobject]@{ Path = $resolved; Sha256 = $sha; Bytes = [IO.File]::ReadAllBytes($resolved) }
}

function Invoke-CoordinateConsumerReplayProtocol {
  param(
    [Parameter(Mandatory = $true)] [scriptblock] $Request,
    [Parameter(Mandatory = $true)] [scriptblock] $RunGate,
    [Parameter(Mandatory = $true)] [object] $SourceArtifact,
    [Parameter(Mandatory = $true)] [string] $RuntimeCommit,
    [Parameter(Mandatory = $true)] [string] $KmlPath
  )
  $registrationAttempted = $false
  $registered = $false
  $closeAttempted = $false
  $closeReceipt = $null
  $failureCode = ''
  $gateReceipt = $null
  $expiresAt = ''
  $lastStage = 'NOT_STARTED'
  $runtimeCommitMatch = $false
  $artifactHashMatch = $SourceArtifact.Sha256 -eq $script:ReplayArtifactSha256
  $registrationBindingDigest = ''
  $browserEvidenceReceived = $false
  try {
    $version = & $Request 'GET' '/api/version' $null 30000
    if ([string]$version.runtimeIdentity.commit -ne $RuntimeCommit -or
        $version.runtimeIdentity.coordinateConsumerReplay.enabled -ne $true -or
        [string]$version.runtimeIdentity.coordinateConsumerReplay.runId -ne $script:ReplayRunId) {
      throw 'CONSUMER_REPLAY_RUNTIME_IDENTITY_MISMATCH'
    }
    $runtimeCommitMatch = $true
    $readiness = & $Request 'POST' '/api/admin/coordinate-consumer-replay/browser-readiness' ([ordered]@{}) 30000
    if ($readiness.browserSessionReady -ne $true) { throw 'CONSUMER_REPLAY_BROWSER_SESSION_NOT_READY' }
    $registrationAttempted = $true
    $registration = & $Request 'POST' '/api/admin/coordinate-consumer-replay/register' ([ordered]@{
      artifactSha256 = $SourceArtifact.Sha256
      artifactBase64 = [Convert]::ToBase64String($SourceArtifact.Bytes)
    }) 30000
    if ($registration.registered -ne $true -or $registration.runId -ne $script:ReplayRunId -or
        $registration.batchId -ne $script:ReplayBatchId -or $registration.caseId -ne 'indonesia' -or
        [string]$registration.bindingDigest -cnotmatch '^[0-9a-f]{64}$') {
      throw 'CONSUMER_REPLAY_REGISTRATION_CONTRACT_INVALID'
    }
    $registered = $true
    $registrationBindingDigest = [string]$registration.bindingDigest
    $expiresAt = [string]$registration.expiresAt
    $deadline = [DateTimeOffset]::MinValue
    if (-not [DateTimeOffset]::TryParse($expiresAt, [Globalization.CultureInfo]::InvariantCulture,
        [Globalization.DateTimeStyles]::AssumeUniversal, [ref]$deadline)) {
      throw 'CONSUMER_REPLAY_HANDOFF_DEADLINE_INVALID'
    }
    while ([DateTimeOffset]::UtcNow -lt $deadline.ToUniversalTime()) {
      $remaining = [int][Math]::Floor(($deadline.ToUniversalTime() - [DateTimeOffset]::UtcNow).TotalMilliseconds)
      if ($remaining -le 0) { break }
      $evidenceReceipt = & $Request 'GET' '/api/admin/coordinate-consumer-replay/evidence' $null ([Math]::Min(30000, $remaining))
      $lastStage = [string]$evidenceReceipt.lastStage
      if (-not [string]::IsNullOrWhiteSpace([string]$evidenceReceipt.failureCode)) {
        throw ('CONSUMER_REPLAY_TERMINAL_' + [string]$evidenceReceipt.failureCode)
      }
      if ($evidenceReceipt.ready -eq $true -and $null -ne $evidenceReceipt.evidence) {
        $browserEvidenceReceived = $true
        $gateReceipt = & $RunGate $evidenceReceipt.evidence $SourceArtifact.Path $KmlPath $expiresAt $registrationBindingDigest
        if ($gateReceipt.success -ne $true) { throw 'CONSUMER_REPLAY_GATE_FAILED' }
        break
      }
      Start-Sleep -Milliseconds 500
    }
    if ($null -eq $gateReceipt) { throw "CONSUMER_REPLAY_EVIDENCE_TIMEOUT_$lastStage" }
  } catch {
    $failureCode = [string]$_.Exception.Message
  } finally {
    if ($registrationAttempted -and -not $closeAttempted) {
      $closeAttempted = $true
      try { $closeReceipt = & $Request 'POST' '/api/admin/coordinate-consumer-replay/close' ([ordered]@{}) 30000 }
      catch {
        if ([string]::IsNullOrWhiteSpace($failureCode)) { $failureCode = 'CONSUMER_REPLAY_CLOSE_FAILED' }
      }
    }
  }
  return [ordered]@{
    success = [string]::IsNullOrWhiteSpace($failureCode) -and $gateReceipt.success -eq $true -and $closeReceipt.closed -eq $true
    evidenceClass = 'HISTORICAL_RESULT_CONSUMER_REPLAY_ONLY'
    runId = $script:ReplayRunId
    batchId = $script:ReplayBatchId
    runtimeCommitMatch = $runtimeCommitMatch
    artifactHashMatch = $artifactHashMatch
    registrationAttemptCount = if ($registrationAttempted) { 1 } else { 0 }
    registrationCount = if ($registered) { 1 } else { 0 }
    browserEvidencePostCount = if ($browserEvidenceReceived) { 1 } else { 'UNKNOWN' }
    mapOpenCount = if ($null -ne $gateReceipt) { if ($gateReceipt.mapReady -eq $true) { 1 } else { 0 } } else { 'UNKNOWN' }
    kmlDownloadCount = if ($null -ne $gateReceipt) { if ($gateReceipt.kmlParsed -eq $true) { 1 } else { 0 } } else { 'UNKNOWN' }
    kmlParsed = $gateReceipt.kmlParsed -eq $true
    kmlGeometryMatch = $gateReceipt.kmlGeometryMatch -eq $true
    kmlSha256 = [string]$gateReceipt.kmlSha256
    replayCloseRequestCount = if ($closeAttempted) { 1 } else { 0 }
    replayClosed = $closeReceipt.closed -eq $true
    lastStage = $lastStage
    failureCodePresent = -not [string]::IsNullOrWhiteSpace($failureCode)
    failureCode = $failureCode
    providerCalls = 0
    modelCalls = 0
    recognitionClaims = 0
    recognitionDispatches = 0
    databaseWrites = 0
    businessQuotaDeductions = 0
    automaticRetries = 0
  }
}

if ($LibraryOnly) { return }

$baseUri = [Uri]$RcBaseUrl
if ($baseUri.Scheme -ne 'https' -or $baseUri.Host -ne 'coordinate-kml-tool-rc.onrender.com' -or $baseUri.Query -or $baseUri.Fragment) {
  throw 'CONSUMER_REPLAY_BASE_URL_REJECTED'
}
$source = Test-ReplaySourceArtifact -Path $HistoricalArtifactPath
$resolvedEvidencePath = [IO.Path]::GetFullPath($EvidencePath)
$evidenceParent = Split-Path -Parent $resolvedEvidencePath
if (-not (Test-Path -LiteralPath $evidenceParent)) { New-Item -ItemType Directory -Path $evidenceParent | Out-Null }
$downloadDirectory = Join-Path $evidenceParent 'coordinate-consumer-replay-download'
$kmlDownloadPath = Join-Path $downloadDirectory 'coordinates.kml'
if ((Test-Path -LiteralPath $resolvedEvidencePath) -or (Test-Path -LiteralPath $downloadDirectory)) {
  throw 'CONSUMER_REPLAY_STALE_ARTIFACT_PRESENT'
}
New-Item -ItemType Directory -Path $downloadDirectory | Out-Null
Write-Host "CONSUMER_REPLAY_DOWNLOAD_DIRECTORY=$downloadDirectory"

$adminSecure = Read-Host '请输入 RC 管理员密码（不会显示）' -AsSecureString
$adminPlain = ConvertFrom-ReplaySecureString $adminSecure
$client = [Net.Http.HttpClient]::new()
$client.Timeout = [TimeSpan]::FromSeconds(30)
$client.DefaultRequestHeaders.Add('x-admin-password', $adminPlain)
$request = {
  param($method, $path, $body, $timeout)
  return Invoke-ReplayJsonRequest -Client $client -Method $method -Url "$($baseUri.AbsoluteUri.TrimEnd('/'))$path" `
    -Body $body -TimeoutMilliseconds $timeout
}
$runGate = {
  param($browserEvidence, $artifactPath, $kmlPath, $expiresAt, $expectedBindingDigest)
  return Invoke-CoordinateConsumerReplayGate -EvidenceObject $browserEvidence `
    -ArtifactPath $artifactPath -DownloadPath $kmlPath -RunId $script:ReplayRunId `
    -BatchId $script:ReplayBatchId -ArtifactSha256 $script:ReplayArtifactSha256 `
    -ExpectedBindingDigest $expectedBindingDigest -ExpiresAt $expiresAt
}
try {
  $receipt = Invoke-CoordinateConsumerReplayProtocol -Request $request -RunGate $runGate `
    -SourceArtifact $source -RuntimeCommit $ExpectedCommit -KmlPath $kmlDownloadPath
  $receipt['generatedAt'] = [DateTimeOffset]::UtcNow.ToString('O')
  $receipt | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $resolvedEvidencePath -Encoding UTF8
  $receipt | ConvertTo-Json -Depth 8
  if ($receipt.success -ne $true) { exit 3 }
  exit 0
} finally {
  $adminPlain = $null
  $client.Dispose()
}
