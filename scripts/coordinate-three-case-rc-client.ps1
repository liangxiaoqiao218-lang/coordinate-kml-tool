param(
  [Parameter(Mandatory = $true)] [string] $RcBaseUrl,
  [Parameter(Mandatory = $true)] [string] $ExpectedCommit,
  [string] $IndonesiaImagePath,
  [Parameter(Mandatory = $true)] [string] $MgrsImagePath,
  [Parameter(Mandatory = $true)] [string] $BftmImagePath,
  [Parameter(Mandatory = $true)] [string] $EvidencePath
)

$ErrorActionPreference = 'Stop'
$runId = 'coordinate-three-case-rc-20261011-r1'
$batchId = 'coordinate-three-case-rc-20261011-v1'
$caseDefinitions = @(
  [ordered]@{ caseId = 'indonesia'; imageSha256 = '41f2b2117667fb92f6a4eb703822b1893e29c985be2e14f7b20fbda103b66cf2'; productMode = 'indonesia_utm50s_structured_b'; path = $IndonesiaImagePath },
  [ordered]@{ caseId = 'mgrs'; imageSha256 = 'af999328e3232af304e03901c5e8d58cad794ea588ee31709aef6668974f4004'; productMode = ''; path = $MgrsImagePath },
  [ordered]@{ caseId = 'bftm'; imageSha256 = '4567ce9889c47d65414b19e543c06b5322b86c53b76bb53e87da761bc0988e1d'; productMode = ''; path = $BftmImagePath }
)

function Assert-LocalInputFile([hashtable] $definition) {
  if ([string]::IsNullOrWhiteSpace($definition.path)) {
    if ($definition.caseId -ne 'indonesia') { throw "CASE_IMAGE_PATH_MISSING" }
    $definition.path = Read-Host '请输入原印尼图的本地绝对路径'
  }
  $resolved = (Resolve-Path -LiteralPath $definition.path).Path
  $actual = (Get-FileHash -LiteralPath $resolved -Algorithm SHA256).Hash.ToLowerInvariant()
  if ($actual -ne $definition.imageSha256) { throw "CASE_IMAGE_SHA256_MISMATCH_$($definition.caseId.ToUpperInvariant())" }
  $definition.path = $resolved
}

function ConvertFrom-SecureStringPlain([Security.SecureString] $secure) {
  $pointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
  try { return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($pointer) }
  finally { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($pointer) }
}

function Invoke-JsonRequest([Net.Http.HttpClient] $client, [string] $method, [string] $url, [object] $body = $null, [switch] $AllowErrorPayload) {
  $request = [Net.Http.HttpRequestMessage]::new([Net.Http.HttpMethod]::new($method), $url)
  try {
    if ($null -ne $body) {
      $json = $body | ConvertTo-Json -Depth 8 -Compress
      $request.Content = [Net.Http.StringContent]::new($json, [Text.Encoding]::UTF8, 'application/json')
    }
    $response = $client.SendAsync($request).GetAwaiter().GetResult()
    $text = $response.Content.ReadAsStringAsync().GetAwaiter().GetResult()
    $payload = if ([string]::IsNullOrWhiteSpace($text)) { $null } else { $text | ConvertFrom-Json }
    if (-not $response.IsSuccessStatusCode -and -not $AllowErrorPayload) {
      $code = if ($payload.code) { [string]$payload.code } elseif ($payload.reason) { [string]$payload.reason } else { 'HTTP_FAILURE' }
      throw "HTTP_$([int]$response.StatusCode)_$code"
    }
    return $payload
  } finally {
    $request.Dispose()
  }
}

function Invoke-RecognitionJob([Net.Http.HttpClient] $adminClient, [string] $baseUrl, [hashtable] $definition) {
  $requestId = [guid]::NewGuid().ToString().ToLowerInvariant()
  $claim = Invoke-JsonRequest $adminClient 'POST' "$baseUrl/api/admin/coordinate-products/three-case-rc/claims" ([ordered]@{
    runId = $runId; batchId = $batchId; caseId = $definition.caseId
    imageSha256 = $definition.imageSha256; recognitionRequestId = $requestId
  })
  if ($claim.accepted -ne $true -or [string]::IsNullOrWhiteSpace([string]$claim.claimToken)) { throw 'CLAIM_CONTRACT_INVALID' }

  $jobClient = [Net.Http.HttpClient]::new()
  $jobClient.Timeout = [TimeSpan]::FromSeconds(190)
  try {
    $jobClient.DefaultRequestHeaders.Authorization = [Net.Http.Headers.AuthenticationHeaderValue]::new('Bearer', [string]$claim.claimToken)
    $jobClient.DefaultRequestHeaders.Add('x-recognition-request-id', $requestId)
    $jobClient.DefaultRequestHeaders.Add('x-coordinate-rc-case-id', $definition.caseId)
    if ($definition.productMode) { $jobClient.DefaultRequestHeaders.Add('x-coordinate-product-mode', $definition.productMode) }
    $form = [Net.Http.MultipartFormDataContent]::new()
    $stream = [IO.File]::OpenRead($definition.path)
    try {
      $image = [Net.Http.StreamContent]::new($stream)
      $image.Headers.ContentType = [Net.Http.Headers.MediaTypeHeaderValue]::new('image/jpeg')
      $form.Add($image, 'image', [IO.Path]::GetFileName($definition.path))
      if ($definition.productMode) { $form.Add([Net.Http.StringContent]::new($definition.productMode), 'coordinateProductMode') }
      $response = $jobClient.PostAsync("$baseUrl/api/recognize-coordinates/jobs", $form).GetAwaiter().GetResult()
      $text = $response.Content.ReadAsStringAsync().GetAwaiter().GetResult()
      $accepted = $text | ConvertFrom-Json
      if (-not $response.IsSuccessStatusCode) {
        $code = if ($accepted.code) { [string]$accepted.code } elseif ($accepted.reason) { [string]$accepted.reason } else { 'JOB_CREATE_FAILED' }
        throw "HTTP_$([int]$response.StatusCode)_$code"
      }
    } finally {
      $form.Dispose()
      $stream.Dispose()
    }
    if ([string]::IsNullOrWhiteSpace([string]$accepted.jobId) -or [string]::IsNullOrWhiteSpace([string]$accepted.jobAccessToken)) {
      throw 'JOB_ACCEPTANCE_CONTRACT_INVALID'
    }

    $pollClient = [Net.Http.HttpClient]::new()
    $pollClient.Timeout = [TimeSpan]::FromSeconds(15)
    $pollClient.DefaultRequestHeaders.Add('x-recognition-job-token', [string]$accepted.jobAccessToken)
    try {
      $deadline = [DateTimeOffset]::UtcNow.AddSeconds(180)
      do {
        Start-Sleep -Seconds 2
        $snapshot = Invoke-JsonRequest $pollClient 'GET' "$baseUrl/api/recognize-coordinates/jobs/$($accepted.jobId)" $null -AllowErrorPayload
        if ($snapshot.status -in @('SUCCEEDED', 'FAILED')) { break }
      } while ([DateTimeOffset]::UtcNow -lt $deadline)
      if ($snapshot.status -notin @('SUCCEEDED', 'FAILED')) { throw 'JOB_TERMINAL_TIMEOUT' }
      return $snapshot
    } finally {
      $pollClient.Dispose()
    }
  } finally {
    $jobClient.Dispose()
  }
}

function Get-SanitizedCaseReceipt([hashtable] $definition, [object] $snapshot) {
  $result = $snapshot.result
  $finalized = $result.finalizedCoordinateResult
  $diagnostics = $result.indonesiaStructuredPreflightDiagnostics
  $identityPresent = -not [string]::IsNullOrWhiteSpace([string]$finalized.resultId) -and
    ([int64]$finalized.resultRevision -gt 0) -and
    -not [string]::IsNullOrWhiteSpace([string]$finalized.geometryHash)
  return [ordered]@{
    caseId = $definition.caseId
    terminalStatus = [string]$snapshot.status
    httpStatus = [int]$snapshot.httpStatus
    success = $result.success -eq $true
    code = [string]$(if ($result.code) { $result.code } elseif ($result.reason) { $result.reason } else { 'NONE' })
    modelMatch = [string]$result.model -like 'qwen3.8-flash*'
    providerCallCount = [int]$result.providerCallCount
    providerCompletionState = [string]$result.providerCompletionState
    resultIdentityPresent = $identityPresent
    technicalKmlReady = $result.technicalKmlReady -eq $true -or $finalized.technicalKmlReady -eq $true
    kmlReady = $result.kmlReady -eq $true -or $finalized.kmlReady -eq $true
    geometryPresent = $null -ne $finalized.geometry
    diagnosticsPresent = $null -ne $diagnostics
    localOcrAttempted = $diagnostics.localOcrAttempted -eq $true
    localOcrContextPresent = $diagnostics.localOcrContextPresent -eq $true
    explicitUtm50s = $diagnostics.explicitUtm50s -eq $true
    projectedColumns = $diagnostics.projectedColumns -eq $true
    controlledRcHintApplied = $diagnostics.controlledRcHintApplied -eq $true
    preflightPassed = $diagnostics.preflightPassed -eq $true
    preflightFailureCode = [string]$(if ($diagnostics.preflightFailureCode) { $diagnostics.preflightFailureCode } else { 'NONE' })
    routeFailureCode = [string]$(if ($diagnostics.routeFailureCode) { $diagnostics.routeFailureCode } else { 'NONE' })
  }
}

$baseUri = [Uri]$RcBaseUrl
if ($baseUri.Scheme -ne 'https' -or $baseUri.Host -ne 'coordinate-kml-tool-rc.onrender.com' -or $baseUri.Query -or $baseUri.Fragment) {
  throw 'RC_BASE_URL_REJECTED'
}
$resolvedEvidencePath = [IO.Path]::GetFullPath($EvidencePath)
if ($resolvedEvidencePath.StartsWith((Get-Location).Path, [StringComparison]::OrdinalIgnoreCase)) { throw 'EVIDENCE_PATH_MUST_BE_OUTSIDE_REPOSITORY' }
$caseDefinitions | ForEach-Object { Assert-LocalInputFile $_ }

$adminSecure = Read-Host '请输入 RC 管理员密码（不会显示）' -AsSecureString
$adminPlain = ConvertFrom-SecureStringPlain $adminSecure
$client = [Net.Http.HttpClient]::new()
$client.Timeout = [TimeSpan]::FromSeconds(30)
$client.DefaultRequestHeaders.Add('x-admin-password', $adminPlain)
$receipts = [Collections.Generic.List[object]]::new()
$closeReceipt = $null
try {
  $version = Invoke-JsonRequest $client 'GET' "$($baseUri.AbsoluteUri.TrimEnd('/'))/api/version"
  $runtimeCommit = [string]$(if ($version.runtimeIdentity.commit) { $version.runtimeIdentity.commit } else { $version.commit })
  if ($runtimeCommit -ne $ExpectedCommit) { throw 'RC_RUNTIME_COMMIT_MISMATCH' }
  foreach ($definition in $caseDefinitions) {
    try {
      $snapshot = Invoke-RecognitionJob $client $baseUri.AbsoluteUri.TrimEnd('/') $definition
      $receipts.Add((Get-SanitizedCaseReceipt $definition $snapshot))
    } catch {
      $receipts.Add([ordered]@{ caseId = $definition.caseId; terminalStatus = 'CLIENT_STOP'; code = [string]$_.Exception.Message })
    }
  }
} finally {
  try {
    $closeReceipt = Invoke-JsonRequest $client 'POST' "$($baseUri.AbsoluteUri.TrimEnd('/'))/api/admin/coordinate-products/three-case-rc/close" ([ordered]@{})
  } catch {
    $closeReceipt = [ordered]@{ closed = $false; code = [string]$_.Exception.Message }
  }
  $adminPlain = $null
  $client.Dispose()
}

$evidence = [ordered]@{
  schemaVersion = 1
  runId = $runId
  runtimeCommitMatch = $true
  executionCount = 1
  automaticRetries = 0
  providerCallLimit = 3
  cases = $receipts
  runClosed = $closeReceipt.closed -eq $true
  generatedAt = [DateTimeOffset]::UtcNow.ToString('O')
}
$parent = Split-Path -Parent $resolvedEvidencePath
if (-not (Test-Path -LiteralPath $parent)) { New-Item -ItemType Directory -Path $parent | Out-Null }
$evidence | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $resolvedEvidencePath -Encoding UTF8
$evidence | ConvertTo-Json -Depth 8
if (-not $evidence.runClosed) { exit 2 }
