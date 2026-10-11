param(
  [string] $RcBaseUrl,
  [string] $ExpectedCommit,
  [string] $HistoricalArtifactPath,
  [string] $EvidencePath,
  [string] $ReleaseControlPath,
  [string] $ClientStatePath,
  [switch] $LibraryOnly
)

$ErrorActionPreference = 'Stop'
$script:ReplayRunId = 'coordinate-indonesia-consumer-replay-20261012-r2'
$script:ReplayBatchId = 'coordinate-indonesia-consumer-replay-20261012-v2'
$script:ReplayArtifactSha256 = '0de3ed5542962d8203e15e654bc2e908dc659745b72b55c5c1518075f94f3522'
$script:ReplayPreparationTimeoutMilliseconds = 15 * 60 * 1000
$script:ReplayReleasePollMilliseconds = 500
$script:ReplayReleasePollLimit = 1800
$replayClientParameters = @{
  RcBaseUrl = $RcBaseUrl
  ExpectedCommit = $ExpectedCommit
  HistoricalArtifactPath = $HistoricalArtifactPath
  EvidencePath = $EvidencePath
  ReleaseControlPath = $ReleaseControlPath
  ClientStatePath = $ClientStatePath
  LibraryOnly = $LibraryOnly
}
. (Join-Path $PSScriptRoot 'coordinate-consumer-replay-gate.ps1') -LibraryOnly
$RcBaseUrl = $replayClientParameters.RcBaseUrl
$ExpectedCommit = $replayClientParameters.ExpectedCommit
$HistoricalArtifactPath = $replayClientParameters.HistoricalArtifactPath
$EvidencePath = $replayClientParameters.EvidencePath
$ReleaseControlPath = $replayClientParameters.ReleaseControlPath
$ClientStatePath = $replayClientParameters.ClientStatePath
$LibraryOnly = $replayClientParameters.LibraryOnly

function ConvertFrom-ReplaySecureString([Security.SecureString] $Secure) {
  $pointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Secure)
  try { return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($pointer) }
  finally { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($pointer) }
}

# Never serialize arbitrary exceptions (including errors from HTTP or console I/O).
function Get-ReplayFailureCode([string] $Code) {
  $allowed = @(
    'CONSUMER_REPLAY_TOTAL_DEADLINE_EXPIRED', 'CONSUMER_REPLAY_PREPARATION_TIMEOUT',
    'CONSUMER_REPLAY_RELEASE_MANIFEST_REJECTED', 'CONSUMER_REPLAY_RUNTIME_IDENTITY_MISMATCH',
    'CONSUMER_REPLAY_CONSOLE_INPUT_FLUSH_UNAVAILABLE', 'CONSUMER_REPLAY_CONSOLE_INPUT_UNAVAILABLE',
    'CONSUMER_REPLAY_EMPTY_PASSWORD_REJECTED', 'CONSUMER_REPLAY_PASSWORD_CANCELLED',
    'CONSUMER_REPLAY_BROWSER_SESSION_NOT_READY', 'CONSUMER_REPLAY_REGISTRATION_CONTRACT_INVALID',
    'CONSUMER_REPLAY_HANDOFF_DEADLINE_INVALID', 'CONSUMER_REPLAY_HANDOFF_EXPIRED',
    'CONSUMER_REPLAY_BROWSER_TERMINAL_FAILURE', 'CONSUMER_REPLAY_EVIDENCE_TIMEOUT',
    'CONSUMER_REPLAY_GATE_FAILED', 'CONSUMER_REPLAY_CLOSE_FAILED',
    'CONSUMER_REPLAY_STATE_PERSISTENCE_FAILED', 'CONSUMER_REPLAY_STATE_TRANSITION_REJECTED',
    'CONSUMER_REPLAY_STATE_WRITE_LIMIT_EXCEEDED', 'CONSUMER_REPLAY_RECEIPT_WRITE_FAILED',
    'CONSUMER_REPLAY_HTTP_FAILED', 'CONSUMER_REPLAY_EXECUTION_FAILED'
  )
  if ($allowed -ccontains $Code) { return $Code }
  return 'CONSUMER_REPLAY_EXECUTION_FAILED'
}

function Get-ReplayRemainingMilliseconds {
  param([Parameter(Mandatory)] [DateTimeOffset] $Deadline,
    [scriptblock] $Now = { [DateTimeOffset]::UtcNow })
  $remaining = [Math]::Floor(($Deadline - (& $Now)).TotalMilliseconds)
  if ($remaining -le 0) { throw 'CONSUMER_REPLAY_TOTAL_DEADLINE_EXPIRED' }
  return [int][Math]::Min(30000, $remaining)
}

function New-ReplayClientStateContext {
  param([string] $Path, [string] $ExpectedCommit, [int] $ProcessId,
    [DateTimeOffset] $ProcessStartedAt, [string] $InstanceNonce)
  return @{
    path = $Path; expectedCommit = $ExpectedCommit; processId = $ProcessId
    processStartedAt = $ProcessStartedAt; instanceNonce = $InstanceNonce
    deadline = $ProcessStartedAt.AddMinutes(15); phase = ''; writeCount = 0; writeFailed = $false
  }
}

function Set-ReplayClientState {
  param([hashtable] $Context, [string] $Phase, [string] $FailureCode = '',
    [scriptblock] $Persist = { param($path, $value, $nonce) Write-ReplayAtomicJson $path $value $nonce },
    [scriptblock] $Now = { [DateTimeOffset]::UtcNow })
  if ($Context.writeFailed) { throw 'CONSUMER_REPLAY_STATE_PERSISTENCE_FAILED' }
  if ($Context.writeCount -ge 7) { throw 'CONSUMER_REPLAY_STATE_WRITE_LIMIT_EXCEEDED' }
  $next = @{ '' = 'PREPARING'; PREPARING = 'WAITING_RELEASE'; WAITING_RELEASE = 'RUNTIME_CHECK'
    RUNTIME_CHECK = 'WAITING_PASSWORD'; WAITING_PASSWORD = 'RUNNING'; RUNNING = 'CLOSING'; CLOSING = 'COMPLETED' }
  if ($Context.phase -in @('FAILED', 'COMPLETED') -or
      ($Phase -ne 'FAILED' -and $next[$Context.phase] -ne $Phase)) {
    throw 'CONSUMER_REPLAY_STATE_TRANSITION_REJECTED'
  }
  # Count attempts before disk I/O; no retry if persistence fails.
  $Context.writeCount += 1
  $Context.phase = $Phase
  $state = [ordered]@{
    schemaVersion = 'coordinate_consumer_replay_client_state_v1'
    runId = $script:ReplayRunId; batchId = $script:ReplayBatchId; expectedCommit = $Context.expectedCommit
    clientInstanceNonce = $Context.instanceNonce; clientProcessId = $Context.processId
    clientProcessStartedAt = $Context.processStartedAt.ToUniversalTime().ToString('O')
    phase = $Phase; terminal = $Phase -in @('FAILED', 'COMPLETED')
    failureCodePresent = -not [string]::IsNullOrWhiteSpace($FailureCode)
    failureCode = if ($FailureCode) { Get-ReplayFailureCode $FailureCode } else { '' }
    preparationStartedAt = $Context.processStartedAt.ToUniversalTime().ToString('O')
    preparationExpiresAt = $Context.deadline.ToUniversalTime().ToString('O')
    updatedAt = (& $Now).ToUniversalTime().ToString('O'); stateWriteCount = $Context.writeCount
  }
  try { & $Persist $Context.path $state $Context.instanceNonce | Out-Null }
  catch { $Context.writeFailed = $true; throw 'CONSUMER_REPLAY_STATE_PERSISTENCE_FAILED' }
}

# Local read-only check for the executor, immediately before enable/auth/release.
# Expected PID/start/nonce must be captured from this launch, never adopted from a stale file.
function Test-ReplayWaitingClient {
  param([string] $StatePath, [string] $ReceiptPath, [string] $ExpectedCommit,
    [int] $ProcessId, [DateTimeOffset] $ProcessStartedAt, [string] $InstanceNonce,
    [string] $ExpectedPhase = 'WAITING_RELEASE',
    [scriptblock] $ReadProcess = { param($id) Get-Process -Id $id -ErrorAction Stop },
    [scriptblock] $Now = { [DateTimeOffset]::UtcNow })
  try {
    if (Test-Path -LiteralPath $ReceiptPath) { return $false }
    $state = Get-Content -LiteralPath $StatePath -Raw | ConvertFrom-Json -DateKind String
    $process = & $ReadProcess $ProcessId
    $started = [DateTimeOffset]$process.StartTime.ToUniversalTime()
    $deadline = $ProcessStartedAt.AddMinutes(15)
    $current = & $Now
    return $null -ne $process -and -not $process.HasExited -and $process.Id -eq $ProcessId -and
      $started -eq $ProcessStartedAt -and $current -lt $deadline -and $current -ge $ProcessStartedAt -and
      $state.schemaVersion -ceq 'coordinate_consumer_replay_client_state_v1' -and
      $state.runId -ceq $script:ReplayRunId -and $state.batchId -ceq $script:ReplayBatchId -and
      $state.expectedCommit -ceq $ExpectedCommit -and $state.clientProcessId -eq $ProcessId -and
      [DateTimeOffset]$state.clientProcessStartedAt -eq $ProcessStartedAt -and
      $state.clientInstanceNonce -ceq $InstanceNonce -and $InstanceNonce -cmatch '^[0-9a-f]{32}$' -and
      $state.phase -ceq $ExpectedPhase -and $ExpectedPhase -in @('WAITING_RELEASE', 'WAITING_PASSWORD') -and
      $state.terminal -is [bool] -and $state.terminal -eq $false -and
      $state.failureCodePresent -is [bool] -and $state.failureCodePresent -eq $false -and -not $state.failureCode -and
      [DateTimeOffset]$state.preparationExpiresAt -eq $deadline -and
      [DateTimeOffset]$state.updatedAt -ge $ProcessStartedAt -and [DateTimeOffset]$state.updatedAt -le $current -and
      $state.stateWriteCount -eq $(if ($ExpectedPhase -eq 'WAITING_RELEASE') { 2 } else { 4 })
  } catch { return $false }
}

function Write-ReplayAtomicJson {
  param(
    [Parameter(Mandatory = $true)] [string] $Path,
    [Parameter(Mandatory = $true)] [object] $Value,
    [Parameter(Mandatory = $true)] [string] $InstanceNonce
  )
  $resolved = [IO.Path]::GetFullPath($Path)
  $parent = Split-Path -Parent $resolved
  if (-not (Test-Path -LiteralPath $parent -PathType Container)) {
    throw 'CONSUMER_REPLAY_STATE_PARENT_MISSING'
  }
  $temporary = "$resolved.$InstanceNonce.tmp"
  try {
    $json = $Value | ConvertTo-Json -Depth 8
    [IO.File]::WriteAllText($temporary, $json, [Text.UTF8Encoding]::new($false))
    if (Test-Path -LiteralPath $resolved -PathType Leaf) {
      [IO.File]::Replace($temporary, $resolved, [System.Management.Automation.Language.NullString]::Value, $true)
    } else {
      [IO.File]::Move($temporary, $resolved)
    }
  } finally {
    if (Test-Path -LiteralPath $temporary -PathType Leaf) {
      Remove-Item -LiteralPath $temporary -Force
    }
  }
}

function Test-ReplayRuntimeIdentity {
  param(
    [Parameter(Mandatory = $true)] [object] $Version,
    [Parameter(Mandatory = $true)] [string] $ExpectedCommit
  )
  $identity = if ($null -ne $Version.runtimeIdentity) { $Version.runtimeIdentity } else { [pscustomobject]@{} }
  $replay = if ($null -ne $identity.coordinateConsumerReplay) {
    $identity.coordinateConsumerReplay
  } else { [pscustomobject]@{} }
  $commitProperty = $identity.PSObject.Properties['commit']
  $enabledProperty = $replay.PSObject.Properties['enabled']
  $runIdProperty = $replay.PSObject.Properties['runId']
  $commitPresent = $null -ne $commitProperty -and -not [string]::IsNullOrWhiteSpace([string]$identity.commit)
  $enabledPresent = $null -ne $enabledProperty -and $replay.enabled -is [bool]
  $runIdPresent = $null -ne $runIdProperty -and -not [string]::IsNullOrWhiteSpace([string]$replay.runId)
  $commitMatch = $commitPresent -and [string]$identity.commit -eq $ExpectedCommit
  $enabledMatch = $enabledPresent -and $replay.enabled -eq $true
  $runIdMatch = $runIdPresent -and [string]$replay.runId -eq $script:ReplayRunId
  return [ordered]@{
    commitPresent = $commitPresent
    commitMatch = $commitMatch
    replayEnabledPresent = $enabledPresent
    replayEnabledMatch = $enabledMatch
    runIdPresent = $runIdPresent
    runIdMatch = $runIdMatch
    runtimeIdentityGate = $commitMatch -and $enabledMatch -and $runIdMatch
  }
}

function Test-ReplayReleaseManifest {
  param(
    [Parameter(Mandatory = $true)] [object] $Manifest,
    [Parameter(Mandatory = $true)] [string] $ExpectedCommit,
    [Parameter(Mandatory = $true)] [string] $InstanceNonce,
    [Parameter(Mandatory = $true)] [int] $ProcessId,
    [Parameter(Mandatory = $true)] [string] $ProcessStartedAt,
    [Parameter(Mandatory = $true)] [DateTimeOffset] $PreparationExpiresAt,
    [scriptblock] $Now = { [DateTimeOffset]::UtcNow }
  )
  $releasedAt = [DateTimeOffset]::MinValue
  $releasedAtValid = [DateTimeOffset]::TryParse([string]$Manifest.releasedAt,
    [Globalization.CultureInfo]::InvariantCulture,
    [Globalization.DateTimeStyles]::AssumeUniversal, [ref]$releasedAt)
  $startedAt = [DateTimeOffset]::MinValue
  $startedValid = [DateTimeOffset]::TryParse($ProcessStartedAt, [ref]$startedAt)
  $manifestPid = 0
  $pidValid = [int]::TryParse([string]$Manifest.clientProcessId, [ref]$manifestPid)
  $current = & $Now
  return $Manifest.schemaVersion -ceq 'coordinate_consumer_replay_release_v1' -and
    $Manifest.runId -ceq $script:ReplayRunId -and $Manifest.batchId -ceq $script:ReplayBatchId -and
    $Manifest.expectedCommit -ceq $ExpectedCommit -and $Manifest.clientInstanceNonce -ceq $InstanceNonce -and
    $pidValid -and $manifestPid -eq $ProcessId -and $Manifest.clientProcessStartedAt -ceq $ProcessStartedAt -and
    [string]$Manifest.enableDeployId -cmatch '^dep-[a-z0-9]+$' -and
    $Manifest.releaseAuthorized -is [bool] -and $Manifest.releaseAuthorized -eq $true -and
    $startedValid -and $PreparationExpiresAt -eq $startedAt.AddMinutes(15) -and
    $releasedAtValid -and $releasedAt -ge $startedAt -and $releasedAt -le $current -and
    $current -lt $PreparationExpiresAt
}

function Wait-ReplayReleaseManifest {
  param(
    [Parameter(Mandatory = $true)] [string] $Path,
    [Parameter(Mandatory = $true)] [string] $ExpectedCommit,
    [Parameter(Mandatory = $true)] [string] $InstanceNonce,
    [Parameter(Mandatory = $true)] [int] $ProcessId,
    [Parameter(Mandatory = $true)] [string] $ProcessStartedAt,
    [Parameter(Mandatory = $true)] [DateTimeOffset] $PreparationExpiresAt,
    [Parameter(Mandatory = $true)] [Diagnostics.Stopwatch] $PreparationClock,
    [int] $TimeoutMilliseconds = $script:ReplayPreparationTimeoutMilliseconds,
    [int] $PollLimit = $script:ReplayReleasePollLimit,
    [scriptblock] $Delay = { param($milliseconds) Start-Sleep -Milliseconds $milliseconds },
    [scriptblock] $Now = { [DateTimeOffset]::UtcNow }
  )
  $resolved = [IO.Path]::GetFullPath($Path)
  for ($attempt = 0; $attempt -lt $PollLimit; $attempt += 1) {
    $remaining = $TimeoutMilliseconds - [int]$PreparationClock.ElapsedMilliseconds
    if ($remaining -le 0 -or (& $Now) -ge $PreparationExpiresAt.ToUniversalTime()) {
      throw 'CONSUMER_REPLAY_PREPARATION_TIMEOUT'
    }
    if (Test-Path -LiteralPath $resolved -PathType Leaf) {
      try { $manifest = Get-Content -LiteralPath $resolved -Raw | ConvertFrom-Json -DateKind String }
      catch { throw 'CONSUMER_REPLAY_RELEASE_MANIFEST_REJECTED' }
      if (-not (Test-ReplayReleaseManifest -Manifest $manifest -ExpectedCommit $ExpectedCommit `
          -InstanceNonce $InstanceNonce -ProcessId $ProcessId -ProcessStartedAt $ProcessStartedAt `
          -PreparationExpiresAt $PreparationExpiresAt -Now $Now)) {
        throw 'CONSUMER_REPLAY_RELEASE_MANIFEST_REJECTED'
      }
      return $manifest
    }
    & $Delay ([Math]::Min($script:ReplayReleasePollMilliseconds, $remaining))
  }
  throw 'CONSUMER_REPLAY_PREPARATION_TIMEOUT'
}

function Clear-ReplayConsoleInputBuffer {
  if (-not ('ReplayConsole.NativeMethods' -as [type])) {
    Add-Type -Namespace ReplayConsole -Name NativeMethods -MemberDefinition @'
[System.Runtime.InteropServices.DllImport("kernel32.dll", SetLastError = true)]
public static extern System.IntPtr GetStdHandle(int nStdHandle);
[System.Runtime.InteropServices.DllImport("kernel32.dll", SetLastError = true)]
[return: System.Runtime.InteropServices.MarshalAs(System.Runtime.InteropServices.UnmanagedType.Bool)]
public static extern bool FlushConsoleInputBuffer(System.IntPtr hConsoleInput);
'@
  }
  $handle = [ReplayConsole.NativeMethods]::GetStdHandle(-10)
  if ($handle -eq [IntPtr]::Zero -or $handle -eq [IntPtr](-1) -or
      -not [ReplayConsole.NativeMethods]::FlushConsoleInputBuffer($handle)) {
    throw 'CONSUMER_REPLAY_CONSOLE_INPUT_FLUSH_UNAVAILABLE'
  }
}

function Read-ReplayPassword {
  param([Parameter(Mandatory)] [DateTimeOffset] $Deadline,
    [scriptblock] $Now = { [DateTimeOffset]::UtcNow },
    [scriptblock] $KeyAvailable = { [Console]::KeyAvailable },
    [scriptblock] $ReadKey = { [Console]::ReadKey($true) },
    [scriptblock] $Render = { param($text) [Console]::Write($text) },
    [scriptblock] $Delay = { param($ms) Start-Sleep -Milliseconds $ms })
  # Unlike blocking Read-Host, this loop can stop at the original process deadline.
  # It is called only after release, public identity verification and native flush.
  $secure = [Security.SecureString]::new()
  $completed = $false
  try {
    & $Render '请输入 RC 管理员密码（遮蔽输入）：'
    while ($true) {
      $remaining = Get-ReplayRemainingMilliseconds -Deadline $Deadline -Now $Now
      if (-not (& $KeyAvailable)) { & $Delay ([Math]::Min(50, $remaining)); continue }
      $key = & $ReadKey
      Get-ReplayRemainingMilliseconds -Deadline $Deadline -Now $Now | Out-Null
      if ($key.Key -eq [ConsoleKey]::Enter) {
        & $Render ([Environment]::NewLine)
        if ($secure.Length -eq 0) { throw 'CONSUMER_REPLAY_EMPTY_PASSWORD_REJECTED' }
        $secure.MakeReadOnly(); $completed = $true
        return $secure
      }
      if ($key.Key -eq [ConsoleKey]::Escape -or
          ($key.Key -eq [ConsoleKey]::C -and ($key.Modifiers -band [ConsoleModifiers]::Control))) {
        throw 'CONSUMER_REPLAY_PASSWORD_CANCELLED'
      }
      if ($key.Key -eq [ConsoleKey]::Backspace) {
        if ($secure.Length -gt 0) { $secure.RemoveAt($secure.Length - 1); & $Render "`b `b" }
      } elseif (-not [char]::IsControl($key.KeyChar)) {
        $secure.AppendChar($key.KeyChar); & $Render '*'
      }
      $key = $null
    }
  } finally { if (-not $completed) { $secure.Dispose() } }
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
    $text = $response.Content.ReadAsStringAsync($cancellation.Token).GetAwaiter().GetResult()
    $payload = if ([string]::IsNullOrWhiteSpace($text)) { $null } else { $text | ConvertFrom-Json -DateKind String }
    if (-not $response.IsSuccessStatusCode) {
      throw 'CONSUMER_REPLAY_HTTP_FAILED'
    }
    return $payload
  } catch { throw 'CONSUMER_REPLAY_HTTP_FAILED' } finally {
    if ($null -ne $response) { $response.Dispose() }
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

function New-ReplayReceipt {
  param([object] $SourceArtifact)
  return [ordered]@{
    success = $false; evidenceClass = 'HISTORICAL_RESULT_CONSUMER_REPLAY_ONLY'
    runId = $script:ReplayRunId; batchId = $script:ReplayBatchId
    artifactHashMatch = $SourceArtifact.Sha256 -eq $script:ReplayArtifactSha256
    registrationAttemptCount = 0; registrationCount = 0; browserEvidencePostCount = 'UNKNOWN'
    mapOpenCount = 'UNKNOWN'; kmlDownloadCount = 'UNKNOWN'; kmlParsed = $false
    kmlGeometryMatch = $false; kmlSha256 = ''; replayCloseRequestCount = 0; replayClosed = $false
    lastStage = 'NOT_STARTED'; failureCodePresent = $false; failureCode = ''; cleanupFailureCode = ''
    statePersistenceFailed = $false; finalReceiptWriteCount = 0; finalReceiptPersisted = $false
    providerCalls = 0; modelCalls = 0; recognitionClaims = 0; recognitionDispatches = 0
    databaseWrites = 0; businessQuotaDeductions = 0; automaticRetries = 0
  }
}

function Invoke-CoordinateConsumerReplayProtocol {
  param(
    [Parameter(Mandatory)] [scriptblock] $Request,
    [Parameter(Mandatory)] [scriptblock] $RunGate,
    [Parameter(Mandatory)] [object] $SourceArtifact,
    [Parameter(Mandatory)] [string] $KmlPath,
    [Parameter(Mandatory)] [DateTimeOffset] $Deadline,
    [System.Collections.IDictionary] $Receipt = (New-ReplayReceipt $SourceArtifact),
    [scriptblock] $StateTransition = { param($phase, $failureCode) },
    [scriptblock] $Now = { [DateTimeOffset]::UtcNow },
    [scriptblock] $Delay = { param($ms) Start-Sleep -Milliseconds $ms }
  )
  $gateReceipt = $null
  try {
    $timeout = Get-ReplayRemainingMilliseconds $Deadline $Now
    $readiness = & $Request 'POST' '/api/admin/coordinate-consumer-replay/browser-readiness' ([ordered]@{}) $timeout
    Get-ReplayRemainingMilliseconds $Deadline $Now | Out-Null
    if ($readiness.browserSessionReady -ne $true) { throw 'CONSUMER_REPLAY_BROWSER_SESSION_NOT_READY' }
    $timeout = Get-ReplayRemainingMilliseconds $Deadline $Now
    $Receipt.registrationAttemptCount = 1
    $registration = & $Request 'POST' '/api/admin/coordinate-consumer-replay/register' ([ordered]@{
      artifactSha256 = $SourceArtifact.Sha256
      artifactBase64 = [Convert]::ToBase64String($SourceArtifact.Bytes)
    }) $timeout
    if ($registration.registered -ne $true -or $registration.runId -cne $script:ReplayRunId -or
        $registration.batchId -cne $script:ReplayBatchId -or $registration.caseId -cne 'indonesia' -or
        [string]$registration.bindingDigest -cnotmatch '^[0-9a-f]{64}$') {
      throw 'CONSUMER_REPLAY_REGISTRATION_CONTRACT_INVALID'
    }
    $Receipt.registrationCount = 1
    Get-ReplayRemainingMilliseconds $Deadline $Now | Out-Null
    $handoffDeadline = [DateTimeOffset]::MinValue
    if (-not [DateTimeOffset]::TryParse([string]$registration.expiresAt, [ref]$handoffDeadline) -or
        $handoffDeadline -gt (& $Now).AddMinutes(8)) {
      throw 'CONSUMER_REPLAY_HANDOFF_DEADLINE_INVALID'
    }
    if ($handoffDeadline -lt $Deadline) { $Deadline = $handoffDeadline }
    $Receipt['effectiveExpiresAt'] = $Deadline.ToUniversalTime().ToString('O')
    for ($poll = 0; $poll -lt 960; $poll += 1) {
      $timeout = Get-ReplayRemainingMilliseconds $Deadline $Now
      $evidenceReceipt = & $Request 'GET' '/api/admin/coordinate-consumer-replay/evidence' $null $timeout
      Get-ReplayRemainingMilliseconds $Deadline $Now | Out-Null
      $stage = [string]$evidenceReceipt.lastStage
      $Receipt.lastStage = if ($stage -cin @('WAITING_FOR_REGISTRATION', 'REGISTERED', 'RESULT_RECEIVED',
          'RESULT_INSTALLED', 'MAP_READY', 'KML_TRIGGERED', 'EVIDENCE_ACCEPTED', 'RECEIVE_FAILED',
          'RESULT_INSTALL_FAILED', 'MAP_FAILED', 'KML_FAILED', 'EVIDENCE_POST_FAILED', 'CLOSED')) { $stage } else { 'UNKNOWN' }
      if (-not [string]::IsNullOrWhiteSpace([string]$evidenceReceipt.failureCode)) {
        throw 'CONSUMER_REPLAY_BROWSER_TERMINAL_FAILURE'
      }
      if ($evidenceReceipt.ready -eq $true -and $null -ne $evidenceReceipt.evidence) {
        $Receipt.browserEvidencePostCount = 1
        try { $gateReceipt = & $RunGate $evidenceReceipt.evidence $SourceArtifact.Path $KmlPath $Receipt.effectiveExpiresAt $registration.bindingDigest }
        catch {
          if ($_.Exception.Message -ceq 'CONSUMER_REPLAY_HANDOFF_EXPIRED') { throw 'CONSUMER_REPLAY_HANDOFF_EXPIRED' }
          throw 'CONSUMER_REPLAY_GATE_FAILED'
        }
        Get-ReplayRemainingMilliseconds $Deadline $Now | Out-Null
        if ($gateReceipt.success -ne $true) { throw 'CONSUMER_REPLAY_GATE_FAILED' }
        $Receipt.mapOpenCount = if ($gateReceipt.mapReady -eq $true) { 1 } else { 'UNKNOWN' }
        $Receipt.kmlDownloadCount = if ($gateReceipt.kmlParsed -eq $true) { 1 } else { 'UNKNOWN' }
        $Receipt.kmlParsed = $gateReceipt.kmlParsed -eq $true
        $Receipt.kmlGeometryMatch = $gateReceipt.kmlGeometryMatch -eq $true
        $Receipt.kmlSha256 = if ([string]$gateReceipt.kmlSha256 -cmatch '^[0-9a-f]{64}$') { $gateReceipt.kmlSha256 } else { '' }
        break
      }
      & $Delay ([Math]::Min(500, (Get-ReplayRemainingMilliseconds $Deadline $Now)))
    }
    if ($null -eq $gateReceipt) { throw 'CONSUMER_REPLAY_EVIDENCE_TIMEOUT' }
  } catch {
    $Receipt.failureCode = Get-ReplayFailureCode ([string]$_.Exception.Message)
  } finally {
    # Cleanup is independent of the execution deadline AND of state-file persistence.
    if ($Receipt.registrationAttemptCount -eq 1 -and $Receipt.replayCloseRequestCount -eq 0) {
      try { & $StateTransition 'CLOSING' '' | Out-Null }
      catch {
        $Receipt.statePersistenceFailed = $true
        $Receipt.cleanupFailureCode = 'CONSUMER_REPLAY_STATE_PERSISTENCE_FAILED'
        if (-not $Receipt.failureCode) { $Receipt.failureCode = $Receipt.cleanupFailureCode }
      }
      $Receipt.replayCloseRequestCount = 1
      try {
        $closed = & $Request 'POST' '/api/admin/coordinate-consumer-replay/close' ([ordered]@{}) 30000
        $Receipt.replayClosed = $closed.closed -eq $true
      } catch { $Receipt.replayClosed = $false }
      if (-not $Receipt.replayClosed) {
        $Receipt.cleanupFailureCode = 'CONSUMER_REPLAY_CLOSE_FAILED'
        if (-not $Receipt.failureCode) { $Receipt.failureCode = $Receipt.cleanupFailureCode }
      }
    }
  }
  $Receipt.failureCodePresent = -not [string]::IsNullOrWhiteSpace($Receipt.failureCode)
  $Receipt.success = -not $Receipt.failureCodePresent -and $gateReceipt.success -eq $true -and $Receipt.replayClosed
  return $Receipt
}

function Invoke-CoordinateConsumerReplayClientEntrypoint {
  param(
    [Parameter(Mandatory)] [object] $SourceArtifact,
    [Parameter(Mandatory)] [string] $ExpectedCommit,
    [Parameter(Mandatory)] [string] $KmlPath,
    [Parameter(Mandatory)] [hashtable] $StateContext,
    [Parameter(Mandatory)] [scriptblock] $WaitForRelease,
    [Parameter(Mandatory)] [scriptblock] $ReadRuntimeVersion,
    [Parameter(Mandatory)] [scriptblock] $FlushInputBuffer,
    [Parameter(Mandatory)] [scriptblock] $ReadPassword,
    [Parameter(Mandatory)] [scriptblock] $RunProtocol,
    [scriptblock] $PersistState = { param($path, $value, $nonce) Write-ReplayAtomicJson $path $value $nonce },
    [scriptblock] $PersistReceipt = { param($receipt) },
    [scriptblock] $Now = { [DateTimeOffset]::UtcNow }
  )
  $receipt = New-ReplayReceipt $SourceArtifact
  $receipt['releaseAccepted'] = $false
  $diagnostic = [ordered]@{
    commitPresent = $false; commitMatch = $false; replayEnabledPresent = $false
    replayEnabledMatch = $false; runIdPresent = $false; runIdMatch = $false; runtimeIdentityGate = $false
  }
  $transition = {
    param($phase, $failureCode)
    Set-ReplayClientState -Context $StateContext -Phase $phase -FailureCode $failureCode -Persist $PersistState -Now $Now
  }.GetNewClosure()
  $adminSecure = $null
  try {
    & $transition 'PREPARING' ''
    Get-ReplayRemainingMilliseconds $StateContext.deadline $Now | Out-Null
    & $transition 'WAITING_RELEASE' ''
    & $WaitForRelease | Out-Null
    Get-ReplayRemainingMilliseconds $StateContext.deadline $Now | Out-Null
    $receipt.releaseAccepted = $true
    & $transition 'RUNTIME_CHECK' ''
    $version = & $ReadRuntimeVersion (Get-ReplayRemainingMilliseconds $StateContext.deadline $Now)
    Get-ReplayRemainingMilliseconds $StateContext.deadline $Now | Out-Null
    $diagnostic = Test-ReplayRuntimeIdentity -Version $version -ExpectedCommit $ExpectedCommit
    if (-not $diagnostic.runtimeIdentityGate) { throw 'CONSUMER_REPLAY_RUNTIME_IDENTITY_MISMATCH' }
    & $FlushInputBuffer
    Get-ReplayRemainingMilliseconds $StateContext.deadline $Now | Out-Null
    & $transition 'WAITING_PASSWORD' ''
    $adminSecure = & $ReadPassword $StateContext.deadline
    Get-ReplayRemainingMilliseconds $StateContext.deadline $Now | Out-Null
    if ($null -eq $adminSecure -or $adminSecure -isnot [Security.SecureString] -or $adminSecure.Length -le 0) {
      throw 'CONSUMER_REPLAY_EMPTY_PASSWORD_REJECTED'
    }
    & $transition 'RUNNING' ''
    # The one mutable receipt survives exceptions after any attempted operation.
    & $RunProtocol $adminSecure $transition $receipt $StateContext.deadline | Out-Null
  } catch {
    $receipt.success = $false
    $receipt.failureCode = Get-ReplayFailureCode ([string]$_.Exception.Message)
  } finally {
    if ($adminSecure -is [Security.SecureString]) { $adminSecure.Dispose() }
  }
  $receipt.statePersistenceFailed = $receipt.statePersistenceFailed -or $StateContext.writeFailed
  if (-not $StateContext.writeFailed -and $StateContext.phase -notin @('FAILED', 'COMPLETED')) {
    try {
      if ($receipt.success) { & $transition 'COMPLETED' '' }
      else { & $transition 'FAILED' $receipt.failureCode }
    } catch {
      $receipt.success = $false
      $receipt.statePersistenceFailed = $true
      if (-not $receipt.failureCode) { $receipt.failureCode = Get-ReplayFailureCode ([string]$_.Exception.Message) }
    }
  }
  foreach ($key in $diagnostic.Keys) { $receipt[$key] = $diagnostic[$key] }
  $receipt['runtimeCommitMatch'] = $diagnostic.commitMatch
  $receipt['clientStateWriteCount'] = $StateContext.writeCount
  $receipt['clientInstancePresent'] = $true
  $receipt['generatedAt'] = (& $Now).ToUniversalTime().ToString('O')
  $receipt['clientExpiresAt'] = $StateContext.deadline.ToUniversalTime().ToString('O')
  $receipt.failureCodePresent = -not [string]::IsNullOrWhiteSpace($receipt.failureCode)
  $receipt.finalReceiptWriteCount = 1
  $receipt.finalReceiptPersisted = $true
  try { & $PersistReceipt $receipt | Out-Null }
  catch {
    # Never retry this write or erase the counts; sanitized console receipt remains available.
    $receipt.finalReceiptPersisted = $false
    $receipt.success = $false
    $receipt.failureCodePresent = $true
    $receipt.failureCode = 'CONSUMER_REPLAY_RECEIPT_WRITE_FAILED'
  }
  return $receipt
}
if ($LibraryOnly) { return }

# Process creation, not completion of preflight, is the origin of the single 15-minute budget.
$processStarted = [DateTimeOffset](Get-Process -Id $PID).StartTime.ToUniversalTime()
$preparationExpiresAt = $processStarted.AddMinutes(15)
$preparationClock = [Diagnostics.Stopwatch]::StartNew()
try {
  Get-ReplayRemainingMilliseconds $preparationExpiresAt | Out-Null
  $baseUri = [Uri]$RcBaseUrl
  if ($baseUri.Scheme -ne 'https' -or $baseUri.Host -ne 'coordinate-kml-tool-rc.onrender.com' -or
      $baseUri.UserInfo -or $baseUri.AbsolutePath -ne '/' -or $baseUri.Query -or $baseUri.Fragment -or
      $ExpectedCommit -cnotmatch '^[0-9a-f]{40}$') { throw 'CONSUMER_REPLAY_PREFLIGHT_FAILED' }
  $source = Test-ReplaySourceArtifact -Path $HistoricalArtifactPath
  $resolvedEvidencePath = [IO.Path]::GetFullPath($EvidencePath)
  $evidenceParent = Split-Path -Parent $resolvedEvidencePath
  if (-not (Test-Path -LiteralPath $evidenceParent)) { New-Item -ItemType Directory -Path $evidenceParent | Out-Null }
  $resolvedReleaseControlPath = if ([string]::IsNullOrWhiteSpace($ReleaseControlPath)) {
    "$resolvedEvidencePath.release.json"
  } else { [IO.Path]::GetFullPath($ReleaseControlPath) }
  $resolvedClientStatePath = if ([string]::IsNullOrWhiteSpace($ClientStatePath)) {
    "$resolvedEvidencePath.state.json"
  } else { [IO.Path]::GetFullPath($ClientStatePath) }
  $downloadDirectory = Join-Path $evidenceParent 'coordinate-consumer-replay-download'
  $kmlDownloadPath = Join-Path $downloadDirectory 'coordinates.kml'
  $uniquePaths = @($resolvedEvidencePath, $resolvedReleaseControlPath, $resolvedClientStatePath) | Sort-Object -Unique
  if ($uniquePaths.Count -ne 3 -or (Test-Path -LiteralPath $resolvedEvidencePath) -or
      (Test-Path -LiteralPath $downloadDirectory) -or (Test-Path -LiteralPath $resolvedReleaseControlPath) -or
      (Test-Path -LiteralPath $resolvedClientStatePath)) { throw 'CONSUMER_REPLAY_PREFLIGHT_FAILED' }
  Get-ReplayRemainingMilliseconds $preparationExpiresAt | Out-Null
  New-Item -ItemType Directory -Path $downloadDirectory | Out-Null
} catch {
  [ordered]@{ success = $false; failureCode = 'CONSUMER_REPLAY_PREFLIGHT_FAILED'; clientStarted = $true;
    registrationAttemptCount = 0; replayCloseRequestCount = 0 } | ConvertTo-Json -Compress
  exit 3
}
$instanceNonce = [guid]::NewGuid().ToString('N')
$processStartedAt = $processStarted.ToUniversalTime().ToString('O')
$stateContext = New-ReplayClientStateContext -Path $resolvedClientStatePath -ExpectedCommit $ExpectedCommit `
  -ProcessId $PID -ProcessStartedAt $processStarted -InstanceNonce $instanceNonce
$waitForRelease = {
  Write-Host 'CONSUMER_REPLAY_CLIENT_STATE=WAITING_RELEASE（暂不输入密码）'
  Write-Host "CONSUMER_REPLAY_DOWNLOAD_DIRECTORY=$downloadDirectory"
  Write-Host "CONSUMER_REPLAY_CLIENT_STATE_PATH=$resolvedClientStatePath"
  Write-Host "CONSUMER_REPLAY_RELEASE_CONTROL_PATH=$resolvedReleaseControlPath"
  Write-Host "CONSUMER_REPLAY_CLIENT_DEADLINE=$($preparationExpiresAt.ToString('O'))"
  Wait-ReplayReleaseManifest -Path $resolvedReleaseControlPath -ExpectedCommit $ExpectedCommit `
    -InstanceNonce $instanceNonce -ProcessId $PID -ProcessStartedAt $processStartedAt `
    -PreparationExpiresAt $preparationExpiresAt -PreparationClock $preparationClock
}.GetNewClosure()
$readRuntimeVersion = {
  param($timeout)
  $versionClient = [Net.Http.HttpClient]::new()
  try {
    $versionClient.Timeout = [TimeSpan]::FromSeconds(30)
    return Invoke-ReplayJsonRequest -Client $versionClient -Method 'GET' `
      -Url "$($baseUri.AbsoluteUri.TrimEnd('/'))/api/version" -TimeoutMilliseconds $timeout
  } finally { $versionClient.Dispose() }
}.GetNewClosure()
$flushInputBuffer = { Clear-ReplayConsoleInputBuffer }
$readPassword = {
  param($deadline)
  if ([Console]::IsInputRedirected) { throw 'CONSUMER_REPLAY_CONSOLE_INPUT_UNAVAILABLE' }
  $oldControlC = [Console]::TreatControlCAsInput
  try {
    [Console]::TreatControlCAsInput = $true
    Read-ReplayPassword -Deadline $deadline
  } finally { [Console]::TreatControlCAsInput = $oldControlC }
}
$boundReplayRunId = $script:ReplayRunId
$boundReplayBatchId = $script:ReplayBatchId
$boundReplayArtifactSha = $script:ReplayArtifactSha256
$runProtocol = {
  param($adminSecure, $stateTransition, $receipt, $deadline)
  $adminPlain = ConvertFrom-ReplaySecureString $adminSecure
  $client = [Net.Http.HttpClient]::new()
  try {
    $client.Timeout = [TimeSpan]::FromSeconds(30)
    $client.DefaultRequestHeaders.Add('x-admin-password', $adminPlain)
    $adminPlain = $null
    $requestBase = $baseUri.AbsoluteUri.TrimEnd('/')
    $gateRunId = $boundReplayRunId
    $gateBatchId = $boundReplayBatchId
    $gateArtifactSha = $boundReplayArtifactSha
    $request = {
      param($method, $path, $body, $timeout)
      Invoke-ReplayJsonRequest -Client $client -Method $method `
        -Url "$requestBase$path" -Body $body -TimeoutMilliseconds $timeout
    }.GetNewClosure()
    $runGate = {
      param($browserEvidence, $artifactPath, $kmlPath, $expiresAt, $expectedBindingDigest)
      Invoke-CoordinateConsumerReplayGate -EvidenceObject $browserEvidence `
        -ArtifactPath $artifactPath -DownloadPath $kmlPath -RunId $gateRunId `
        -BatchId $gateBatchId -ArtifactSha256 $gateArtifactSha `
        -ExpectedBindingDigest $expectedBindingDigest -ExpiresAt $expiresAt
    }.GetNewClosure()
    Invoke-CoordinateConsumerReplayProtocol -Request $request -RunGate $runGate `
      -SourceArtifact $source -KmlPath $kmlDownloadPath -StateTransition $stateTransition -Receipt $receipt -Deadline $deadline
  } finally { $adminPlain = $null; $client.Dispose() }
}.GetNewClosure()
$persistReceipt = {
  param($receipt)
  if (Test-Path -LiteralPath $resolvedEvidencePath) { throw 'CONSUMER_REPLAY_RECEIPT_WRITE_FAILED' }
  Write-ReplayAtomicJson -Path $resolvedEvidencePath -Value $receipt -InstanceNonce $instanceNonce
}.GetNewClosure()
$receipt = Invoke-CoordinateConsumerReplayClientEntrypoint -SourceArtifact $source -ExpectedCommit $ExpectedCommit `
  -KmlPath $kmlDownloadPath -StateContext $stateContext -WaitForRelease $waitForRelease `
  -ReadRuntimeVersion $readRuntimeVersion -FlushInputBuffer $flushInputBuffer -ReadPassword $readPassword `
  -RunProtocol $runProtocol -PersistReceipt $persistReceipt
$receipt | ConvertTo-Json -Depth 8
if ($receipt.success -ne $true) { exit 3 }
exit 0
