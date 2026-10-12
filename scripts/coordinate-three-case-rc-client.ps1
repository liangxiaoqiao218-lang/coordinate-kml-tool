param(
  [string] $RcBaseUrl,
  [string] $ExpectedCommit,
  [string] $IndonesiaImagePath,
  [string] $EvidencePath,
  [switch] $LibraryOnly
)
$ErrorActionPreference = 'Stop'
$r8Parameters = @{ RcBaseUrl=$RcBaseUrl; ExpectedCommit=$ExpectedCommit; IndonesiaImagePath=$IndonesiaImagePath; EvidencePath=$EvidencePath; LibraryOnly=$LibraryOnly }
# Reuse accepted R2 lifecycle helpers, not its historical replay protocol.
. (Join-Path $PSScriptRoot 'coordinate-consumer-replay-client.ps1') -LibraryOnly
. (Join-Path $PSScriptRoot 'coordinate-three-case-rc-r7-browser-consumer-gate.ps1') -LibraryOnly
. (Join-Path $PSScriptRoot 'coordinate-three-case-rc-result-artifact.ps1')
$RcBaseUrl=$r8Parameters.RcBaseUrl; $ExpectedCommit=$r8Parameters.ExpectedCommit
$IndonesiaImagePath=$r8Parameters.IndonesiaImagePath; $EvidencePath=$r8Parameters.EvidencePath
$LibraryOnly=$r8Parameters.LibraryOnly
$script:ReplayRunId='coordinate-three-case-rc-20261012-r8'
$script:ReplayBatchId='coordinate-three-case-rc-20261012-v8'
$script:R8ImageSha='41f2b2117667fb92f6a4eb703822b1893e29c985be2e14f7b20fbda103b66cf2'

function Invoke-R8ClientEntrypoint {
  param([hashtable] $StateContext, [string] $Commit, [scriptblock] $WaitForRelease,
    [scriptblock] $ReadVersion, [scriptblock] $FlushInput, [scriptblock] $ReadPassword,
    [scriptblock] $RunProtocol,
    [scriptblock] $PersistState={ param($path,$value,$nonce) Write-ReplayAtomicJson $path $value $nonce },
    [scriptblock] $Now={ [DateTimeOffset]::UtcNow })
  $receipt=[ordered]@{ schemaVersion='coordinate_r8_client_receipt_v1'; runId=$script:ReplayRunId;
    batchId=$script:ReplayBatchId; success=$false; phase='PREPARING'; failureCode=''; releaseAccepted=$false;
    runtimeCommitMatch=$false; uploadReleaseCount=0; providerCallCount=$null; recognitionUsageConsumed=$null; failureStage='';
    consumerGateCount=0; closeRequestCount=0; runClosed=$false; resultArtifact=$null; consumer=$null;
    clientExpiresAt=$StateContext.deadline.ToUniversalTime().ToString('O') }
  $transition={ param($phase)
    $receipt.phase=$phase
    Set-ReplayClientState -Context $StateContext -Phase $phase -Persist $PersistState -Now $Now
  }.GetNewClosure()
  $secure=$null
  try {
    & $transition 'PREPARING'
    Get-ReplayRemainingMilliseconds $StateContext.deadline $Now | Out-Null
    & $transition 'WAITING_RELEASE'
    & $WaitForRelease | Out-Null
    Get-ReplayRemainingMilliseconds $StateContext.deadline $Now | Out-Null
    $receipt.releaseAccepted=$true
    & $transition 'RUNTIME_CHECK'
    $version=& $ReadVersion (Get-ReplayRemainingMilliseconds $StateContext.deadline $Now)
    $runtime=$version.runtimeIdentity
    if ($runtime.commit -cne $Commit -or $runtime.coordinateThreeCaseRc.enabled -ne $true -or
        $runtime.coordinateThreeCaseRc.runId -cne $script:ReplayRunId -or
        $runtime.coordinateConsumerReplay.enabled -ne $false) { throw 'RC_R8_RUNTIME_IDENTITY_REJECTED' }
    $receipt.runtimeCommitMatch=$true
    & $FlushInput
    Get-ReplayRemainingMilliseconds $StateContext.deadline $Now | Out-Null
    & $transition 'WAITING_PASSWORD'
    $secure=& $ReadPassword $StateContext.deadline
    Get-ReplayRemainingMilliseconds $StateContext.deadline $Now | Out-Null
    if ($secure -isnot [Security.SecureString] -or $secure.Length -eq 0) { throw 'RC_R8_PASSWORD_UNAVAILABLE' }
    & $transition 'RUNNING'
    & $RunProtocol $secure $receipt $StateContext.deadline $transition | Out-Null
    if (-not $receipt.success -or -not $receipt.runClosed) { throw 'RC_R8_DELIVERY_OR_CLOSE_INCOMPLETE' }
    & $transition 'COMPLETED'
  } catch {
    $receipt.success=$false
    if (-not $receipt.failureStage) { $receipt.failureStage=$receipt.phase }
    $receipt.failureCode='RC_R8_' + $receipt.failureStage + '_FAILED'
    if (-not $StateContext.writeFailed -and $StateContext.phase -notin @('FAILED','COMPLETED')) {
      try { Set-ReplayClientState -Context $StateContext -Phase 'FAILED' -FailureCode 'CONSUMER_REPLAY_HTTP_FAILED' -Persist $PersistState -Now $Now } catch {}
    }
  } finally { if ($secure -is [Security.SecureString]) { $secure.Dispose() } }
  $receipt['clientStateWriteCount']=$StateContext.writeCount
  $receipt['generatedAt']=(& $Now).ToUniversalTime().ToString('O')
  return $receipt
}

function Invoke-R8FreshConsumerProtocol {
  param([scriptblock] $Request, [System.Collections.IDictionary] $Receipt, [DateTimeOffset] $Deadline,
    [scriptblock] $Transition, [string] $Commit, [string] $ResultPath, [string] $BrowserPath,
    [string] $DownloadPath,
    [scriptblock] $Delay={ Start-Sleep -Milliseconds 1500 },
    [scriptblock] $Now={ [DateTimeOffset]::UtcNow },
    [scriptblock] $WriteResult={ param($path,$state) Write-CoordinateThreeCaseValidatedResultArtifact -Path $path -State $state },
    [scriptblock] $WriteBrowser={ param($path,$value) Write-ReplayAtomicJson $path $value ([guid]::NewGuid().ToString('N')) },
    [scriptblock] $Gate={ param($browser,$result,$download,$run,$expires)
      Invoke-R7BrowserConsumerGate -EvidencePath $browser -ResultPath $result -DownloadPath $download -RunId $run -CaseId 'indonesia' -ExpiresAt $expires -WaitSeconds 480 })
  $base='/api/admin/coordinate-products/three-case-rc'
  $stage='BROWSER_READINESS'
  try {
    $ready=& $Request 'POST' "$base/browser-readiness" $null (Get-ReplayRemainingMilliseconds $Deadline $Now)
    if ($ready.browserSessionReady -ne $true) { throw 'RC_R8_BROWSER_NOT_READY' }
    $Receipt.uploadReleaseCount=1
    $stage='UPLOAD_RELEASE'
    $release=& $Request 'POST' "$base/release-upload" @{expiresAt=$Deadline.ToUniversalTime().ToString('O')} (Get-ReplayRemainingMilliseconds $Deadline $Now)
    if ($release.released -ne $true -or $release.runId -cne $script:ReplayRunId) { throw 'RC_R8_UPLOAD_RELEASE_FAILED' }
    Write-Host 'R8_UPLOAD_READY：请在已认证的 RC 页面上传原印尼图一次；勿刷新或重复上传。'
    $stage='FRESH_RECOGNITION'
    do {
      $fresh=& $Request 'GET' "$base/fresh-result" $null (Get-ReplayRemainingMilliseconds $Deadline $Now)
      if ($fresh.stage -eq 'FAILED') { throw 'RC_R8_RECOGNITION_FAILED' }
      if (-not $fresh.ready) { & $Delay }
    } while (-not $fresh.ready)
    $record=New-CoordinateThreeCaseValidatedResultRecord -Definition @{caseId='indonesia';imageSha256=$script:R8ImageSha} -Snapshot $fresh.snapshot -ExpectedModel 'qwen3.8-flash'
    if ($record.success -ne $true -or $record.modelMatch -ne $true -or $record.providerCallCount -ne 1 -or
        $record.finalizedResult.consumerIdentityMatch -ne $true -or $record.finalizedResult.technicalKmlReady -ne $true -or
        $fresh.snapshot.result.recognitionUsageConsumed -isnot [bool]) { throw 'RC_R8_NEW_RESULT_CONTRACT_FAILED' }
    $stage='RESULT_RETENTION'
    $Receipt.providerCallCount=$record.providerCallCount
    $Receipt.recognitionUsageConsumed=$fresh.snapshot.result.recognitionUsageConsumed
    $state=New-CoordinateThreeCaseValidatedResultState -RunId $script:ReplayRunId -BatchId $script:ReplayBatchId -ExpectedCommit $Commit -Cases @($record) -RuntimeCommitMatch $true
    $Receipt.resultArtifact=& $WriteResult $ResultPath $state
    $handoffDeadline=[DateTimeOffset]$fresh.expiresAt
    if ($handoffDeadline -gt $Deadline) { throw 'RC_R8_HANDOFF_DEADLINE_REJECTED' }
    $stage='MAP_OR_DOWNLOAD'
    do {
      $browser=& $Request 'GET' "$base/browser-evidence" $null (Get-ReplayRemainingMilliseconds $handoffDeadline $Now)
      if (-not $browser.ready) { & $Delay }
    } while (-not $browser.ready)
    & $WriteBrowser $BrowserPath $browser.evidence | Out-Null
    $Receipt.consumerGateCount=1
    $stage='PHYSICAL_KML'
    $Receipt.consumer=& $Gate $BrowserPath $ResultPath $DownloadPath $script:ReplayRunId $fresh.expiresAt
    if ($Receipt.consumer.success -ne $true) { throw 'RC_R8_PHYSICAL_DOWNLOAD_NOT_VERIFIED' }
    $Receipt.success=$true
  } catch {
    $Receipt.failureStage=$stage
    throw
  } finally {
    try { & $Transition 'CLOSING' } finally {
      $Receipt.closeRequestCount=1
      try {
        $closed=& $Request 'POST' "$base/close" @{} 30000
        $Receipt.runClosed=$closed.closed -eq $true
      } catch { $Receipt.runClosed=$false }
      if (-not $Receipt.runClosed) {
        $Receipt.success=$false
        if (-not $Receipt.failureStage) { $Receipt.failureStage='CLOSING' }
      }
    }
  }
}

if ($LibraryOnly) { return }
$processStarted=[DateTimeOffset](Get-Process -Id $PID).StartTime.ToUniversalTime()
$deadline=$processStarted.AddMinutes(15)
$clock=[Diagnostics.Stopwatch]::StartNew()
$baseUri=[Uri]$RcBaseUrl
if ($baseUri.Scheme -cne 'https' -or $baseUri.Host -cne 'coordinate-kml-tool-rc.onrender.com' -or
    $baseUri.Port -ne 443 -or $baseUri.UserInfo -or $baseUri.AbsolutePath -ne '/' -or $baseUri.Query -or $baseUri.Fragment -or
    $ExpectedCommit -cnotmatch '^[0-9a-f]{40}$' -or
    (Get-FileHash -LiteralPath $IndonesiaImagePath -Algorithm SHA256).Hash.ToLowerInvariant() -cne $script:R8ImageSha) { throw 'RC_R8_PREFLIGHT_REJECTED' }
$receiptPath=[IO.Path]::GetFullPath($EvidencePath)
$statePath="$receiptPath.state.json"; $releasePath="$receiptPath.release.json"
$resultPath="$receiptPath.validated-results.json"; $browserPath="$receiptPath.browser.json"
$downloadDirectory=Join-Path (Split-Path -Parent $receiptPath) 'r8-browser-download'
$downloadPath=Join-Path $downloadDirectory 'coordinates.kml'
foreach ($path in @($receiptPath,$statePath,$releasePath,$resultPath,$browserPath)) {
  if (Test-Path -LiteralPath $path) { throw 'RC_R8_EXISTING_RECEIPT_REJECTED' }
}
# Prepared before launch, no prior downloads may be adopted.
if (-not (Test-Path -LiteralPath $downloadDirectory -PathType Container) -or
    @(Get-ChildItem -LiteralPath $downloadDirectory -Force).Count -ne 0) { throw 'RC_R8_DOWNLOAD_DIRECTORY_NOT_PREPARED' }
$nonce=[guid]::NewGuid().ToString('N')
$context=New-ReplayClientStateContext $statePath $ExpectedCommit $PID $processStarted $nonce
$wait={
  Write-Host 'R8_CLIENT_STATE=WAITING_RELEASE（暂不输入密码）'
  Write-Host "R8_DOWNLOAD_DIRECTORY=$downloadDirectory"
  Write-Host "R8_CLIENT_STATE_PATH=$statePath"
  Write-Host "R8_RELEASE_CONTROL_PATH=$releasePath"
  Write-Host "R8_CLIENT_DEADLINE=$($deadline.ToString('O'))"
  Wait-ReplayReleaseManifest -Path $releasePath -ExpectedCommit $ExpectedCommit -InstanceNonce $nonce -ProcessId $PID -ProcessStartedAt $processStarted.ToUniversalTime().ToString('O') -PreparationExpiresAt $deadline -PreparationClock $clock
}.GetNewClosure()
$version={ param($timeout)
  $client=[Net.Http.HttpClient]::new()
  try { Invoke-ReplayJsonRequest $client 'GET' "$($baseUri.AbsoluteUri.TrimEnd('/'))/api/version" $null $timeout }
  finally { $client.Dispose() }
}.GetNewClosure()
$protocol={ param($secure,$receipt,$expires,$transition)
  $client=[Net.Http.HttpClient]::new()
  try {
    $plain=ConvertFrom-ReplaySecureString $secure
    $client.DefaultRequestHeaders.Add('x-admin-password',$plain)
    $plain=$null
    $request={ param($method,$path,$body,$timeout)
      Invoke-ReplayJsonRequest $client $method "$($baseUri.AbsoluteUri.TrimEnd('/'))$path" $body $timeout
    }.GetNewClosure()
    Invoke-R8FreshConsumerProtocol -Request $request -Receipt $receipt -Deadline $expires -Transition $transition -Commit $ExpectedCommit -ResultPath $resultPath -BrowserPath $browserPath -DownloadPath $downloadPath
  } finally { $plain=$null; $client.Dispose() }
}.GetNewClosure()
$receipt=Invoke-R8ClientEntrypoint -StateContext $context -Commit $ExpectedCommit -WaitForRelease $wait -ReadVersion $version -FlushInput { Clear-ReplayConsoleInputBuffer } -ReadPassword {
  param($expires)
  $oldControlC=[Console]::TreatControlCAsInput
  try { [Console]::TreatControlCAsInput=$true; Read-ReplayPassword -Deadline $expires }
  finally { [Console]::TreatControlCAsInput=$oldControlC }
} -RunProtocol $protocol
Write-ReplayAtomicJson -Path $receiptPath -Value $receipt -InstanceNonce $nonce
$receipt | ConvertTo-Json -Depth 8
if (-not $receipt.success) { exit 3 }
