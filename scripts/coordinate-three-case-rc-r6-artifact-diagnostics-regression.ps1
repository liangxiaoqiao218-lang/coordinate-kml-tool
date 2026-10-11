$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'coordinate-three-case-rc-result-artifact.ps1')

$passed = 0
$failed = 0
$results = [Collections.Generic.List[object]]::new()
function Assert-Case([string] $Name, [scriptblock] $Body) {
  try {
    & $Body
    $script:passed += 1
    $script:results.Add([ordered]@{ name = $Name; status = 'PASS' })
  } catch {
    $script:failed += 1
    $script:results.Add([ordered]@{ name = $Name; status = 'FAIL'; code = [string]$_.Exception.Message })
  }
}
function Require([bool] $Condition, [string] $Code) {
  if (-not $Condition) { throw $Code }
}

$hash = 'sha256:' + ('a' * 64)
$baseResult = [ordered]@{
  success = $true
  model = 'qwen3.8-flash'
  providerModel = 'qwen3.8-flash'
  adapterLabel = 'structured-product-bridge'
  providerCallCount = 1
  providerCompletionState = 'SUCCEEDED'
  finalizedCoordinateResult = [ordered]@{
    schemaVersion = 'finalized_coordinate_result_v1'
    resultId = 'offline-result'
    resultRevision = 1
    currentRevision = 1
    geometryHash = $hash
    coordinateType = 'indonesia_utm50_projected'
    precisionMode = 'indonesia-utm50s-structured-projected'
    family = 'indonesia_utm50_projected'
    crs = [ordered]@{ id = 'EPSG:4326'; axisOrder = 'longitude_latitude' }
    geometry = [ordered]@{ type = 'Point'; coordinates = @(110, -7) }
    confirmationStatus = 'pending'
    qualityGateStatus = 'review_required'
    decisionState = 'REVIEW_REQUIRED'
    requiresReview = $true
    technicalKmlReady = $true
    kmlReady = $false
  }
  outputCapabilities = [ordered]@{
    technicallyGeneratable = $true
    resultId = 'offline-result'
    resultRevision = 1
    geometryHash = $hash
    mapReady = $true
    kmlReady = $true
    blockReasons = @()
    warningReasons = @('REVIEW_REQUIRED')
  }
}
$definition = [ordered]@{ caseId = 'offline'; imageSha256 = ('b' * 64) }
function New-Snapshot([object] $Result) {
  return [pscustomobject]@{ status = 'SUCCEEDED'; httpStatus = 200; result = [pscustomobject]$Result }
}
function Copy-Result([object] $Result) {
  return $Result | ConvertTo-Json -Depth 20 | ConvertFrom-Json -AsHashtable
}

Assert-Case 'matching capability is consumer-ready while formal authorization stays unknown' {
  $record = New-CoordinateThreeCaseValidatedResultRecord -Definition $definition -Snapshot (New-Snapshot $baseResult) -ExpectedModel 'qwen3.8-flash'
  Require ($record.finalizedResult.outputCapabilityState -eq 'PRESENT_MATCH') 'CAPABILITY_MATCH_NOT_RETAINED'
  Require ($record.finalizedResult.consumerIdentityMatch -eq $true) 'CONSUMER_IDENTITY_MATCH_MISSING'
  Require ($record.finalizedResult.consumerMapReady -eq $true) 'CONSUMER_MAP_NOT_READY'
  Require ($record.finalizedResult.consumerKmlReady -eq $true) 'CONSUMER_KML_NOT_READY'
  Require ($record.finalizedResult.finalizedKmlReady -eq $false) 'FINALIZED_KML_SEMANTICS_CHANGED'
  Require ($record.finalizedResult.formalAuthorizationState -eq 'UNKNOWN') 'FORMAL_AUTHORITY_INVENTED'
  Require ($record.finalizedResult.requiresReview -eq $true) 'REVIEW_REQUIREMENT_REMOVED'
}

Assert-Case 'missing capability remains missing rather than mismatch or denial' {
  $missing = Copy-Result $baseResult
  $missing.Remove('outputCapabilities')
  $record = New-CoordinateThreeCaseValidatedResultRecord -Definition $definition -Snapshot (New-Snapshot $missing) -ExpectedModel 'qwen3.8-flash'
  Require ($record.finalizedResult.outputCapabilityState -eq 'MISSING') 'MISSING_CAPABILITY_RECLASSIFIED'
  Require ($null -eq $record.finalizedResult.consumerIdentityMatch) 'MISSING_CAPABILITY_MARKED_MISMATCH'
  Require ($null -eq $record.finalizedResult.outputCapabilityDenied) 'MISSING_CAPABILITY_MARKED_DENIED'
  Require ($record.finalizedResult.consumerKmlReady -eq $false) 'MISSING_CAPABILITY_CONSUMED'
}

Assert-Case 'missing finalized identity remains unknown rather than denied' {
  $missingIdentity = Copy-Result $baseResult
  $missingIdentity.finalizedCoordinateResult.Remove('resultId')
  $record = New-CoordinateThreeCaseValidatedResultRecord -Definition $definition -Snapshot (New-Snapshot $missingIdentity) -ExpectedModel 'qwen3.8-flash'
  Require ($record.finalizedResult.outputCapabilityState -eq 'PRESENT_IDENTITY_UNKNOWN') 'MISSING_IDENTITY_RECLASSIFIED'
  Require ($null -eq $record.finalizedResult.consumerIdentityMatch) 'MISSING_IDENTITY_MARKED_MISMATCH'
  Require ($record.finalizedResult.consumerMapReady -eq $false) 'MISSING_IDENTITY_CONSUMED'
  Require ($record.finalizedResult.consumerKmlReady -eq $false) 'MISSING_IDENTITY_EXPORTED'
}

Assert-Case 'identity mismatch and explicit denial are distinct states' {
  $mismatch = $baseResult | ConvertTo-Json -Depth 20 | ConvertFrom-Json -AsHashtable
  $mismatch.outputCapabilities.geometryHash = 'sha256:' + ('c' * 64)
  $mismatchRecord = New-CoordinateThreeCaseValidatedResultRecord -Definition $definition -Snapshot (New-Snapshot $mismatch) -ExpectedModel 'qwen3.8-flash'
  Require ($mismatchRecord.finalizedResult.outputCapabilityState -eq 'PRESENT_MISMATCH') 'MISMATCH_NOT_DISTINCT'
  Require ($mismatchRecord.finalizedResult.consumerIdentityMatch -eq $false) 'MISMATCH_NOT_RETAINED'

  $denied = $baseResult | ConvertTo-Json -Depth 20 | ConvertFrom-Json -AsHashtable
  $denied.outputCapabilities.technicallyGeneratable = $false
  $denied.outputCapabilities.mapReady = $false
  $denied.outputCapabilities.kmlReady = $false
  $deniedRecord = New-CoordinateThreeCaseValidatedResultRecord -Definition $definition -Snapshot (New-Snapshot $denied) -ExpectedModel 'qwen3.8-flash'
  Require ($deniedRecord.finalizedResult.outputCapabilityState -eq 'PRESENT_DENIED') "DENIAL_NOT_DISTINCT_$($deniedRecord.finalizedResult.outputCapabilityState)"
  Require ($deniedRecord.finalizedResult.outputCapabilityDenied -eq $true) 'DENIAL_FLAG_MISSING'
}

Assert-Case 'provider model adapter and actual call observations stay separate' {
  $exactRecord = New-CoordinateThreeCaseValidatedResultRecord -Definition $definition -Snapshot (New-Snapshot $baseResult) -ExpectedModel 'qwen3.8-flash'
  Require ($exactRecord.modelMatch -eq $true) 'EXACT_PROVIDER_MODEL_NOT_MATCHED'

  $combined = Copy-Result $baseResult
  $combined.model = 'qwen3.8-flash+structured-product-bridge'
  $record = New-CoordinateThreeCaseValidatedResultRecord -Definition $definition -Snapshot (New-Snapshot $combined) -ExpectedModel 'qwen3.8-flash'
  Require ($record.modelConflict -eq $true) 'MODEL_CONFLICT_NOT_RETAINED'
  Require ($record.modelMatch -eq $false) 'MODEL_CONFLICT_FALSE_PASS'
  Require ($record.providerModel -eq 'qwen3.8-flash') 'BASE_PROVIDER_MODEL_NOT_RETAINED'
  Require ($record.adapterLabel -eq 'structured-product-bridge') 'ADAPTER_LABEL_NOT_RETAINED'

  $unknown = Copy-Result $baseResult
  $unknown.Remove('providerCallCount')
  $unknown.Remove('providerCompletionState')
  $unknownRecord = New-CoordinateThreeCaseValidatedResultRecord -Definition $definition -Snapshot (New-Snapshot $unknown) -ExpectedModel 'qwen3.8-flash'
  Require ($null -eq $unknownRecord.providerCallCount) 'MISSING_CALL_COUNT_FABRICATED'
  Require ($unknownRecord.providerCallCountKnown -eq $false) 'MISSING_CALL_COUNT_MARKED_KNOWN'
  Require ($unknownRecord.providerCompletionState -eq 'UNKNOWN') 'MISSING_COMPLETION_FABRICATED'

  $conflict = Copy-Result $baseResult
  $conflict.model = 'unexpected-model'
  $conflictRecord = New-CoordinateThreeCaseValidatedResultRecord -Definition $definition -Snapshot (New-Snapshot $conflict) -ExpectedModel 'qwen3.8-flash'
  Require ($conflictRecord.modelConflict -eq $true) 'MODEL_CONFLICT_NOT_RETAINED'
  Require ($conflictRecord.modelMatch -eq $false) 'MODEL_CONFLICT_FALSE_PASS'
}

Assert-Case 'fixed terminal branch diagnostics survive the whitelist' {
  $diagnostic = Copy-Result $baseResult
  $diagnostic.recognitionTerminalDiagnostics = [ordered]@{
    schemaVersion = 'recognition_terminal_diagnostics_v1'
    failureStage = 'UNIFIED_ACQUISITION_TERMINAL'
    failureBranch = 'DECISION_REVIEW_REQUIRED'
    unifiedTerminalReason = 'COORDINATE_FORMAT_REQUIRES_VALIDATION'
    contractReasons = @('COORDINATE_FORMAT_REQUIRES_VALIDATION')
    candidatePointCount = 7
    candidateGroupCount = 1
    downstreamFamilyParserReached = $false
    deterministicParserOutcome = 'NOT_REACHED'
    detectedFamilyCue = 'MGRS'
    rawProviderResponse = 'forbidden'
  }
  $record = New-CoordinateThreeCaseValidatedResultRecord -Definition $definition -Snapshot (New-Snapshot $diagnostic) -ExpectedModel 'qwen3.8-flash'
  Require ($record.recognitionTerminalDiagnostics.failureBranch -eq 'DECISION_REVIEW_REQUIRED') 'FAILURE_BRANCH_DROPPED'
  Require ($record.recognitionTerminalDiagnostics.candidatePointCount -eq 7) 'CANDIDATE_COUNT_DROPPED'
  Require ($record.recognitionTerminalDiagnostics.downstreamFamilyParserReached -eq $false) 'PARSER_REACHED_CHANGED'
  $json = $record | ConvertTo-Json -Depth 20 -Compress
  Require (-not $json.Contains('forbidden')) 'RAW_DIAGNOSTIC_LEAKED'
}

Assert-Case 'terminal diagnostics persist through the retained artifact write and read chain' {
  $diagnostic = Copy-Result $baseResult
  $diagnostic.recognitionTerminalDiagnostics = [ordered]@{
    schemaVersion = 'recognition_terminal_diagnostics_v1'
    failureStage = 'UNIFIED_RESPONSE_ATTACHMENT'
    failureBranch = 'NO_TRUSTED_CANDIDATES'
    unifiedTerminalReason = 'NO_TRUSTED_CANDIDATES'
    contractReasons = @('UNIFIED_RECOGNITION_EVIDENCE_INCOMPLETE')
    candidatePointCount = 0
    candidateGroupCount = 0
    downstreamFamilyParserReached = $false
    deterministicParserOutcome = 'NOT_REACHED'
    detectedFamilyCue = 'BFTM'
  }
  $record = New-CoordinateThreeCaseValidatedResultRecord -Definition $definition -Snapshot (New-Snapshot $diagnostic) -ExpectedModel 'qwen3.8-flash'
  $tempRoot = Join-Path ([IO.Path]::GetTempPath()) ('r6-artifact-diagnostics-' + [guid]::NewGuid().ToString('N'))
  New-Item -ItemType Directory -Path $tempRoot | Out-Null
  try {
    $artifactPath = Join-Path $tempRoot 'validated-results.json'
    $state = New-CoordinateThreeCaseValidatedResultState -RunId 'offline-r6' -BatchId 'offline-r6' `
      -ExpectedCommit ('c' * 40) -Cases @($record) -RuntimeCommitMatch $true
    $integrity = Write-CoordinateThreeCaseValidatedResultArtifact -Path $artifactPath -State $state
    $persisted = Get-Content -LiteralPath $artifactPath -Raw | ConvertFrom-Json
    Require ($integrity.verified -eq $true) 'PERSISTED_ARTIFACT_NOT_VERIFIED'
    Require ($persisted.cases[0].recognitionTerminalDiagnostics.failureBranch -eq 'NO_TRUSTED_CANDIDATES') 'PERSISTED_BRANCH_DROPPED'
    Require ($persisted.cases[0].recognitionTerminalDiagnostics.detectedFamilyCue -eq 'BFTM') 'PERSISTED_FAMILY_CUE_DROPPED'
  } finally {
    $resolved = (Resolve-Path -LiteralPath $tempRoot -ErrorAction Stop).Path
    $osTemp = [IO.Path]::GetFullPath([IO.Path]::GetTempPath()).TrimEnd('\') + '\'
    if (-not $resolved.StartsWith($osTemp, [StringComparison]::OrdinalIgnoreCase)) {
      throw 'TEMP_CLEANUP_PATH_REJECTED'
    }
    Remove-Item -LiteralPath $resolved -Recurse -Force
  }
}

Assert-Case 'missing parser reach observation remains unknown' {
  $diagnostic = Copy-Result $baseResult
  $diagnostic.recognitionTerminalDiagnostics = [ordered]@{
    schemaVersion = 'recognition_terminal_diagnostics_v1'
    failureStage = 'GENERIC_FALLBACK'
    failureBranch = 'FALLBACK_ERROR'
    unifiedTerminalReason = 'FALLBACK_ERROR'
    contractReasons = @()
    candidatePointCount = 0
    candidateGroupCount = 0
    deterministicParserOutcome = 'NOT_REACHED'
    detectedFamilyCue = 'UNKNOWN'
  }
  $record = New-CoordinateThreeCaseValidatedResultRecord -Definition $definition -Snapshot (New-Snapshot $diagnostic) -ExpectedModel 'qwen3.8-flash'
  Require ($null -eq $record.recognitionTerminalDiagnostics.downstreamFamilyParserReached) 'MISSING_PARSER_OBSERVATION_RECLASSIFIED'
}

$summary = [ordered]@{
  suite = 'COORDINATE_THREE_CASE_RC_R6_ARTIFACT_DIAGNOSTICS'
  passed = $passed
  failed = $failed
  total = $passed + $failed
  providerCalls = 0
  httpRequests = 0
  cases = $results
}
$summary | ConvertTo-Json -Depth 8
if ($failed -gt 0) { exit 1 }
