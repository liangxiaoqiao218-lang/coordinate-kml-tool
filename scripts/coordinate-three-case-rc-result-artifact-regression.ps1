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

$definition = [ordered]@{
  caseId = 'fixture-a'
  imageSha256 = ('a' * 64)
}
$successSnapshot = @'
{
  "status": "SUCCEEDED",
  "httpStatus": 200,
  "result": {
    "success": true,
    "code": "NONE",
    "model": "qwen3.8-flash",
    "providerCallCount": 1,
    "providerCompletionState": "SUCCEEDED",
    "claimToken": "forbidden-claim-sentinel",
    "jobAccessToken": "forbidden-job-sentinel",
    "rawProviderResponse": "forbidden-provider-sentinel",
    "headers": { "Authorization": "forbidden-auth-sentinel" },
    "coordinateEngineV2": {
      "source_crs": {
        "id": "EPSG:32750",
        "type": "projected",
        "projection": "utm",
        "datum": "WGS84",
        "zone": 50,
        "hemisphere": "S",
        "axisOrder": "easting_northing",
        "sourceText": "forbidden-source-text-sentinel"
      }
    },
    "outputCapabilities": {
      "resultId": "result-fixture-a",
      "resultRevision": 3,
      "geometryHash": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "mapReady": true,
      "kmlReady": true,
      "blockReasons": [],
      "warningReasons": ["REVIEW_REQUIRED"]
    },
    "indonesiaStructuredPreflightDiagnostics": {
      "localOcrAttempted": true,
      "sourceContextPresent": true,
      "localOcrContextPresent": true,
      "explicitUtm50s": true,
      "projectedColumns": false,
      "controlledRcHintAuthorized": true,
      "controlledRcHintApplied": true,
      "preflightChecked": true,
      "preflightPassed": true,
      "routeSelected": true,
      "preflightFailureCode": "NONE",
      "routeFailureCode": "NONE",
      "rawText": "forbidden-ocr-sentinel"
    },
    "finalizedCoordinateResult": {
      "schemaVersion": "finalized_coordinate_result_v1",
      "resultId": "result-fixture-a",
      "resultRevision": 3,
      "currentRevision": 3,
      "geometryHash": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "coordinateType": "projected_xy",
      "precisionMode": "projected",
      "family": "indonesia_utm",
      "crs": { "id": "EPSG:4326", "axisOrder": "longitude_latitude", "raw": "forbidden-crs-sentinel" },
      "geometry": {
        "type": "Polygon",
        "coordinates": [[[110.1,-7.1],[110.2,-7.1],[110.2,-7.2],[110.1,-7.1]]],
        "rawText": "forbidden-geometry-sentinel"
      },
      "confirmationStatus": "pending",
      "qualityGateStatus": "review_required",
      "decisionState": "REVIEW_REQUIRED",
      "requiresReview": true,
      "technicalKmlReady": true,
      "kmlReady": true,
      "reasonCodes": ["REVIEW_REQUIRED"],
      "blockingReasons": [],
      "sourceCandidates": [{ "rawText": "forbidden-candidate-sentinel" }]
    }
  }
}
'@ | ConvertFrom-Json

$failureSnapshot = @'
{
  "status": "FAILED",
  "httpStatus": 422,
  "result": {
    "success": false,
    "code": "SOURCE_EVIDENCE_REQUIRED",
    "model": "qwen3.8-flash",
    "providerCallCount": 0,
    "providerCompletionState": "NOT_STARTED",
    "password": "forbidden-password-sentinel",
    "rawProviderResponse": "forbidden-failure-provider-sentinel",
    "indonesiaStructuredPreflightDiagnostics": {
      "localOcrAttempted": true,
      "sourceContextPresent": false,
      "localOcrContextPresent": false,
      "explicitUtm50s": false,
      "projectedColumns": false,
      "controlledRcHintAuthorized": true,
      "controlledRcHintApplied": false,
      "preflightChecked": true,
      "preflightPassed": false,
      "routeSelected": false,
      "preflightFailureCode": "INDONESIA_STRUCTURED_B_RC_JOB_PREFLIGHT_REJECTED",
      "routeFailureCode": "STRUCTURED_PRODUCT_UTM50S_EVIDENCE_REQUIRED"
    }
  }
}
'@ | ConvertFrom-Json

$successRecord = New-CoordinateThreeCaseValidatedResultRecord -Definition $definition `
  -Snapshot $successSnapshot -ExpectedModel 'qwen3.8-flash'
$failureDefinition = [ordered]@{ caseId = 'fixture-b'; imageSha256 = ('b' * 64) }
$failureRecord = New-CoordinateThreeCaseValidatedResultRecord -Definition $failureDefinition `
  -Snapshot $failureSnapshot -ExpectedModel 'qwen3.8-flash'

Assert-Case 'success result retains current finalized geometry and consumer identity binding' {
  Require ($successRecord.finalizedResult.identityPresent -eq $true) 'IDENTITY_NOT_RETAINED'
  Require ($successRecord.finalizedResult.currentRevisionMatch -eq $true) 'CURRENT_REVISION_NOT_BOUND'
  Require ($successRecord.finalizedResult.consumerIdentityMatch -eq $true) 'CONSUMER_IDENTITY_NOT_BOUND'
  Require ($successRecord.finalizedResult.geometry.type -eq 'Polygon') 'GEOMETRY_NOT_RETAINED'
  Require ($successRecord.finalizedResult.geometry.coordinates.Count -eq 1) 'POLYGON_RING_LEVEL_FLATTENED'
  Require ($successRecord.finalizedResult.geometry.coordinates[0].Count -eq 4) 'POLYGON_POSITION_LEVEL_FLATTENED'
  Require ($successRecord.finalizedResult.geometry.coordinates[0][0].Count -eq 2) 'POLYGON_ORDINATE_LEVEL_FLATTENED'
  Require ($successRecord.finalizedResult.crs.id -eq 'EPSG:4326') 'FINAL_CRS_NOT_RETAINED'
  Require ($successRecord.finalizedResult.sourceCrs.id -eq 'EPSG:32750') 'SOURCE_CRS_NOT_RETAINED'
  Require ($successRecord.finalizedResult.mapReady -eq $true) 'MAP_CAPABILITY_NOT_RETAINED'
  Require ($successRecord.finalizedResult.kmlReady -eq $true) 'KML_CAPABILITY_NOT_RETAINED'
}

Assert-Case 'geometry whitelist preserves MultiPolygon and GeometryCollection array depth' {
  $multiPolygon = @'
{"type":"MultiPolygon","coordinates":[[[[1,2],[3,4],[1,2]]]]}
'@ | ConvertFrom-Json
  $sanitizedMultiPolygon = ConvertTo-CoordinateThreeCaseGeometry -Geometry $multiPolygon
  $multiJson = $sanitizedMultiPolygon | ConvertTo-Json -Depth 20 -Compress
  Require ($multiJson -eq '{"type":"MultiPolygon","coordinates":[[[[1,2],[3,4],[1,2]]]]}') 'MULTIPOLYGON_DEPTH_CHANGED'

  $collection = @'
{"type":"GeometryCollection","geometries":[{"type":"Point","coordinates":[1,2]}]}
'@ | ConvertFrom-Json
  $sanitizedCollection = ConvertTo-CoordinateThreeCaseGeometry -Geometry $collection
  $collectionJson = $sanitizedCollection | ConvertTo-Json -Depth 20 -Compress
  Require ($collectionJson -eq '{"type":"GeometryCollection","geometries":[{"type":"Point","coordinates":[1,2]}]}') 'GEOMETRY_COLLECTION_DEPTH_CHANGED'
}

Assert-Case 'source CRS whitelist retains both structured and fixed string contracts' {
  $structured = ConvertTo-CoordinateThreeCaseSourceCrs -Crs $successSnapshot.result.coordinateEngineV2.source_crs
  Require ($structured.id -eq 'EPSG:32750') 'STRUCTURED_SOURCE_CRS_ID_MISSING'
  Require ($structured.axisOrder -eq 'easting_northing') 'STRUCTURED_SOURCE_CRS_AXIS_MISSING'
  $stringCrs = ConvertTo-CoordinateThreeCaseSourceCrs -Crs 'EPSG:4326'
  Require ($stringCrs.id -eq 'EPSG:4326') 'STRING_SOURCE_CRS_ID_MISSING'
  Require ($stringCrs.axisOrder -eq 'UNKNOWN') 'STRING_SOURCE_CRS_AXIS_FABRICATED'
}

Assert-Case 'diagnostic artifact retains the full fixed Run04 predicate envelope' {
  $diagnostics = $successRecord.diagnostics
  Require ($diagnostics.localOcrAttempted -eq $true) 'LOCAL_OCR_ATTEMPT_MISSING'
  Require ($diagnostics.sourceContextPresent -eq $true) 'SOURCE_CONTEXT_MISSING'
  Require ($diagnostics.localOcrContextPresent -eq $true) 'LOCAL_OCR_CONTEXT_MISSING'
  Require ($diagnostics.explicitUtm50s -eq $true) 'UTM50S_EVIDENCE_MISSING'
  Require ($diagnostics.projectedColumns -eq $false) 'PROJECTED_COLUMNS_VALUE_MISMATCH'
  Require ($diagnostics.controlledRcHintAuthorized -eq $true) 'HINT_AUTHORIZATION_MISSING'
  Require ($diagnostics.controlledRcHintApplied -eq $true) 'HINT_APPLICATION_MISSING'
  Require ($diagnostics.preflightChecked -eq $true) 'PREFLIGHT_CHECK_MISSING'
  Require ($diagnostics.routeSelected -eq $true) 'ROUTE_SELECTION_MISSING'
}

Assert-Case 'explicit whitelist excludes tokens raw responses headers source text and candidates' {
  $json = $successRecord | ConvertTo-Json -Depth 30 -Compress
  foreach ($forbidden in @(
    'forbidden-claim-sentinel', 'forbidden-job-sentinel', 'forbidden-provider-sentinel',
    'forbidden-auth-sentinel', 'forbidden-source-text-sentinel', 'forbidden-ocr-sentinel',
    'forbidden-geometry-sentinel', 'forbidden-candidate-sentinel', 'sourceCandidates', 'rawProviderResponse'
  )) {
    Require (-not $json.Contains($forbidden)) "FORBIDDEN_ARTIFACT_FIELD_$forbidden"
  }
}

Assert-Case 'failed pre-provider result retains fixed diagnostics without geometry or identity' {
  Require ($failureRecord.providerCallCount -eq 0) 'FAILURE_PROVIDER_COUNT_MISMATCH'
  Require ($failureRecord.providerCompletionState -eq 'NOT_STARTED') 'FAILURE_PROVIDER_STATE_MISMATCH'
  Require ($failureRecord.diagnostics.routeFailureCode -eq 'STRUCTURED_PRODUCT_UTM50S_EVIDENCE_REQUIRED') 'FAILURE_REASON_MISSING'
  Require ($null -eq $failureRecord.finalizedResult) 'FAILURE_FINALIZED_RESULT_LEAKED'
  $json = $failureRecord | ConvertTo-Json -Depth 30 -Compress
  Require (-not $json.Contains('forbidden-password-sentinel')) 'PASSWORD_LEAKED'
  Require (-not $json.Contains('forbidden-failure-provider-sentinel')) 'FAILURE_PROVIDER_RESPONSE_LEAKED'
}

Assert-Case 'provider call observation preserves over-limit values and keeps missing values unknown' {
  $overLimitSnapshot = $successSnapshot | ConvertTo-Json -Depth 30 | ConvertFrom-Json
  $overLimitSnapshot.result.providerCallCount = 2
  $overLimit = New-CoordinateThreeCaseValidatedResultRecord -Definition $definition `
    -Snapshot $overLimitSnapshot -ExpectedModel 'qwen3.8-flash'
  Require ($overLimit.providerCallCount -eq 2) 'PROVIDER_COUNT_WAS_CLAMPED'
  Require ($overLimit.providerCallCountKnown -eq $true) 'PROVIDER_COUNT_NOT_KNOWN'
  Require ($overLimit.providerCallLimitExceeded -eq $true) 'PROVIDER_LIMIT_EXCESS_HIDDEN'

  $missingSnapshot = $failureSnapshot | ConvertTo-Json -Depth 30 | ConvertFrom-Json
  $missingSnapshot.result.PSObject.Properties.Remove('providerCallCount')
  $missing = New-CoordinateThreeCaseValidatedResultRecord -Definition $failureDefinition `
    -Snapshot $missingSnapshot -ExpectedModel 'qwen3.8-flash'
  Require ($null -eq $missing.providerCallCount) 'MISSING_PROVIDER_COUNT_REWRITTEN'
  Require ($missing.providerCallCountKnown -eq $false) 'MISSING_PROVIDER_COUNT_MARKED_KNOWN'
  Require ($missing.providerCallLimitExceeded -eq $false) 'MISSING_PROVIDER_COUNT_MARKED_OVER_LIMIT'
}

$tempRoot = Join-Path ([IO.Path]::GetTempPath()) ("coordinate-three-case-result-artifact-" + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $tempRoot | Out-Null
try {
  $artifactPath = Join-Path $tempRoot 'validated-results.json'
  Assert-Case 'artifact write is atomic re-readable and SHA-256 verified' {
    $state = New-CoordinateThreeCaseValidatedResultState -RunId 'offline-run' -BatchId 'offline-batch' `
      -ExpectedCommit ('c' * 40) -Cases @($successRecord) -RuntimeCommitMatch $true
    $integrity = Write-CoordinateThreeCaseValidatedResultArtifact -Path $artifactPath -State $state
    Require ($integrity.verified -eq $true) 'ARTIFACT_NOT_VERIFIED'
    Require ($integrity.caseCount -eq 1) 'ARTIFACT_CASE_COUNT_MISMATCH'
    Require ((Test-Path -LiteralPath $artifactPath) -and (Test-Path -LiteralPath "$artifactPath.sha256")) 'ARTIFACT_FILES_MISSING'
    Require (-not @(Get-ChildItem -LiteralPath $tempRoot -Filter '*.tmp-*').Count) 'ATOMIC_TEMP_FILE_REMAINED'
    $actual = (Get-FileHash -LiteralPath $artifactPath -Algorithm SHA256).Hash.ToLowerInvariant()
    Require ($actual -eq $integrity.sha256) 'ARTIFACT_SHA_MISMATCH'
  }

  Assert-Case 'later failed case cannot erase the prior validated result' {
    $state = New-CoordinateThreeCaseValidatedResultState -RunId 'offline-run' -BatchId 'offline-batch' `
      -ExpectedCommit ('c' * 40) -Cases @($successRecord, $failureRecord) -RuntimeCommitMatch $true `
      -ClientFailureCode 'CASE_TERMINAL_FAILURE'
    $integrity = Write-CoordinateThreeCaseValidatedResultArtifact -Path $artifactPath -State $state
    $persisted = Get-Content -LiteralPath $artifactPath -Raw | ConvertFrom-Json
    Require ($integrity.caseCount -eq 2) 'SEQUENCE_CASE_COUNT_MISMATCH'
    Require ($persisted.cases[0].finalizedResult.geometry.type -eq 'Polygon') 'FIRST_RESULT_WAS_ERASED'
    Require ($null -eq $persisted.cases[1].finalizedResult) 'FAILED_CASE_GAINED_GEOMETRY'
  }

  Assert-Case 'close failure is recorded without deleting retained case evidence' {
    $state = New-CoordinateThreeCaseValidatedResultState -RunId 'offline-run' -BatchId 'offline-batch' `
      -ExpectedCommit ('c' * 40) -Cases @($successRecord) -RuntimeCommitMatch $true `
      -RunClosed $false -ClientFailureCode 'RUN_CLOSE_FAILED'
    Write-CoordinateThreeCaseValidatedResultArtifact -Path $artifactPath -State $state | Out-Null
    $persisted = Get-Content -LiteralPath $artifactPath -Raw | ConvertFrom-Json
    Require ($persisted.runClosed -eq $false) 'RUN_CLOSE_STATE_MISMATCH'
    Require ($persisted.clientFailureCode -eq 'RUN_CLOSE_FAILED') 'RUN_CLOSE_FAILURE_MISSING'
    Require ($persisted.cases.Count -eq 1) 'CLOSE_FAILURE_ERASED_RESULT'
  }
} finally {
  $resolvedTempRoot = (Resolve-Path -LiteralPath $tempRoot -ErrorAction Stop).Path
  $expectedTempRoot = [IO.Path]::GetFullPath([IO.Path]::GetTempPath()).TrimEnd('\') + '\'
  if (-not $resolvedTempRoot.StartsWith($expectedTempRoot, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'TEMP_CLEANUP_PATH_REJECTED'
  }
  Remove-Item -LiteralPath $resolvedTempRoot -Recurse -Force
}

$clientText = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'coordinate-three-case-rc-client.ps1') -Raw
Assert-Case 'client writes the private artifact per terminal case and prints only sanitized summary evidence' {
  $convertIndex = $clientText.IndexOf('New-CoordinateThreeCaseValidatedResultRecord')
  $writeIndex = $clientText.IndexOf('Write-CoordinateThreeCaseValidatedResultArtifact', $convertIndex)
  $summaryIndex = $clientText.LastIndexOf('$execution.evidence | ConvertTo-Json')
  Require ($convertIndex -ge 0 -and $writeIndex -gt $convertIndex) 'PER_CASE_ARTIFACT_WRITE_MISSING'
  Require ($summaryIndex -gt $writeIndex) 'SANITIZED_SUMMARY_OUTPUT_MISSING'
  Require (-not $clientText.Contains('$validatedState | ConvertTo-Json')) 'PRIVATE_ARTIFACT_PRINTED'
  Require ($clientText.Contains('modelMatch = [string]$result.model -eq $expectedModel')) 'SUMMARY_MODEL_MATCH_NOT_EXACT'
  Require ($clientText.Contains('providerCallLimitExceeded = $providerCalls.limitExceeded')) 'SUMMARY_PROVIDER_LIMIT_EXCESS_MISSING'
}

$summary = [ordered]@{
  suite = 'COORDINATE_THREE_CASE_RC_RESULT_ARTIFACT'
  passed = $passed
  failed = $failed
  total = $passed + $failed
  providerCalls = 0
  httpRequests = 0
  cases = $results
}
$summary | ConvertTo-Json -Depth 8
if ($failed -gt 0) { exit 1 }
