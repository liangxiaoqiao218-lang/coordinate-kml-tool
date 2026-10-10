function ConvertTo-CoordinateThreeCaseFixedCode {
  param([object] $Value, [string] $Fallback = 'NONE')
  $candidate = [string]$Value
  if ($candidate -match '^[A-Za-z0-9_.:-]{1,160}$') { return $candidate }
  return $Fallback
}

function ConvertTo-CoordinateThreeCaseStringList {
  param([object] $Value)
  if ($null -eq $Value) { return @() }
  $items = @($Value)
  return @($items | ForEach-Object {
    $candidate = [string]$_
    if ($candidate -match '^[A-Za-z0-9_.:-]{1,160}$') { $candidate }
  } | Select-Object -Unique)
}

function Get-CoordinateThreeCaseProviderCallObservation {
  param([object] $Value, [int] $Limit = 1)
  $parsed = 0
  $known = $null -ne $Value -and
    $Value -isnot [bool] -and
    [int]::TryParse([string]$Value, [ref]$parsed) -and
    $parsed -ge 0
  return [ordered]@{
    value = if ($known) { $parsed } else { $null }
    known = $known
    limitExceeded = $known -and $parsed -gt $Limit
  }
}

function ConvertTo-CoordinateThreeCaseGeometryValue {
  param([object] $Value, [int] $Depth = 0)
  if ($Depth -gt 12) { throw 'RESULT_ARTIFACT_GEOMETRY_DEPTH_EXCEEDED' }
  if ($null -eq $Value) { return $null }
  if ($Value -is [bool]) { throw 'RESULT_ARTIFACT_GEOMETRY_BOOLEAN_REJECTED' }
  if ($Value -is [byte] -or $Value -is [sbyte] -or $Value -is [int16] -or
      $Value -is [uint16] -or $Value -is [int32] -or $Value -is [uint32] -or
      $Value -is [int64] -or $Value -is [uint64] -or $Value -is [single] -or
      $Value -is [double] -or $Value -is [decimal]) {
    $number = [double]$Value
    if ([double]::IsNaN($number) -or [double]::IsInfinity($number)) {
      throw 'RESULT_ARTIFACT_GEOMETRY_NONFINITE_REJECTED'
    }
    return $Value
  }
  if ($Value -is [System.Collections.IEnumerable] -and $Value -isnot [string]) {
    $children = [Collections.Generic.List[object]]::new()
    foreach ($item in $Value) {
      $child = ConvertTo-CoordinateThreeCaseGeometryValue -Value $item -Depth ($Depth + 1)
      $children.Add($child)
    }
    Write-Output -NoEnumerate ([object[]]$children.ToArray())
    return
  }
  throw 'RESULT_ARTIFACT_GEOMETRY_VALUE_REJECTED'
}

function ConvertTo-CoordinateThreeCaseGeometry {
  param([object] $Geometry)
  if ($null -eq $Geometry) { return $null }
  $type = [string]$Geometry.type
  $allowed = @('Point', 'MultiPoint', 'LineString', 'MultiLineString', 'Polygon', 'MultiPolygon', 'GeometryCollection')
  if ($type -notin $allowed) { throw 'RESULT_ARTIFACT_GEOMETRY_TYPE_REJECTED' }
  if ($type -eq 'GeometryCollection') {
    return [ordered]@{
      type = $type
      geometries = @($Geometry.geometries | ForEach-Object { ConvertTo-CoordinateThreeCaseGeometry -Geometry $_ })
    }
  }
  return [ordered]@{
    type = $type
    coordinates = ConvertTo-CoordinateThreeCaseGeometryValue -Value $Geometry.coordinates
  }
}

function ConvertTo-CoordinateThreeCaseCrs {
  param([object] $Crs)
  if ($null -eq $Crs) { return $null }
  return [ordered]@{
    id = ConvertTo-CoordinateThreeCaseFixedCode -Value $Crs.id -Fallback 'UNKNOWN'
    axisOrder = ConvertTo-CoordinateThreeCaseFixedCode -Value $Crs.axisOrder -Fallback 'UNKNOWN'
  }
}

function ConvertTo-CoordinateThreeCaseSourceCrs {
  param([object] $Crs)
  if ($null -eq $Crs) { return $null }
  if ($Crs -is [string]) {
    return [ordered]@{
      id = ConvertTo-CoordinateThreeCaseFixedCode -Value $Crs -Fallback 'UNKNOWN'
      type = 'UNKNOWN'
      projection = 'UNKNOWN'
      datum = 'UNKNOWN'
      zone = $null
      hemisphere = 'UNKNOWN'
      axisOrder = 'UNKNOWN'
    }
  }
  $zone = $null
  if ($null -ne $Crs.zone) {
    $parsedZone = 0
    if ([int]::TryParse([string]$Crs.zone, [ref]$parsedZone) -and $parsedZone -ge 1 -and $parsedZone -le 60) {
      $zone = $parsedZone
    }
  }
  return [ordered]@{
    id = ConvertTo-CoordinateThreeCaseFixedCode -Value $Crs.id -Fallback 'UNKNOWN'
    type = ConvertTo-CoordinateThreeCaseFixedCode -Value $Crs.type -Fallback 'UNKNOWN'
    projection = ConvertTo-CoordinateThreeCaseFixedCode -Value $Crs.projection -Fallback 'UNKNOWN'
    datum = ConvertTo-CoordinateThreeCaseFixedCode -Value $Crs.datum -Fallback 'UNKNOWN'
    zone = $zone
    hemisphere = ConvertTo-CoordinateThreeCaseFixedCode -Value $Crs.hemisphere -Fallback 'UNKNOWN'
    axisOrder = ConvertTo-CoordinateThreeCaseFixedCode -Value $Crs.axisOrder -Fallback 'UNKNOWN'
  }
}

function New-CoordinateThreeCaseValidatedResultRecord {
  param(
    [Parameter(Mandatory = $true)] [System.Collections.IDictionary] $Definition,
    [Parameter(Mandatory = $true)] [object] $Snapshot,
    [Parameter(Mandatory = $true)] [string] $ExpectedModel
  )

  $result = $Snapshot.result
  $finalized = $result.finalizedCoordinateResult
  $capabilities = $result.outputCapabilities
  $diagnostics = $result.indonesiaStructuredPreflightDiagnostics
  $providerCalls = Get-CoordinateThreeCaseProviderCallObservation -Value $result.providerCallCount -Limit 1
  $safeResultId = ConvertTo-CoordinateThreeCaseFixedCode -Value $finalized.resultId -Fallback ''
  $safeGeometryHash = if ([string]$finalized.geometryHash -match '^sha256:[0-9a-f]{64}$') {
    [string]$finalized.geometryHash
  } else { '' }
  $safeResultRevision = if ($null -ne $finalized -and [int64]$finalized.resultRevision -gt 0) {
    [int64]$finalized.resultRevision
  } else { $null }
  $identityPresent = $null -ne $finalized -and
    -not [string]::IsNullOrWhiteSpace($safeResultId) -and
    $null -ne $safeResultRevision -and
    -not [string]::IsNullOrWhiteSpace($safeGeometryHash)
  $consumerIdentityMatch = $identityPresent -and $null -ne $capabilities -and
    [string]$capabilities.resultId -eq $safeResultId -and
    [int64]$capabilities.resultRevision -eq $safeResultRevision -and
    [string]$capabilities.geometryHash -eq $safeGeometryHash
  $currentRevisionMatch = $identityPresent -and (
    $null -eq $finalized.currentRevision -or
    [int64]$finalized.currentRevision -eq $safeResultRevision
  )
  $safeImageSha256 = if ([string]$Definition.imageSha256 -match '^[0-9a-f]{64}$') {
    [string]$Definition.imageSha256
  } else { '' }
  $httpStatus = 0
  $httpStatusKnown = [int]::TryParse([string]$Snapshot.httpStatus, [ref]$httpStatus) -and
    $httpStatus -ge 100 -and $httpStatus -le 599

  $record = [ordered]@{
    schemaVersion = 'coordinate_three_case_validated_result_v1'
    caseId = ConvertTo-CoordinateThreeCaseFixedCode -Value $Definition.caseId -Fallback 'UNKNOWN'
    imageSha256 = $safeImageSha256
    terminalStatus = ConvertTo-CoordinateThreeCaseFixedCode -Value $Snapshot.status -Fallback 'UNKNOWN'
    httpStatus = if ($httpStatusKnown) { $httpStatus } else { $null }
    success = $result.success -eq $true
    code = ConvertTo-CoordinateThreeCaseFixedCode -Value $(if ($result.code) { $result.code } elseif ($result.reason) { $result.reason } else { 'NONE' })
    model = if ([string]$result.model -eq $ExpectedModel) { $ExpectedModel } else { 'UNEXPECTED' }
    modelMatch = [string]$result.model -eq $ExpectedModel
    providerCallCount = $providerCalls.value
    providerCallCountKnown = $providerCalls.known
    providerCallLimitExceeded = $providerCalls.limitExceeded
    providerCompletionState = ConvertTo-CoordinateThreeCaseFixedCode -Value $result.providerCompletionState -Fallback 'UNKNOWN'
    diagnostics = [ordered]@{
      present = $null -ne $diagnostics
      localOcrAttempted = $diagnostics.localOcrAttempted -eq $true
      sourceContextPresent = $diagnostics.sourceContextPresent -eq $true
      localOcrContextPresent = $diagnostics.localOcrContextPresent -eq $true
      explicitUtm50s = $diagnostics.explicitUtm50s -eq $true
      projectedColumns = $diagnostics.projectedColumns -eq $true
      controlledRcHintAuthorized = $diagnostics.controlledRcHintAuthorized -eq $true
      controlledRcHintApplied = $diagnostics.controlledRcHintApplied -eq $true
      preflightChecked = $diagnostics.preflightChecked -eq $true
      preflightPassed = $diagnostics.preflightPassed -eq $true
      routeSelected = $diagnostics.routeSelected -eq $true
      preflightFailureCode = ConvertTo-CoordinateThreeCaseFixedCode -Value $diagnostics.preflightFailureCode
      routeFailureCode = ConvertTo-CoordinateThreeCaseFixedCode -Value $diagnostics.routeFailureCode
    }
    finalizedResult = $null
  }

  if ($null -ne $finalized) {
    $record.finalizedResult = [ordered]@{
      schemaVersion = ConvertTo-CoordinateThreeCaseFixedCode -Value $finalized.schemaVersion -Fallback 'UNKNOWN'
      resultId = $safeResultId
      resultRevision = $safeResultRevision
      geometryHash = $safeGeometryHash
      identityPresent = $identityPresent
      currentRevisionMatch = $currentRevisionMatch
      consumerIdentityMatch = $consumerIdentityMatch
      coordinateType = ConvertTo-CoordinateThreeCaseFixedCode -Value $finalized.coordinateType -Fallback 'UNKNOWN'
      precisionMode = ConvertTo-CoordinateThreeCaseFixedCode -Value $finalized.precisionMode -Fallback 'UNKNOWN'
      family = ConvertTo-CoordinateThreeCaseFixedCode -Value $finalized.family -Fallback 'UNKNOWN'
      crs = ConvertTo-CoordinateThreeCaseCrs -Crs $finalized.crs
      sourceCrs = ConvertTo-CoordinateThreeCaseSourceCrs -Crs $result.coordinateEngineV2.source_crs
      geometry = ConvertTo-CoordinateThreeCaseGeometry -Geometry $finalized.geometry
      confirmationStatus = ConvertTo-CoordinateThreeCaseFixedCode -Value $finalized.confirmationStatus -Fallback 'UNKNOWN'
      qualityGateStatus = ConvertTo-CoordinateThreeCaseFixedCode -Value $finalized.qualityGateStatus -Fallback 'UNKNOWN'
      decisionState = ConvertTo-CoordinateThreeCaseFixedCode -Value $finalized.decisionState -Fallback 'UNKNOWN'
      requiresReview = $finalized.requiresReview -eq $true
      technicalKmlReady = $finalized.technicalKmlReady -eq $true
      mapReady = $capabilities.mapReady -eq $true
      kmlReady = $capabilities.kmlReady -eq $true -and $finalized.kmlReady -eq $true
      reasonCodes = @(ConvertTo-CoordinateThreeCaseStringList -Value $finalized.reasonCodes)
      blockingReasons = @(ConvertTo-CoordinateThreeCaseStringList -Value $finalized.blockingReasons)
      outputCapabilityBlockReasons = @(ConvertTo-CoordinateThreeCaseStringList -Value $capabilities.blockReasons)
      outputCapabilityWarningReasons = @(ConvertTo-CoordinateThreeCaseStringList -Value $capabilities.warningReasons)
    }
  }
  return $record
}

function New-CoordinateThreeCaseValidatedResultState {
  param(
    [Parameter(Mandatory = $true)] [string] $RunId,
    [Parameter(Mandatory = $true)] [string] $BatchId,
    [Parameter(Mandatory = $true)] [string] $ExpectedCommit,
    [Parameter(Mandatory = $true)] [System.Collections.IEnumerable] $Cases,
    [bool] $RuntimeCommitMatch = $false,
    [bool] $RunClosed = $false,
    [string] $ClientFailureCode = ''
  )
  return [ordered]@{
    schemaVersion = 'coordinate_three_case_validated_results_v1'
    runId = ConvertTo-CoordinateThreeCaseFixedCode -Value $RunId -Fallback 'UNKNOWN'
    batchId = ConvertTo-CoordinateThreeCaseFixedCode -Value $BatchId -Fallback 'UNKNOWN'
    expectedCommit = if ($ExpectedCommit -match '^[0-9a-f]{40}$') { $ExpectedCommit } else { 'UNKNOWN' }
    runtimeCommitMatch = $RuntimeCommitMatch
    runClosed = $RunClosed
    clientFailureCode = ConvertTo-CoordinateThreeCaseFixedCode -Value $ClientFailureCode
    cases = @($Cases)
  }
}

function Write-CoordinateThreeCaseValidatedResultArtifact {
  param(
    [Parameter(Mandatory = $true)] [string] $Path,
    [Parameter(Mandatory = $true)] [System.Collections.IDictionary] $State
  )
  $resolved = [IO.Path]::GetFullPath($Path)
  $parent = Split-Path -Parent $resolved
  if (-not (Test-Path -LiteralPath $parent)) { New-Item -ItemType Directory -Path $parent | Out-Null }
  $temporary = "$resolved.tmp-$([guid]::NewGuid().ToString('N'))"
  $shaPath = "$resolved.sha256"
  $shaTemporary = "$shaPath.tmp-$([guid]::NewGuid().ToString('N'))"
  try {
    $json = $State | ConvertTo-Json -Depth 30
    [IO.File]::WriteAllText($temporary, $json, [Text.UTF8Encoding]::new($false))
    Move-Item -LiteralPath $temporary -Destination $resolved -Force
    $sha = (Get-FileHash -LiteralPath $resolved -Algorithm SHA256).Hash.ToLowerInvariant()
    [IO.File]::WriteAllText($shaTemporary, "$sha  $([IO.Path]::GetFileName($resolved))`n", [Text.UTF8Encoding]::new($false))
    Move-Item -LiteralPath $shaTemporary -Destination $shaPath -Force
    $verifiedSha = (Get-FileHash -LiteralPath $resolved -Algorithm SHA256).Hash.ToLowerInvariant()
    $verifiedState = Get-Content -LiteralPath $resolved -Raw | ConvertFrom-Json
    if ($sha -ne $verifiedSha -or $verifiedState.schemaVersion -ne 'coordinate_three_case_validated_results_v1') {
      throw 'RESULT_ARTIFACT_INTEGRITY_VERIFICATION_FAILED'
    }
    return [ordered]@{
      path = $resolved
      sha256Path = $shaPath
      sha256 = $sha
      verified = $true
      caseCount = @($verifiedState.cases).Count
    }
  } finally {
    if (Test-Path -LiteralPath $temporary) { Remove-Item -LiteralPath $temporary -Force }
    if (Test-Path -LiteralPath $shaTemporary) { Remove-Item -LiteralPath $shaTemporary -Force }
  }
}
