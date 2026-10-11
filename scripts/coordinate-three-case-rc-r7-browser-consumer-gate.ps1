param(
  [string] $BrowserEvidencePath,
  [string] $ValidatedResultPath,
  [string] $KmlDownloadPath,
  [string] $ExpectedRunId = 'coordinate-three-case-rc-20261011-r7',
  [string] $ExpectedCaseId = 'indonesia',
  [string] $HandoffExpiresAt,
  [int] $TimeoutSeconds = 480,
  [switch] $LibraryOnly,
  [switch] $SelfTest
)

$ErrorActionPreference = 'Stop'

function Get-R7IdentityBindingDigest {
  param([string] $RunId, [string] $CaseId, [string] $RequestId, [object] $FinalizedResult)
  $material = @(
    $RunId,
    $CaseId,
    $RequestId,
    [string]$FinalizedResult.resultId,
    [string]$FinalizedResult.resultRevision,
    [string]$FinalizedResult.geometryHash
  ) -join "`n"
  $bytes = [Text.Encoding]::UTF8.GetBytes($material)
  $hash = [Security.Cryptography.SHA256]::HashData($bytes)
  return [Convert]::ToHexString($hash).ToLowerInvariant()
}
function ConvertTo-R7CoordinateGroups {
  param([object] $Geometry)
  $type = [string]$Geometry.type
  if ($type -eq 'Point') { return ,@(@([double]$Geometry.coordinates[0], [double]$Geometry.coordinates[1])) }
  if ($type -eq 'MultiPoint') { return @($Geometry.coordinates | ForEach-Object { ,@([double]$_[0], [double]$_[1]) }) }
  if ($type -eq 'LineString') { return ,@($Geometry.coordinates | ForEach-Object { ,@([double]$_[0], [double]$_[1]) }) }
  if ($type -eq 'Polygon') {
    if (@($Geometry.coordinates).Count -ne 1) { throw 'R7_KML_INNER_RING_UNSUPPORTED' }
    return ,@($Geometry.coordinates[0] | ForEach-Object { ,@([double]$_[0], [double]$_[1]) })
  }
  if ($type -eq 'MultiPolygon') {
    $groups = [Collections.Generic.List[object]]::new()
    foreach ($polygon in @($Geometry.coordinates)) {
      if (@($polygon).Count -ne 1) { throw 'R7_KML_INNER_RING_UNSUPPORTED' }
      $groups.Add(@($polygon[0] | ForEach-Object { ,@([double]$_[0], [double]$_[1]) }))
    }
    return @($groups)
  }
  throw 'R7_KML_GEOMETRY_TYPE_UNSUPPORTED'
}

function Read-R7KmlCoordinateGroups {
  param([Parameter(Mandatory = $true)] [string] $Path)
  $settings = [Xml.XmlReaderSettings]::new()
  $settings.DtdProcessing = [Xml.DtdProcessing]::Prohibit
  $settings.XmlResolver = $null
  $settings.MaxCharactersInDocument = 4MB
  $reader = [Xml.XmlReader]::Create($Path, $settings)
  try {
    $document = [Xml.XmlDocument]::new()
    $document.XmlResolver = $null
    $document.Load($reader)
  } finally {
    $reader.Dispose()
  }
  $namespace = [Xml.XmlNamespaceManager]::new($document.NameTable)
  $namespace.AddNamespace('k', 'http://www.opengis.net/kml/2.2')
  if (@($document.SelectNodes('//k:Polygon/k:innerBoundaryIs', $namespace)).Count -gt 0) {
    throw 'R7_KML_INNER_RING_UNSUPPORTED'
  }
  $pointNodes = @($document.SelectNodes('//k:Point/k:coordinates', $namespace))
  $lineNodes = @($document.SelectNodes('//k:LineString/k:coordinates', $namespace))
  $polygonNodes = @($document.SelectNodes('//k:Polygon/k:outerBoundaryIs/k:LinearRing/k:coordinates', $namespace))
  $categories = [Collections.Generic.List[string]]::new()
  if ($pointNodes.Count -gt 0) { $categories.Add('Point') }
  if ($lineNodes.Count -gt 0) { $categories.Add('LineString') }
  if ($polygonNodes.Count -gt 0) { $categories.Add('Polygon') }
  if ($categories.Count -ne 1 -or $lineNodes.Count -gt 1) { throw 'R7_KML_GEOMETRY_STRUCTURE_INVALID' }
  $geometryType = if ($pointNodes.Count -gt 0) {
    if ($pointNodes.Count -eq 1) { 'Point' } else { 'MultiPoint' }
  } elseif ($lineNodes.Count -gt 0) {
    'LineString'
  } else {
    if ($polygonNodes.Count -eq 1) { 'Polygon' } else { 'MultiPolygon' }
  }
  $nodes = if ($pointNodes.Count -gt 0) { $pointNodes } elseif ($lineNodes.Count -gt 0) { $lineNodes } else { $polygonNodes }
  if ($nodes.Count -eq 0) { throw 'R7_KML_COORDINATES_MISSING' }
  $groups = [Collections.Generic.List[object]]::new()
  foreach ($node in $nodes) {
    $group = [Collections.Generic.List[object]]::new()
    foreach ($token in ([string]$node.InnerText -split '\s+')) {
      if ([string]::IsNullOrWhiteSpace($token)) { continue }
      $parts = $token.Split(',')
      if ($parts.Count -lt 2) { throw 'R7_KML_COORDINATE_INVALID' }
      $longitude = 0.0
      $latitude = 0.0
      if (-not [double]::TryParse($parts[0], [Globalization.NumberStyles]::Float, [Globalization.CultureInfo]::InvariantCulture, [ref]$longitude) -or
          -not [double]::TryParse($parts[1], [Globalization.NumberStyles]::Float, [Globalization.CultureInfo]::InvariantCulture, [ref]$latitude)) {
        throw 'R7_KML_COORDINATE_INVALID'
      }
      if ([double]::IsNaN($longitude) -or [double]::IsInfinity($longitude) -or
          [double]::IsNaN($latitude) -or [double]::IsInfinity($latitude) -or
          $longitude -lt -180 -or $longitude -gt 180 -or $latitude -lt -90 -or $latitude -gt 90) {
        throw 'R7_KML_COORDINATE_NONFINITE_OR_OUT_OF_RANGE'
      }
      $group.Add(@($longitude, $latitude))
    }
    if ($group.Count -eq 0) { throw 'R7_KML_COORDINATE_GROUP_EMPTY' }
    $groups.Add(@($group))
  }
  return [ordered]@{ geometryType = $geometryType; groups = @($groups) }
}

function Test-R7CoordinateGroupsExact {
  param([object[]] $Expected, [object[]] $Actual)
  if ($Expected.Count -ne $Actual.Count) { return $false }
  for ($groupIndex = 0; $groupIndex -lt $Expected.Count; $groupIndex++) {
    $left = @($Expected[$groupIndex])
    $right = @($Actual[$groupIndex])
    if ($left.Count -ne $right.Count) { return $false }
    for ($pointIndex = 0; $pointIndex -lt $left.Count; $pointIndex++) {
      if ([Math]::Abs([double]$left[$pointIndex][0] - [double]$right[$pointIndex][0]) -gt 0.000000001 -or
          [Math]::Abs([double]$left[$pointIndex][1] - [double]$right[$pointIndex][1]) -gt 0.000000001) { return $false }
    }
  }
  return $true
}

function Wait-R7File {
  param([string] $Path, [Diagnostics.Stopwatch] $Clock, [int] $TimeoutMilliseconds)
  while ($Clock.ElapsedMilliseconds -lt $TimeoutMilliseconds) {
    if (Test-Path -LiteralPath $Path -PathType Leaf) { return $true }
    Start-Sleep -Milliseconds 250
  }
  return $false
}

function Wait-R7StableFile {
  param([string] $Path, [Diagnostics.Stopwatch] $Clock, [int] $TimeoutMilliseconds)
  $previousLength = -1L
  $stableObservations = 0
  while ($Clock.ElapsedMilliseconds -lt $TimeoutMilliseconds) {
    if (Test-Path -LiteralPath $Path -PathType Leaf) {
      $length = (Get-Item -LiteralPath $Path).Length
      if ($length -gt 0 -and $length -eq $previousLength) {
        $stableObservations++
        if ($stableObservations -ge 2) { return $true }
      } else {
        $stableObservations = 0
        $previousLength = $length
      }
    }
    Start-Sleep -Milliseconds 200
  }
  return $false
}

function Invoke-R7BrowserConsumerGate {
  param(
    [Parameter(Mandatory = $true)] [string] $EvidencePath,
    [Parameter(Mandatory = $true)] [string] $ResultPath,
    [Parameter(Mandatory = $true)] [string] $DownloadPath,
    [Parameter(Mandatory = $true)] [string] $RunId,
    [Parameter(Mandatory = $true)] [string] $CaseId,
    [Parameter(Mandatory = $true)] [string] $ExpiresAt,
    [int] $WaitSeconds = 480
  )
  foreach ($candidate in @($EvidencePath, $ResultPath, $DownloadPath)) {
    if (-not [IO.Path]::IsPathFullyQualified($candidate)) { throw 'R7_CONSUMER_GATE_ABSOLUTE_PATH_REQUIRED' }
  }
  $handoffDeadline = [DateTimeOffset]::MinValue
  if (-not [DateTimeOffset]::TryParse($ExpiresAt, [Globalization.CultureInfo]::InvariantCulture,
      [Globalization.DateTimeStyles]::AssumeUniversal, [ref]$handoffDeadline)) {
    throw 'R7_CONSUMER_GATE_HANDOFF_DEADLINE_INVALID'
  }
  $remainingMilliseconds = [int64][Math]::Floor(($handoffDeadline.ToUniversalTime() - [DateTimeOffset]::UtcNow).TotalMilliseconds)
  if ($remainingMilliseconds -le 0) { throw 'R7_CONSUMER_GATE_HANDOFF_EXPIRED' }
  $timeoutMilliseconds = [int][Math]::Min([int64]($WaitSeconds * 1000), $remainingMilliseconds)
  $clock = [Diagnostics.Stopwatch]::StartNew()
  if (-not (Wait-R7File -Path $EvidencePath -Clock $clock -TimeoutMilliseconds $timeoutMilliseconds)) {
    throw 'R7_BROWSER_EVIDENCE_TIMEOUT'
  }
  if (-not (Wait-R7StableFile -Path $DownloadPath -Clock $clock -TimeoutMilliseconds $timeoutMilliseconds)) {
    throw 'R7_KML_DOWNLOAD_TIMEOUT'
  }
  if ([DateTimeOffset]::UtcNow -ge $handoffDeadline.ToUniversalTime()) { throw 'R7_CONSUMER_GATE_HANDOFF_EXPIRED' }
  $browser = Get-Content -LiteralPath $EvidencePath -Raw | ConvertFrom-Json
  $validated = Get-Content -LiteralPath $ResultPath -Raw | ConvertFrom-Json
  $case = @($validated.cases | Where-Object { $_.caseId -eq $CaseId })
  if ($case.Count -ne 1) { throw 'R7_VALIDATED_RESULT_CASE_BINDING_INVALID' }
  $record = $case[0]
  $finalized = $record.finalizedResult
  $expectedDigest = Get-R7IdentityBindingDigest -RunId $RunId -CaseId $CaseId `
    -RequestId ([string]$record.recognitionRequestId) -FinalizedResult $finalized
  $browserContract = $browser.schemaVersion -eq 'coordinate_three_case_rc_r7_browser_evidence_v1' -and
    $browser.runId -eq $RunId -and $browser.caseId -eq $CaseId -and
    $browser.bindingDigest -eq $expectedDigest -and $browser.resultInstalled -eq $true -and
    $browser.resultIdentityMatch -eq $true -and $browser.mapState -eq 'READY' -and
    $browser.mapProvider -eq 'OPENFREEMAP' -and $browser.mapIdentityMatch -eq $true -and
    [int]$browser.authorityMutationCount -eq 0 -and $browser.kmlDownloadTriggered -eq $true -and
    $browser.kmlIdentityMatch -eq $true -and $browser.downloadFileName -eq 'coordinates.kml'
  if (-not $browserContract) { throw 'R7_BROWSER_CONSUMER_EVIDENCE_REJECTED' }
  $expectedGroups = @(ConvertTo-R7CoordinateGroups -Geometry $finalized.geometry)
  $actualKml = Read-R7KmlCoordinateGroups -Path $DownloadPath
  if ([string]$actualKml.geometryType -ne [string]$finalized.geometry.type) {
    throw 'R7_KML_GEOMETRY_TYPE_MISMATCH'
  }
  if (-not (Test-R7CoordinateGroupsExact -Expected $expectedGroups -Actual @($actualKml.groups))) {
    throw 'R7_KML_GEOMETRY_IDENTITY_MISMATCH'
  }
  $sha256 = (Get-FileHash -LiteralPath $DownloadPath -Algorithm SHA256).Hash.ToLowerInvariant()
  return [ordered]@{
    success = $true
    browserEvidenceAccepted = $true
    mapReady = $true
    mapProvider = 'OPENFREEMAP'
    authorityMutationCount = 0
    kmlParsed = $true
    kmlGeometryType = [string]$actualKml.geometryType
    kmlGeometryMatch = $true
    kmlSha256 = $sha256
  }
}

function Invoke-R7KmlParserSelfTest {
  $root = Join-Path ([IO.Path]::GetTempPath()) ("r7-kml-gate-" + [guid]::NewGuid().ToString('N'))
  New-Item -ItemType Directory -Path $root | Out-Null
  $passed = 0
  try {
    $valid = Join-Path $root 'valid.kml'
    Set-Content -LiteralPath $valid -Encoding UTF8 -Value '<?xml version="1.0"?><kml xmlns="http://www.opengis.net/kml/2.2"><Document><Placemark><Polygon><outerBoundaryIs><LinearRing><coordinates>1,2 3,4 1,2</coordinates></LinearRing></outerBoundaryIs></Polygon></Placemark></Document></kml>'
    $parsed = Read-R7KmlCoordinateGroups -Path $valid
    if ($parsed.geometryType -ne 'Polygon' -or @($parsed.groups).Count -ne 1) { throw 'R7_SELF_TEST_VALID_POLYGON_FAILED' }
    $passed++
    $negative = @(
      @{ Name = 'type'; Xml = '<?xml version="1.0"?><kml xmlns="http://www.opengis.net/kml/2.2"><Placemark><LineString><coordinates>1,2 3,4 1,2</coordinates></LineString></Placemark></kml>'; Code = 'R7_KML_GEOMETRY_TYPE_MISMATCH' },
      @{ Name = 'inner'; Xml = '<?xml version="1.0"?><kml xmlns="http://www.opengis.net/kml/2.2"><Placemark><Polygon><outerBoundaryIs><LinearRing><coordinates>1,2 3,4 1,2</coordinates></LinearRing></outerBoundaryIs><innerBoundaryIs><LinearRing><coordinates>1,2 2,3 1,2</coordinates></LinearRing></innerBoundaryIs></Polygon></Placemark></kml>'; Code = 'R7_KML_INNER_RING_UNSUPPORTED' },
      @{ Name = 'nonfinite'; Xml = '<?xml version="1.0"?><kml xmlns="http://www.opengis.net/kml/2.2"><Placemark><Point><coordinates>NaN,2</coordinates></Point></Placemark></kml>'; Code = 'R7_KML_COORDINATE_NONFINITE_OR_OUT_OF_RANGE' },
      @{ Name = 'grouping'; Xml = '<?xml version="1.0"?><kml xmlns="http://www.opengis.net/kml/2.2"><Document><Placemark><Point><coordinates>1,2</coordinates></Point></Placemark><Placemark><Point><coordinates>3,4</coordinates></Point></Placemark></Document></kml>'; Code = 'R7_KML_GEOMETRY_IDENTITY_MISMATCH' }
    )
    foreach ($item in $negative) {
      $path = Join-Path $root ($item.Name + '.kml')
      Set-Content -LiteralPath $path -Encoding UTF8 -Value $item.Xml
      $caught = ''
      try {
        $candidate = Read-R7KmlCoordinateGroups -Path $path
        if ($item.Name -eq 'type' -and $candidate.geometryType -ne 'Polygon') { throw 'R7_KML_GEOMETRY_TYPE_MISMATCH' }
        if ($item.Name -eq 'grouping' -and (Test-R7CoordinateGroupsExact -Expected @(@(@(1.0, 2.0))) -Actual @($candidate.groups))) { throw 'R7_SELF_TEST_NEGATIVE_NOT_REJECTED' }
        if ($item.Name -eq 'grouping') { throw 'R7_KML_GEOMETRY_IDENTITY_MISMATCH' }
      } catch { $caught = [string]$_.Exception.Message }
      if ($caught -ne $item.Code) { throw "R7_SELF_TEST_EXPECTED_$($item.Code)_GOT_$caught" }
      $passed++
    }
    return [ordered]@{ success = $true; passed = $passed; total = 5; evidenceClass = 'SYNTHETIC_OFFLINE_KML_XML' }
  } finally {
    if (Test-Path -LiteralPath $root) { Remove-Item -LiteralPath $root -Recurse -Force }
  }
}

if ($LibraryOnly) { return }
if ($SelfTest) {
  try { Invoke-R7KmlParserSelfTest | ConvertTo-Json -Compress; exit 0 }
  catch { [ordered]@{ success = $false; code = [string]$_.Exception.Message } | ConvertTo-Json -Compress; exit 8 }
}

try {
  $result = Invoke-R7BrowserConsumerGate -EvidencePath $BrowserEvidencePath `
    -ResultPath $ValidatedResultPath -DownloadPath $KmlDownloadPath `
    -RunId $ExpectedRunId -CaseId $ExpectedCaseId -ExpiresAt $HandoffExpiresAt -WaitSeconds $TimeoutSeconds
  $result | ConvertTo-Json -Depth 6 -Compress
  exit 0
} catch {
  [ordered]@{ success = $false; code = [string]$_.Exception.Message } | ConvertTo-Json -Compress
  exit 7
}
