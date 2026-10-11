function Resolve-CoordinateThreeCaseLocalInputs {
  param(
    [Parameter(Mandatory = $true)] [System.Collections.IList] $Definitions,
    [scriptblock] $PromptForIndonesiaPath = { Read-Host '请输入原印尼图的本地绝对路径' }
  )

  foreach ($definition in $Definitions) {
    if ($definition -isnot [System.Collections.IDictionary]) {
      throw 'CASE_DEFINITION_INVALID'
    }

    $caseId = [string]$definition['caseId']
    $path = [string]$definition['path']
    if ([string]::IsNullOrWhiteSpace($path)) {
      if ($caseId -ne 'indonesia') { throw 'CASE_IMAGE_PATH_MISSING' }
      $path = [string](& $PromptForIndonesiaPath)
    }
    if ([string]::IsNullOrWhiteSpace($path)) {
      throw "CASE_IMAGE_PATH_MISSING_$($caseId.ToUpperInvariant())"
    }

    try {
      $resolved = (Resolve-Path -LiteralPath $path -ErrorAction Stop).Path
    } catch {
      throw "CASE_IMAGE_PATH_UNREADABLE_$($caseId.ToUpperInvariant())"
    }

    try {
      $actual = (Get-FileHash -LiteralPath $resolved -Algorithm SHA256 -ErrorAction Stop).Hash.ToLowerInvariant()
    } catch {
      throw "CASE_IMAGE_HASH_UNAVAILABLE_$($caseId.ToUpperInvariant())"
    }
    $expected = [string]$definition['imageSha256']
    if ($actual -ne $expected) {
      throw "CASE_IMAGE_SHA256_MISMATCH_$($caseId.ToUpperInvariant())"
    }
    $definition['path'] = $resolved
  }

  return $Definitions
}

function Resolve-CoordinateThreeCaseControlledVisitorId {
  param([Parameter(Mandatory = $true)] [string] $VisitorId)
  $normalized = $VisitorId.Trim()
  if ([string]::IsNullOrWhiteSpace($normalized) -or $normalized.Length -gt 256 -or $normalized -match '[\x00-\x1f\x7f]') {
    throw 'CONTROLLED_VISITOR_ID_INVALID'
  }
  return $normalized
}

function New-CoordinateThreeCaseClaimBody {
  param(
    [Parameter(Mandatory = $true)] [string] $RunId,
    [Parameter(Mandatory = $true)] [string] $BatchId,
    [Parameter(Mandatory = $true)] [System.Collections.IDictionary] $Definition,
    [Parameter(Mandatory = $true)] [string] $RecognitionRequestId,
    [Parameter(Mandatory = $true)] [string] $VisitorId
  )
  $controlledVisitorId = Resolve-CoordinateThreeCaseControlledVisitorId -VisitorId $VisitorId
  return [ordered]@{
    runId = $RunId
    batchId = $BatchId
    caseId = [string]$Definition['caseId']
    imageSha256 = [string]$Definition['imageSha256']
    recognitionRequestId = $RecognitionRequestId
    visitorId = $controlledVisitorId
  }
}

function Add-CoordinateThreeCaseJobVisitorIdentity {
  param(
    [Parameter(Mandatory = $true)] [Net.Http.HttpClient] $Client,
    [Parameter(Mandatory = $true)] [Net.Http.MultipartFormDataContent] $Form,
    [Parameter(Mandatory = $true)] [string] $VisitorId
  )
  $controlledVisitorId = Resolve-CoordinateThreeCaseControlledVisitorId -VisitorId $VisitorId
  $Client.DefaultRequestHeaders.Remove('x-visitor-id') | Out-Null
  $Client.DefaultRequestHeaders.Add('x-visitor-id', $controlledVisitorId)
  $Form.Add([Net.Http.StringContent]::new($controlledVisitorId), 'visitorId')
  return $controlledVisitorId
}

function Write-CoordinateThreeCaseBrowserConsumerEvidenceArtifact {
  param(
    [Parameter(Mandatory = $true)] [object] $Evidence,
    [Parameter(Mandatory = $true)] [string] $Path,
    [Parameter(Mandatory = $true)] [string] $ExpectedRunId,
    [Parameter(Mandatory = $true)] [string] $ExpectedCaseId
  )

  $expectedProperties = @(
    'schemaVersion', 'runId', 'caseId', 'bindingDigest', 'resultInstalled',
    'resultIdentityMatch', 'mapState', 'mapProvider', 'mapIdentityMatch',
    'authorityMutationCount', 'kmlDownloadTriggered', 'kmlIdentityMatch',
    'downloadFileName'
  )
  $actualProperties = @($Evidence.PSObject.Properties.Name | Sort-Object)
  $expectedSorted = @($expectedProperties | Sort-Object)
  $propertyDifference = @(Compare-Object -ReferenceObject $expectedSorted -DifferenceObject $actualProperties)
  if (($actualProperties.Count -ne $expectedSorted.Count) -or ($propertyDifference.Count -ne 0)) {
    throw 'R7_BROWSER_CONSUMER_EVIDENCE_SCHEMA_REJECTED'
  }
  foreach ($booleanField in @('resultInstalled', 'resultIdentityMatch', 'mapIdentityMatch', 'kmlDownloadTriggered', 'kmlIdentityMatch')) {
    if ($Evidence.$booleanField -isnot [bool] -or $Evidence.$booleanField -ne $true) {
      throw 'R7_BROWSER_CONSUMER_EVIDENCE_CONTRACT_REJECTED'
    }
  }
  $authorityMutationCountIsInteger = ($Evidence.authorityMutationCount -is [int]) -or ($Evidence.authorityMutationCount -is [long])
  $contractMatches = ([string]$Evidence.schemaVersion -eq 'coordinate_three_case_rc_r7_browser_evidence_v1') `
    -and ([string]$Evidence.runId -eq $ExpectedRunId) `
    -and ([string]$Evidence.caseId -eq $ExpectedCaseId) `
    -and ([string]$Evidence.bindingDigest -match '^[a-f0-9]{64}$') `
    -and ([string]$Evidence.mapState -eq 'READY') `
    -and ([string]$Evidence.mapProvider -eq 'OPENFREEMAP') `
    -and $authorityMutationCountIsInteger `
    -and ([int64]$Evidence.authorityMutationCount -eq 0) `
    -and ([string]$Evidence.downloadFileName -eq 'coordinates.kml')
  if (-not $contractMatches) {
    throw 'R7_BROWSER_CONSUMER_EVIDENCE_CONTRACT_REJECTED'
  }

  $resolvedPath = [IO.Path]::GetFullPath($Path)
  if (-not (Test-Path -LiteralPath (Split-Path -Parent $resolvedPath) -PathType Container)) {
    throw 'R7_BROWSER_CONSUMER_EVIDENCE_PARENT_MISSING'
  }
  $json = $Evidence | ConvertTo-Json -Depth 4 -Compress
  [IO.File]::WriteAllText($resolvedPath, $json, [Text.UTF8Encoding]::new($false))
  $roundTrip = Get-Content -LiteralPath $resolvedPath -Raw | ConvertFrom-Json
  if ([string]$roundTrip.bindingDigest -ne [string]$Evidence.bindingDigest) {
    throw 'R7_BROWSER_CONSUMER_EVIDENCE_WRITE_VERIFY_FAILED'
  }
  return [pscustomobject]@{
    path = $resolvedPath
    sha256 = (Get-FileHash -LiteralPath $resolvedPath -Algorithm SHA256).Hash.ToLowerInvariant()
  }
}

function Test-CoordinateThreeCaseReceiptAllowsNext {
  param([Parameter(Mandatory = $true)] [System.Collections.IDictionary] $Receipt)
  return [string]$Receipt['terminalStatus'] -eq 'SUCCEEDED' -and $Receipt['success'] -eq $true
}

function Invoke-CoordinateThreeCaseControlledRun {
  param(
    [Parameter(Mandatory = $true)] [System.Collections.IList] $Definitions,
    [Parameter(Mandatory = $true)] [scriptblock] $BeforeCases,
    [Parameter(Mandatory = $true)] [scriptblock] $InvokeCase,
    [Parameter(Mandatory = $true)] [scriptblock] $ConvertReceipt,
    [scriptblock] $AfterCase = { param($definition, $snapshot, $receipt) return $true },
    [Parameter(Mandatory = $true)] [scriptblock] $CloseRun,
    [Parameter(Mandatory = $true)] [scriptblock] $WriteEvidence
  )

  $receipts = [Collections.Generic.List[object]]::new()
  $failureCode = $null
  $runtimeReady = $false
  $closeReceipt = $null
  try {
    & $BeforeCases
    $runtimeReady = $true
    foreach ($definition in $Definitions) {
      try {
        $snapshot = & $InvokeCase $definition
        $receipt = & $ConvertReceipt $definition $snapshot
        $receipts.Add($receipt)
        if (-not (Test-CoordinateThreeCaseReceiptAllowsNext -Receipt $receipt)) {
          $failureCode = 'CASE_TERMINAL_FAILURE'
          break
        }
        $consumerReady = & $AfterCase $definition $snapshot $receipt
        if ($consumerReady -ne $true) {
          $failureCode = 'CASE_CONSUMER_GATE_FAILED'
          break
        }
      } catch {
        $failureCode = [string]$_.Exception.Message
        $receipts.Add([ordered]@{
          caseId = [string]$definition['caseId']
          terminalStatus = 'CLIENT_STOP'
          code = $failureCode
        })
        break
      }
    }
  } catch {
    $failureCode = [string]$_.Exception.Message
    $receipts.Add([ordered]@{ caseId = 'runtime'; terminalStatus = 'CLIENT_STOP'; code = $failureCode })
  } finally {
    try {
      $closeReceipt = & $CloseRun
    } catch {
      $closeReceipt = [ordered]@{ closed = $false; code = [string]$_.Exception.Message }
    }
  }

  $evidence = & $WriteEvidence $receipts $closeReceipt $failureCode $runtimeReady
  return [pscustomobject]@{
    receipts = $receipts
    closeReceipt = $closeReceipt
    failureCode = $failureCode
    runtimeReady = $runtimeReady
    evidence = $evidence
  }
}
