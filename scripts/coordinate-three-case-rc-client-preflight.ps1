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
