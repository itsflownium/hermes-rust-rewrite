param([string]$Destination = "")
$ErrorActionPreference = "Stop"
Push-Location (Join-Path $PSScriptRoot "..")
try {
    if (!$Destination) { $Destination = Join-Path $PWD "dist" }
    $stage = Join-Path ([System.IO.Path]::GetTempPath()) ([guid]::NewGuid().ToString())
    $root = Join-Path $stage "Hermes"
    $null = New-Item -ItemType Directory -Path "$root/Notices" -Force
    $null = New-Item -ItemType Directory -Path $Destination -Force
    # hermes-python-host embeds this exact CPython; it ships in python\ with its DLLs beside the host.
    if (!$env:PYO3_PYTHON) { $env:PYO3_PYTHON = & (Join-Path $PSScriptRoot "python-runtime.ps1") }
    cargo build --locked --release --features embedded-herdr -p hermes-desktop
    if ($LASTEXITCODE -ne 0) { throw "Cargo build failed ($LASTEXITCODE)" }
    cargo build --locked --release -p hermes-python-host
    if ($LASTEXITCODE -ne 0) { throw "Cargo build failed ($LASTEXITCODE)" }
    $meta = cargo metadata --no-deps --format-version 1 | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0) { throw "Cargo metadata failed" }
    Copy-Item "$($meta.target_directory)/release/hermes-desktop.exe" "$root/Hermes.exe"
    $fixture = & (Join-Path $PSScriptRoot "prepare-herdr.ps1")
    $fixtureDir = Split-Path -Parent $fixture
    Copy-Item (Join-Path $fixtureDir "conpty") $root -Recurse
    Copy-Item (Join-Path $fixtureDir "THIRD-PARTY-NOTICES") "$root/Notices/Herdr-THIRD-PARTY-NOTICES" -Recurse
    Copy-Item (Join-Path $fixtureDir "LICENSE") "$root/Notices/Herdr-LICENSE"
    Copy-Item "$($meta.target_directory)/release/hermes-python-host.exe" $root
    $prefix = & $env:PYO3_PYTHON -c "import sys; print(sys.base_prefix)"
    # Everything plugins can use; not tests, IDLE, Tk, headers, import libraries or the interpreter executables.
    robocopy $prefix (Join-Path $root "python") /E /NFL /NDL /NJH /NJS /NP /XD (Join-Path $prefix "Lib/test") (Join-Path $prefix "Lib/idlelib") (Join-Path $prefix "Lib/tkinter") (Join-Path $prefix "Lib/turtledemo") (Join-Path $prefix "tcl") (Join-Path $prefix "include") (Join-Path $prefix "libs") (Join-Path $prefix "Scripts") /XF python.exe pythonw.exe _tkinter.pyd tcl*.dll tk*.dll
    if ($LASTEXITCODE -ge 8) { throw "Could not copy the Python runtime ($LASTEXITCODE)" }
    $global:LASTEXITCODE = 0
    # The loader finds python3XX.dll and the VC runtime next to the executable.
    Get-ChildItem $prefix -File | Where-Object { $_.Name -match '^(python3\d*|vcruntime140(_1)?)\.dll$' } | Copy-Item -Destination $root
    $reported = & (Join-Path $root "hermes-python-host.exe") --version
    if ($LASTEXITCODE -ne 0 -or $reported -notmatch '\\Hermes\\python$') { throw "hermes-python-host does not run the bundled Python: $reported" }
    Copy-Item "assets/icon.ico" "$root/Hermes.ico"
    & $env:PYO3_PYTHON "scripts/reliquary-notices.py" stage "$root/Notices" "$root/Hermes.exe"
    if ($LASTEXITCODE -ne 0) { throw "Could not stage locked upstream licenses ($LASTEXITCODE)" }
    & $env:PYO3_PYTHON "scripts/stage-whatsapp-bridge.py" "$root/whatsapp-bridge"
    if ($LASTEXITCODE -ne 0) { throw "Could not stage the WhatsApp bridge ($LASTEXITCODE)" }
    Get-ChildItem "crates/desktop/assets/fonts" -File | Where-Object { $_.Name -match '(NOTICE|LICENSE).*\.txt$' } | Copy-Item -Destination "$root/Notices"
    # Authenticode, when a certificate is given (a .pfx path and its password).
    if ($env:WINDOWS_CERTIFICATE) {
        $signtool = Get-ChildItem "${env:ProgramFiles(x86)}\Windows Kits\10\bin\*\x64\signtool.exe" | Sort-Object FullName | Select-Object -Last 1
        if (!$signtool) { throw "signtool is not installed" }
        function Sign($file) {
            & $signtool.FullName sign /f $env:WINDOWS_CERTIFICATE /p $env:WINDOWS_CERTIFICATE_PASSWORD /fd SHA256 /tr http://timestamp.digicert.com /td SHA256 $file
            if ($LASTEXITCODE -ne 0) { throw "Could not sign $file" }
        }
        Sign "$root/Hermes.exe"; Sign "$root/hermes-python-host.exe"
    }
    $iscc = @("${env:ProgramFiles(x86)}\Inno Setup 6\ISCC.exe", "$env:ProgramFiles\Inno Setup 6\ISCC.exe") | Where-Object { Test-Path $_ } | Select-Object -First 1
    if ($iscc) {
        $version = ($meta.packages | Where-Object { $_.name -eq "hermes-desktop" }).version
        & $iscc /Qp "/DSourceDir=$root" "/DOutputDir=$((Resolve-Path $Destination).Path)" "/DAppVersion=$version" packaging/windows/hermes.iss
        if ($LASTEXITCODE -ne 0) { throw "Could not build the installer ($LASTEXITCODE)" }
        if ($env:WINDOWS_CERTIFICATE) { Sign (Join-Path $Destination "Hermes-windows-AMD64-setup.exe") }
    } else { Write-Warning "Inno Setup 6 is not installed; no installer was built" }
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $archive = Join-Path $stage "Hermes.zip"
    [System.IO.Compression.ZipFile]::CreateFromDirectory(
        $root, $archive, [System.IO.Compression.CompressionLevel]::Optimal, $true)
    Copy-Item -LiteralPath $archive -Destination (Join-Path $Destination "Hermes-windows-$env:PROCESSOR_ARCHITECTURE.zip") -Force
} finally {
    if ($stage -and (Test-Path $stage)) { Remove-Item -LiteralPath $stage -Recurse -Force }
    Pop-Location
}
