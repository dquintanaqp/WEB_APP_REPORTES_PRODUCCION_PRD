# escape=`
FROM mcr.microsoft.com/windows/servercore:ltsc2025

SHELL ["powershell", "-NoProfile", "-Command", "$ErrorActionPreference='Stop'; $ProgressPreference='SilentlyContinue';"]

# --- 1) VC++ Runtime (requerido por msodbcsql18) ---
RUN Invoke-WebRequest -Uri "https://aka.ms/vs/17/release/vc_redist.x64.exe" -OutFile "vc_redist.x64.exe" ; `
    cmd /c "vc_redist.x64.exe /install /quiet /norestart" ; `
    if (($LASTEXITCODE -ne 0) -and ($LASTEXITCODE -ne 3010) -and ($LASTEXITCODE -ne 1638)) { throw ('VC runtime install failed. ExitCode=' + $LASTEXITCODE) } ; `
    Remove-Item "vc_redist.x64.exe" -Force

# --- 2) ODBC Driver 18 for SQL Server (x64) ---
RUN Invoke-WebRequest -Uri "https://go.microsoft.com/fwlink/?linkid=2202930" -OutFile "msodbcsql.msi" ; `
    cmd /c "msiexec.exe /i msodbcsql.msi /quiet /norestart IACCEPTMSODBCSQLLICENSETERMS=YES /L*v C:\msodbcsql_install.log" ; `
    $ec = $LASTEXITCODE ; `
    if (($ec -ne 0) -and ($ec -ne 3010) -and ($ec -ne 1638)) { `
        Write-Host '---- msodbcsql_install.log (tail) ----' ; `
        Get-Content C:\msodbcsql_install.log -Tail 200 | Out-String | Write-Host ; `
        throw ('msodbcsql install failed. ExitCode=' + $ec) `
    } ; `
    Remove-Item "msodbcsql.msi" -Force ; `
    Get-OdbcDriver | Select-Object Name,Platform,Version | Format-Table -AutoSize | Out-String | Write-Host

# --- 3) Python 3.11.7 (hard pinned) ---
RUN Write-Host 'Downloading Python 3.11.7...'; `
    Invoke-WebRequest -Uri 'https://www.python.org/ftp/python/3.11.7/python-3.11.7-amd64.exe' -OutFile 'python-installer.exe'; `
    Start-Process .\python-installer.exe -ArgumentList '/quiet InstallAllUsers=1 PrependPath=1 Include_test=0' -Wait; `
    Remove-Item 'python-installer.exe' -Force; `
    $env:Path = [System.Environment]::GetEnvironmentVariable('Path','Machine'); `
    $py = 'C:\Program Files\Python311\python.exe'; `
    if (!(Test-Path $py)) { throw ('Python not found at ' + $py) }; `
    & $py --version; `
    & $py -m pip --version

WORKDIR C:\app

# --- 4) Install dependencies ---
COPY flask_pdf_viewer\requirements.txt C:\app\requirements.txt
RUN pip install --no-cache-dir -r C:\app\requirements.txt

# --- 5) Copy application code ---
COPY flask_pdf_viewer C:\app\flask_pdf_viewer

ENV FLASK_ENV=production
EXPOSE 5000

CMD ["python", "-m", "waitress", "--listen=0.0.0.0:5000", "flask_pdf_viewer.app:app"]
