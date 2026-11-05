# FamilyBudget - GitHub-Secrets einrichten (einmalig)
# von FGSeRaser - https://github.com/FGSeRaser
#
# Legt die 6 Secrets an, die GitHub zum Bauen der signierten APK braucht.
# Mit GitHub-CLI (gh) geht alles automatisch, sonst Schritt fuer Schritt per Zwischenablage.

$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot
$repo = 'FGSeRaser/family_budget'

function B64($path) {
  if (-not (Test-Path $path)) { throw "Datei fehlt: $path" }
  [Convert]::ToBase64String([IO.File]::ReadAllBytes((Resolve-Path $path)))
}

# Passwort und Alias aus android\key.properties lesen
$props = @{}
Get-Content 'android\key.properties' | ForEach-Object {
  if ($_ -match '^\s*([^#=]+?)\s*=\s*(.*)$') { $props[$matches[1]] = $matches[2] }
}

$secrets = [ordered]@{
  'GOOGLE_SERVICES_JSON'  = B64 'android\app\google-services.json'
  'FIREBASE_OPTIONS_DART' = B64 'lib\firebase_options.dart'
  'KEYSTORE_BASE64'       = B64 'familybudget-release.jks'
  'KEYSTORE_PASSWORD'     = $props['storePassword']
  'KEY_ALIAS'             = $props['keyAlias']
  'KEY_PASSWORD'          = $props['keyPassword']
}

Write-Host '============================================'
Write-Host '  FamilyBudget - GitHub-Secrets einrichten'
Write-Host '  von FGSeRaser - github.com/FGSeRaser'
Write-Host '============================================'
Write-Host ''

$gh = Get-Command gh -ErrorAction SilentlyContinue
if ($gh) {
  & gh auth status *> $null
  if ($LASTEXITCODE -eq 0) {
    foreach ($k in $secrets.Keys) {
      $secrets[$k] | & gh secret set $k --repo $repo
      Write-Host "OK: $k"
    }
    Write-Host ''
    Write-Host 'FERTIG! Alle Secrets sind gesetzt. Beim naechsten Hochladen baut GitHub die APK.'
    Read-Host 'Enter zum Beenden'
    exit 0
  }
}

Write-Host 'GitHub-CLI nicht gefunden oder nicht angemeldet -> wir machen es per Browser.'
Write-Host 'Fuer jedes Secret: Name eintragen, Strg+V in das Feld "Secret", dann "Add secret".'
Write-Host ''
$i = 1
foreach ($k in $secrets.Keys) {
  Set-Clipboard -Value $secrets[$k]
  Start-Process "https://github.com/$repo/settings/secrets/actions/new"
  Write-Host "[$i/6] Name:  $k"
  Write-Host '       Wert ist in der Zwischenablage -> im Browser mit Strg+V einfuegen.'
  Read-Host '       Fertig? Enter fuer das naechste Secret'
  $i++
}
Set-Clipboard -Value ' '
Write-Host ''
Write-Host 'FERTIG! Jetzt unter Actions den letzten Lauf mit "Re-run all jobs" neu starten.'
Start-Process "https://github.com/$repo/actions"
Read-Host 'Enter zum Beenden'
