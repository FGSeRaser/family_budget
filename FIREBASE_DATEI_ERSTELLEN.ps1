# FamilyBudget – erzeugt lib/firebase_options.dart aus android/app/google-services.json
# von FGSeRaser (https://github.com/FGSeRaser)
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$gs = Join-Path $root 'android\app\google-services.json'

Write-Host '============================================'
Write-Host '  FamilyBudget - firebase_options.dart erstellen'
Write-Host '  von FGSeRaser - github.com/FGSeRaser'
Write-Host '============================================'
Write-Host ''

if (-not (Test-Path $gs)) {
    Write-Host 'FEHLER: android\app\google-services.json fehlt.' -ForegroundColor Red
    Write-Host 'Lade sie in der Firebase Console herunter (die Seite oeffnet sich gleich)'
    Write-Host 'und kopiere sie nach android\app\ - dann dieses Skript nochmal starten.'
    Start-Process 'https://console.firebase.google.com/project/familybudget-ccbf0/settings/general'
    exit 1
}

$j = Get-Content $gs -Raw -Encoding UTF8 | ConvertFrom-Json
$pkg = 'com.example.familybudget'
$client = $j.client | Where-Object { $_.client_info.android_client_info.package_name -eq $pkg } | Select-Object -First 1
if (-not $client) {
    Write-Host "Hinweis: keine App mit Paketname $pkg gefunden, nehme die erste." -ForegroundColor Yellow
    $client = $j.client | Select-Object -First 1
}

$apiKey  = $client.api_key[0].current_key
$appId   = $client.client_info.mobilesdk_app_id
$sender  = $j.project_info.project_number
$project = $j.project_info.project_id
$bucket  = $j.project_info.storage_bucket

$dart = @"
// Automatisch erzeugt aus android/app/google-services.json
// FamilyBudget – von FGSeRaser (https://github.com/FGSeRaser)
import 'package:firebase_core/firebase_core.dart' show FirebaseOptions;
import 'package:flutter/foundation.dart' show defaultTargetPlatform, kIsWeb, TargetPlatform;

class DefaultFirebaseOptions {
  static FirebaseOptions get currentPlatform {
    if (kIsWeb) {
      throw UnsupportedError('Web ist in FamilyBudget nicht eingerichtet.');
    }
    if (defaultTargetPlatform == TargetPlatform.android) return android;
    throw UnsupportedError('FamilyBudget ist nur fuer Android eingerichtet.');
  }

  static const FirebaseOptions android = FirebaseOptions(
    apiKey: '$apiKey',
    appId: '$appId',
    messagingSenderId: '$sender',
    projectId: '$project',
    storageBucket: '$bucket',
  );
}
"@

$out = Join-Path $root 'lib\firebase_options.dart'
[System.IO.File]::WriteAllText($out, $dart, (New-Object System.Text.UTF8Encoding($false)))
Write-Host "FERTIG: lib\firebase_options.dart wurde erstellt." -ForegroundColor Green
Write-Host "Projekt: $project   App: $appId"
