@echo off
chcp 65001 >nul
title FamilyBudget APK bauen - FGSeRaser
cd /d "%~dp0"
echo ============================================
echo   FamilyBudget 2.2 - APK bauen
echo   von FGSeRaser - github.com/FGSeRaser
echo ============================================
echo.
if not exist "lib\firebase_options.dart" goto fehltoptions
if not exist "android\app\google-services.json" goto fehltgs

echo [1/3] Pakete holen ...
call flutter pub get
if errorlevel 1 goto fehler

echo [2/3] Tests ...
call flutter test
if errorlevel 1 goto fehler

echo [3/3] APK bauen - dauert ein paar Minuten ...
call flutter build apk --release
if errorlevel 1 goto fehler

copy /y "build\app\outputs\flutter-apk\app-release.apk" "FamilyBudget-2.2.apk" >nul
echo.
echo FERTIG: FamilyBudget-2.2.apk liegt in diesem Ordner.
explorer /select,"%~dp0FamilyBudget-2.2.apk"
pause
exit /b 0

:fehltoptions
echo FEHLER: lib\firebase_options.dart fehlt.
echo Kopiere die Datei aus deinem alten Projektordner nach lib\
pause
exit /b 1

:fehltgs
echo FEHLER: android\app\google-services.json fehlt.
echo Kopiere die Datei aus deinem alten Projektordner nach android\app\
pause
exit /b 1

:fehler
echo.
echo FEHLER beim Bauen - mach bitte einen Screenshot vom Text oben und schick ihn mir.
pause
exit /b 1
