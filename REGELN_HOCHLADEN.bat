@echo off
chcp 65001 >nul
title FamilyBudget Firestore-Regeln - FGSeRaser
cd /d "%~dp0"
echo ============================================
echo   FamilyBudget 2.2 - Firestore-Regeln + Index hochladen
echo   von FGSeRaser - github.com/FGSeRaser
echo ============================================
echo.
where firebase >nul 2>nul
if errorlevel 1 goto ohnecli

call firebase deploy --only firestore:rules,firestore:indexes --project familybudget-ccbf0
if errorlevel 1 goto fehler
echo.
echo FERTIG: Regeln und Index sind online.
echo Der Index braucht danach noch ein paar Minuten, bis er fertig gebaut ist.
pause
exit /b 0

:ohnecli
echo Firebase-CLI ist nicht installiert. Kein Problem, so geht es per Browser:
echo.
echo REGELN:
echo 1. Die Regel-Seite oeffnet sich gleich im Browser
echo 2. Alles im Editor loeschen
echo 3. Inhalt der Datei firestore.rules aus diesem Ordner reinkopieren
echo 4. Auf Veroeffentlichen klicken
echo.
echo INDEX (neu in 2.2, einmalig):
echo 1. Die Index-Seite oeffnet sich gleich im Browser
echo 2. "Index erstellen" / "Create index"
echo 3. Sammlungs-ID: tx    Bereich: Sammlung
echo 4. Feld 1: type  = Aufsteigend
echo 5. Feld 2: dueDate = Aufsteigend
echo 6. Erstellen klicken (dauert ein paar Minuten)
echo.
echo Ohne Index funktioniert die App trotzdem, sie liest dann nur mehr Daten.
echo.
start "" notepad "%~dp0firestore.rules"
start "" "https://console.firebase.google.com/project/familybudget-ccbf0/firestore/rules"
start "" "https://console.firebase.google.com/project/familybudget-ccbf0/firestore/indexes"
pause
exit /b 0

:fehler
echo.
echo FEHLER - evtl. erst einmal "firebase login" ausfuehren und dann nochmal starten.
pause
exit /b 1
