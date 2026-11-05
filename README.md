# 💰 FamilyBudget - Haushalt & Finanzen (Android)

Willkommen bei **FamilyBudget**, entwickelt von **FGSeRaser**. 
Diese Flutter-App ermöglicht es Familien und WGs, ihre Finanzen, Budgets und Arbeitszeiten gemeinsam in Echtzeit zu verwalten.

## ✨ Features
- **Familien-Gruppen:** Gemeinsame Verwaltung via Firebase.
- **Finanzübersicht:** Einnahmen, Ausgaben und grafische Auswertungen.
- **Budget-Planer:** Monatliche Limits setzen und einhalten.
- **Brutto-Netto-Rechner:** Integriertes Tool für die Gehaltsplanung (DE).
- **Arbeitszeit-Tracker:** Stoppuhr, Schichtprofile (Früh/Spät/Nacht), Pausen, Brutto/Netto mit echter Lohnsteuer.
- **Zuschläge:** Nacht, Samstag, Sonntag, Feiertag (Feiertage je Bundesland), steuerfreie Anteile nach § 3b EStG.
- **Planer:** Tagebuch, Termine mit Erinnerung, gemeinsame Einkaufsliste.
- **Kassenzettel-Scanner:** Summe per Foto erkennen und direkt als Ausgabe verbuchen.
- **Benachrichtigungen:** Termine, fällige Zahlungen und „Arbeitszeit läuft“-Anzeige.
- **Sicherheit:** Schutz durch Biometrie und verschlüsselte Backups.
- **Wiederkehrende Zahlungen:** Miete, Abos, Gehalt – automatisch für 90 Tage im Voraus.
- **Kontostand-Prognose:** Startsaldo + alle Buchungen bis Monatsende.
- **Konto sichern:** Optional mit E-Mail, damit man nach Neuinstallation wieder reinkommt.

## 🛠 Tech-Stack
- **Framework:** Flutter (Dart)
- **Backend:** Firebase (Firestore, Auth)
- **Status:** Optimiert für Android.

## 🤝 Mitwirken & Entwicklung
Da ich die App aktuell auf **Android** entwickle, freue ich mich besonders über Unterstützung in folgenden Bereichen:
- **iOS-Testing:** Da ich kein macOS/iOS-Setup habe, ist Hilfe beim Testen auf iPhones sehr willkommen.
- **UI/UX:** Vorschläge für ein noch intuitiveres Design.
- **Refactoring:** Hilfe bei der Aufteilung der großen `main.dart` in kleinere Module.
## 🌱 Ein Wort an die Community
Ich bin noch ein **Anfänger** im Bereich Flutter und App-Entwicklung. Dieses Projekt ist mein Herzensprojekt, um zu lernen. 
Wenn du helfen möchtest:
- **Sei bitte geduldig:** Ich lerne noch, wie man Code strukturiert.
- **Erklärungen sind willkommen:** Wenn du einen Pull Request stellst, erkläre mir gerne kurz, *warum* du den Code so geändert hast, damit ich daraus lernen kann.
- **Keine Scheu:** Jeder Tipp, egal wie klein, hilft mir weiter!

## 🚀 Setup
1. `git clone https://github.com/FGSeRaser/family_budget.git`
2. `flutter pub get`
3. Firebase Projekt erstellen, `flutterfire configure` ausführen (erzeugt `lib/firebase_options.dart`)
   und `google-services.json` in `android/app/` ablegen.
4. Firestore Regeln aus der Datei `firestore.rules` übernehmen (`firebase deploy --only firestore:rules`).
5. In Firebase unter **Authentication → Sign-in method** „Anonym“ und „E-Mail/Passwort“ einschalten.

## 📝 Änderungen in 2.2
- **10 neue kräftige Themes:** Blood Moon, Neon Nights, AMOLED Gold, Matrix, Synthwave, Polarlicht, Sunset Blaze, Tropical, Royal Blue, Candy – mit farbiger Kopfleiste, Farbverlauf im Hintergrund und leuchtenden Karten.
- **Neue Theme-Auswahl** mit Farbvorschau (Einstellungen → Themen).
- **Weniger Firestore-Lesevorgänge:** Übersicht, Zahlungen, Budgets und Jahresbericht laden nur noch den gewählten Monat bzw. das Jahr statt aller Buchungen. Die Kontostand-Prognose rechnet Firestore selbst als Summe aus. Das hält die App bei vielen Nutzern im kostenlosen Firebase-Plan.
- **Neuer Firestore-Index** (`firestore.indexes.json`) für die Prognose – wird mit `REGELN_HOCHLADEN.bat` hochgeladen. Ohne Index funktioniert alles weiter, nur mit mehr Lesevorgängen.
- **Sicherheit:** Einladungs-QRs können nur noch von Mitgliedern der Familie gelöscht werden.
- Richtige Meldung, wenn ein Einladungs-QR abgelaufen ist.

## 📝 Änderungen in 2.1
- Benachrichtigungen funktionieren: Termine (Erinnerung wählbar), fällige Zahlungen (Tage vorher + Uhrzeit), Stoppuhr in der Statusleiste, Test-Knopf in den Einstellungen.
- Arbeit: echtes Netto statt Pauschale, Samstags-/Sonntags-/Feiertagszuschläge, Pausen, Einträge-Liste mit Löschen, Hochrechnung bis Monatsende, Schichten bearbeiten/löschen.
- Planer: Listen laden ohne extra Firestore-Index, Tagebuch-Einträge löschbar (gedrückt halten).
- Einkaufsliste: Verbuchen als bezahlte Ausgabe, Scanner verwechselt Bargeld/Rückgeld nicht mehr mit der Summe.
- Firestore-Regeln für Termine, Tagebuch (privat), Einkaufsliste, Arbeitszeiten und Schichtprofile.

## 📝 Änderungen in 2.0
- Firestore-Regeln vollständig (Zahlungen, Konten, Regeln, Einladungen fehlten).
- Beitritt per Code: Owner kann Anfragen jetzt in den Einstellungen bestätigen.
- Einladungs-QR auf Knopfdruck (10 Minuten gültig, ohne Bestätigung).
- Lohnsteuer, Soli und Kirchensteuer nach Tarif 2025/2026, Sozialabgaben mit Beitragsbemessungsgrenzen.
- Wiederkehrende Zahlungen: keine Doppelbuchungen mehr, Bearbeiten/Pausieren wirkt auf zukünftige Termine.
- Bezahlt/Offen einheitlich, Budget-Warnung funktioniert, Kategorien umbenennen ändert alle Buchungen.
- Backup-Import sperrt einen nicht mehr aus und kann nach Neuinstallation eine neue Familie herstellen.
- App-Sperre lädt die App nicht mehr neu (Kamera/Belege gehen nicht mehr verloren).

---
Erstellt von [FGSeRaser](https://github.com/FGSeRaser)
