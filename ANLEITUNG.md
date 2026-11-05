# FamilyBudget 2.2 – so bringst du das Update auf dein Handy

von **FGSeRaser** – https://github.com/FGSeRaser

## 1. Dateien einspielen
1. Die ZIP in deinen bisherigen FamilyBudget-Ordner entpacken und **alles überschreiben**.
2. Deine geheimen Dateien bleiben dabei erhalten. Sie sind nicht in der ZIP:
   - `lib/firebase_options.dart`
   - `android/app/google-services.json`

## 2. Firestore-Regeln + Index hochladen (wichtig!)
Ohne die neuen Regeln gehen Zahlungen, Konten, Einladungen usw. nicht.
Neu in 2.2: Es gibt zusätzlich einen **Index** für die Kontostand-Prognose (spart Firebase-Kosten).
- Doppelklick auf **`REGELN_HOCHLADEN.bat`** – lädt Regeln **und** Index hoch.
- Ohne Firebase-CLI öffnet sich zusätzlich die Index-Seite. Dort einmalig anlegen:
  Sammlung `tx`, Felder `type` (aufsteigend) und `dueDate` (aufsteigend).
- Hast du die Firebase-CLI nicht, öffnet sich automatisch die Firebase-Webseite und die Regel-Datei.
  Dann den Inhalt reinkopieren und auf **Veröffentlichen** klicken.

## 3. E-Mail-Anmeldung einschalten (einmalig, 1 Minute)
Firebase Console → **Authentication** → **Sign-in method**:
- **Anonym** muss an sein (war schon).
- **E-Mail/Passwort** einschalten → Speichern.

Damit funktioniert „Konto sichern“. Nach einer Neuinstallation oder auf einem neuen Handy kommst du damit wieder in deine Familie.

## 4. APK bauen
- Doppelklick auf **`APK_BAUEN.bat`**.
- Am Ende liegt **`FamilyBudget-2.2.apk`** im Ordner. Aufs Handy kopieren und installieren.
  Die App wird dabei aktualisiert, deine Daten bleiben erhalten.

## Neu in 2.2
| Bereich | Neu |
|---|---|
| Themes | **10 kräftige Themes** (Blood Moon, Neon Nights, AMOLED Gold, Matrix, Synthwave, Polarlicht, Sunset Blaze, Tropical, Royal Blue, Candy). Auswahl mit Farbvorschau unter **Einstellungen → Themen**. |
| Kosten | Die App lädt nur noch den gewählten Monat statt aller Buchungen – deutlich weniger Firebase-Lesevorgänge. |
| Sicherheit | Einladungs-QRs kann nur noch die eigene Familie löschen. |

## Neu in 2.1
| Bereich | Neu |
|---|---|
| Benachrichtigungen | Beim ersten Start fragt die App, ob sie Benachrichtigungen schicken darf → **Zulassen**. Unter **Einstellungen → Benachrichtigungen** gibt es einen **Test-Knopf**. Dort stellst du auch ein, wie viele Tage vor einer Zahlung und um welche Uhrzeit erinnert wird. |
| Termine | Unter **Planer → Termine** wählst du beim Anlegen die Erinnerung (z.B. 1 Stunde vorher). |
| Arbeit | **Profil & Zuschläge**: Stundenlohn, Steuerklasse, Nacht-, Samstags-, Sonntags- und Feiertagszuschlag. Das Netto wird jetzt mit echter Lohnsteuer berechnet. Krankenkasse, Kirchensteuer und Bundesland kommen aus dem Tab **Steuern**. |
| Schichten | Tippen = Schicht eintragen, gedrückt halten = bearbeiten oder löschen. Pausen werden abgezogen. |
| Stoppuhr | Läuft auch, wenn die App zu ist. Oben in der Statusleiste steht „Arbeitszeit läuft“. |
| Einkauf | Kassenzettel scannen → Summe prüfen → **Alles verbuchen** legt eine bezahlte Ausgabe an. |

**Tipp Erinnerungen:** Unter Einstellungen → Benachrichtigungen erscheint eventuell „Minutengenaue Erinnerungen erlauben“. Antippen und den Schalter umlegen, dann kommen Erinnerungen pünktlich.

## Was in 2.0 neu war
| Bereich | Neu |
|---|---|
| Beitreten | **Einladungs-QR** unter Einstellungen erstellen. Die andere Person scannt ihn und ist sofort drin (10 Minuten gültig). Beitritt per Code geht auch, du bestätigst dann unter **Einstellungen → Mitglieder → Beitrittsanfragen**. |
| Gehalt | Echte **Lohnsteuer, Soli, Kirchensteuer** nach Steuerklasse (2025/2026). Unter **Steuern** gibt es eine Probe-Rechnung. |
| Wiederkehrend | Keine doppelten Buchungen mehr. Bearbeiten, Pausieren und Löschen wirken auf die kommenden Termine. |
| Zahlungen | Monat mit ◀ ▶ wechseln, Bezahlt/Offen per langem Drücken, Budget-Warnung beim Speichern. |
| Übersicht | „Noch offen“, Kontostand-Prognose zum Monatsende, Budget-Balken. |
| Einstellungen | Startsaldo, **Konto sichern**, Beitrittsanfragen. Kategorien umbenennen ändert auch alle alten Buchungen. |
| Backup | Der Import sperrt dich nicht mehr aus. Nach einer Neuinstallation wird aus dem Backup eine neue Familie gebaut. |
| App-Sperre | Sperrt erst nach 1 Minute im Hintergrund. Kamera und Belege gehen dabei nicht mehr verloren. |

## Automatisch bauen auf GitHub (optional)
Wenn du den Ordner auf GitHub hochlädst, prüft GitHub den Code automatisch (Reiter **Actions**).
Für eine fertige APK direkt von GitHub legst du zwei Secrets an.
Pfad: Repo → Settings → Secrets and variables → Actions.
- `GOOGLE_SERVICES_JSON`: Inhalt von `android/app/google-services.json` als Base64
- `FIREBASE_OPTIONS_DART`: Inhalt von `lib/firebase_options.dart` als Base64

Base64 erzeugen (PowerShell):
```
[Convert]::ToBase64String([IO.File]::ReadAllBytes("android\app\google-services.json")) | Set-Clipboard
```
