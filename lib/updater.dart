// FamilyBudget – Auto-Update über GitHub-Releases
// von FGSeRaser (https://github.com/FGSeRaser)
//
// Die App fragt beim Start das neueste Release von github.com/FGSeRaser/family_budget ab.
// Gibt es eine neuere Version, wird der Nutzer gewarnt und kann das Update direkt installieren.
// Das klappt nur, weil jede APK mit demselben Release-Schlüssel signiert ist.
import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:firebase_auth/firebase_auth.dart';
import 'package:flutter/material.dart';
import 'package:ota_update/ota_update.dart';
import 'package:package_info_plus/package_info_plus.dart';
import 'package:shared_preferences/shared_preferences.dart';

const String kUpdateRepo = 'FGSeRaser/family_budget';

class UpdateInfo {
  final String version; // z. B. 2.3.0
  final String apkUrl;
  final String? sha256;
  final String notes;
  final String pageUrl;

  const UpdateInfo({
    required this.version,
    required this.apkUrl,
    required this.sha256,
    required this.notes,
    required this.pageUrl,
  });
}

/// Vergleicht Versionen wie "2.10.0" und "2.9.1". Ergebnis > 0, wenn [a] neuer ist.
int compareVersions(String a, String b) {
  List<int> parts(String v) => v
      .replaceFirst(RegExp(r'^[vV]'), '')
      .split('+')
      .first
      .split('.')
      .map((p) => int.tryParse(RegExp(r'\d+').stringMatch(p) ?? '') ?? 0)
      .toList();
  final pa = parts(a);
  final pb = parts(b);
  for (var i = 0; i < 3; i++) {
    final x = i < pa.length ? pa[i] : 0;
    final y = i < pb.length ? pb[i] : 0;
    if (x != y) return x.compareTo(y);
  }
  return 0;
}

class AppUpdater {
  static const _kSkipped = 'update_skipped_version';
  static const _kLastCheck = 'update_last_check';
  static bool _running = false;

  /// Holt das neueste Release. Gibt null zurück, wenn es nichts Neueres gibt.
  static Future<UpdateInfo?> fetchNewer() async {
    final info = await PackageInfo.fromPlatform();
    final current = info.version;

    final client = HttpClient()..connectionTimeout = const Duration(seconds: 10);
    try {
      final req = await client.getUrl(Uri.parse('https://api.github.com/repos/$kUpdateRepo/releases/latest'));
      req.headers.set(HttpHeaders.acceptHeader, 'application/vnd.github+json');
      req.headers.set(HttpHeaders.userAgentHeader, 'FamilyBudget-App');
      final res = await req.close().timeout(const Duration(seconds: 15));
      if (res.statusCode != 200) return null;
      final body = await res.transform(utf8.decoder).join();
      final j = jsonDecode(body) as Map<String, dynamic>;

      final tag = (j['tag_name'] ?? '').toString();
      if (tag.isEmpty || compareVersions(tag, current) <= 0) return null;

      final assets = (j['assets'] as List? ?? const []).cast<Map<String, dynamic>>();
      Map<String, dynamic>? apk;
      for (final a in assets) {
        if ((a['name'] ?? '').toString().toLowerCase().endsWith('.apk')) {
          apk = a;
          break;
        }
      }
      if (apk == null) return null;

      // GitHub liefert die Prüfsumme mit ("sha256:..."), damit prüfen wir den Download
      String? sha;
      final digest = (apk['digest'] ?? '').toString();
      if (digest.startsWith('sha256:')) sha = digest.substring(7);

      return UpdateInfo(
        version: tag.replaceFirst(RegExp(r'^[vV]'), ''),
        apkUrl: (apk['browser_download_url'] ?? '').toString(),
        sha256: sha,
        notes: (j['body'] ?? '').toString(),
        pageUrl: (j['html_url'] ?? '').toString(),
      );
    } catch (e) {
      debugPrint('Update-Prüfung fehlgeschlagen: $e');
      return null;
    } finally {
      client.close(force: true);
    }
  }

  /// Beim App-Start: höchstens alle 6 Stunden prüfen, übersprungene Version nicht nochmal zeigen.
  static Future<void> checkOnStart(BuildContext context) async {
    if (!Platform.isAndroid) return;
    try {
      final prefs = await SharedPreferences.getInstance();
      final last = prefs.getInt(_kLastCheck) ?? 0;
      final now = DateTime.now().millisecondsSinceEpoch;
      if (now - last < const Duration(hours: 6).inMilliseconds) return;
      await prefs.setInt(_kLastCheck, now);

      final upd = await fetchNewer();
      if (upd == null) return;
      if (prefs.getString(_kSkipped) == upd.version) return;
      if (!context.mounted) return;
      await showUpdateDialog(context, upd);
    } catch (e) {
      debugPrint('Update-Prüfung beim Start fehlgeschlagen: $e');
    }
  }

  /// Über den Knopf in den Einstellungen.
  static Future<void> checkManually(BuildContext context) async {
    if (!Platform.isAndroid) {
      _snack(context, 'Updates gibt es nur für Android.');
      return;
    }
    _snack(context, 'Suche nach Updates …');
    final upd = await fetchNewer();
    if (!context.mounted) return;
    if (upd == null) {
      final info = await PackageInfo.fromPlatform();
      if (!context.mounted) return;
      _snack(context, 'Du hast die neueste Version (${info.version}).');
      return;
    }
    await showUpdateDialog(context, upd);
  }

  static void _snack(BuildContext context, String msg) {
    ScaffoldMessenger.of(context).showSnackBar(SnackBar(content: Text(msg)));
  }

  static Future<void> showUpdateDialog(BuildContext context, UpdateInfo upd) async {
    final info = await PackageInfo.fromPlatform();
    final user = FirebaseAuth.instance.currentUser;
    final notSecured = user == null || user.isAnonymous;
    if (!context.mounted) return;

    final choice = await showDialog<String>(
      context: context,
      builder: (ctx) => AlertDialog(
        icon: const Icon(Icons.system_update, size: 36),
        title: Text('Update auf ${upd.version}'),
        content: SingleChildScrollView(
          child: Column(
            mainAxisSize: MainAxisSize.min,
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Text('Installiert: ${info.version}\nNeu: ${upd.version}'),
              const SizedBox(height: 12),
              const Text(
                '⚠️ Bitte vor dem Update lesen:\n'
                '• Die App wird geschlossen und neu installiert.\n'
                '• Deine Familien-Daten liegen in der Cloud und bleiben erhalten.\n'
                '• Eine laufende Stoppuhr vorher am besten stoppen.\n'
                '• Android fragt evtl., ob FamilyBudget Apps installieren darf → erlauben.',
              ),
              if (notSecured) ...[
                const SizedBox(height: 12),
                Container(
                  padding: const EdgeInsets.all(10),
                  decoration: BoxDecoration(
                    color: Colors.orange.withValues(alpha: 0.18),
                    borderRadius: BorderRadius.circular(10),
                    border: Border.all(color: Colors.orange),
                  ),
                  child: const Text(
                    'Dein Konto ist noch nicht mit E-Mail gesichert. Normalerweise bleibt beim Update alles erhalten – '
                    'zur Sicherheit vorher unter Einstellungen → „Konto sichern“ eine E-Mail hinterlegen.',
                  ),
                ),
              ],
            ],
          ),
        ),
        actions: [
          TextButton(onPressed: () => Navigator.pop(ctx, 'skip'), child: const Text('Diese Version überspringen')),
          TextButton(onPressed: () => Navigator.pop(ctx, 'later'), child: const Text('Später')),
          FilledButton.icon(
            onPressed: () => Navigator.pop(ctx, 'install'),
            icon: const Icon(Icons.download),
            label: const Text('Jetzt aktualisieren'),
          ),
        ],
      ),
    );

    if (choice == 'skip') {
      final prefs = await SharedPreferences.getInstance();
      await prefs.setString(_kSkipped, upd.version);
      return;
    }
    if (choice != 'install' || !context.mounted) return;
    await _install(context, upd);
  }

  static Future<void> _install(BuildContext context, UpdateInfo upd) async {
    if (_running) return;
    _running = true;
    final progress = ValueNotifier<String>('Download startet …');
    final percent = ValueNotifier<double?>(null);
    StreamSubscription<OtaEvent>? sub;
    var dialogOpen = true;

    showDialog<void>(
      context: context,
      barrierDismissible: false,
      builder: (ctx) => AlertDialog(
        title: Text('FamilyBudget ${upd.version}'),
        content: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            ValueListenableBuilder<double?>(
              valueListenable: percent,
              builder: (_, v, __) => LinearProgressIndicator(value: v),
            ),
            const SizedBox(height: 12),
            ValueListenableBuilder<String>(valueListenable: progress, builder: (_, t, __) => Text(t)),
          ],
        ),
        actions: [
          TextButton(
            onPressed: () {
              sub?.cancel();
              _running = false;
              dialogOpen = false;
              Navigator.pop(ctx);
            },
            child: const Text('Abbrechen'),
          ),
        ],
      ),
    );

    void close(String? msg) {
      _running = false;
      sub?.cancel();
      if (!context.mounted) return;
      if (dialogOpen) {
        dialogOpen = false;
        Navigator.of(context, rootNavigator: true).pop();
      }
      if (msg != null) _snack(context, msg);
    }

    try {
      sub = OtaUpdate()
          .execute(
            upd.apkUrl,
            destinationFilename: 'FamilyBudget-${upd.version}.apk',
            sha256checksum: upd.sha256,
          )
          .listen(
        (OtaEvent e) {
          switch (e.status) {
            case OtaStatus.DOWNLOADING:
              final p = double.tryParse(e.value ?? '');
              percent.value = p == null ? null : (p / 100).clamp(0.0, 1.0);
              progress.value = 'Lade herunter … ${e.value ?? ''} %';
              break;
            case OtaStatus.INSTALLING:
              progress.value = 'Installation startet – bitte bestätigen.';
              close(null);
              break;
            case OtaStatus.INSTALLATION_DONE:
              close(null);
              break;
            case OtaStatus.PERMISSION_NOT_GRANTED_ERROR:
              close('Bitte erlaube FamilyBudget, Apps zu installieren, und versuche es nochmal.');
              break;
            case OtaStatus.CHECKSUM_ERROR:
              close('Download beschädigt (Prüfsumme falsch). Bitte nochmal versuchen.');
              break;
            case OtaStatus.ALREADY_RUNNING_ERROR:
              close('Ein Update läuft schon.');
              break;
            case OtaStatus.CANCELED:
              close('Update abgebrochen.');
              break;
            default:
              close('Update fehlgeschlagen (${e.status.name}): ${e.value ?? ''}');
          }
        },
        onError: (Object err) => close('Update fehlgeschlagen: $err'),
      );
    } catch (e) {
      close('Update fehlgeschlagen: $e');
    }
  }
}
