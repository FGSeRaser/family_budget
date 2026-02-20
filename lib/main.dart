import 'dart:math';
import 'dart:async';
import 'dart:io';
import 'dart:convert';
import 'dart:typed_data';
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:flutter_secure_storage/flutter_secure_storage.dart';
import 'package:local_auth/local_auth.dart';
import 'package:fl_chart/fl_chart.dart';

import 'package:firebase_core/firebase_core.dart';
import 'package:firebase_auth/firebase_auth.dart';
import 'package:cloud_firestore/cloud_firestore.dart';

import 'package:shared_preferences/shared_preferences.dart';
import 'package:qr_flutter/qr_flutter.dart';
import 'package:mobile_scanner/mobile_scanner.dart' hide GeoPoint;
import 'package:path/path.dart' as p;
import 'package:path_provider/path_provider.dart';
import 'package:image_picker/image_picker.dart';
import 'package:share_plus/share_plus.dart';
import 'package:archive/archive_io.dart';
import 'package:file_picker/file_picker.dart';
import 'package:cryptography/cryptography.dart';

import 'firebase_options.dart';
import 'package:crypto/crypto.dart' as crypto;





String _fmtJoinedAt(dynamic joinedAt) {
  try {
    final dt = (joinedAt is Timestamp)
        ? joinedAt.toDate()
        : (joinedAt is DateTime ? joinedAt : null);
    if (dt == null) return '';
    String two(int v) => v.toString().padLeft(2, '0');
    return '${dt.year}-${two(dt.month)}-${two(dt.day)} ${two(dt.hour)}:${two(dt.minute)}';
  } catch (_) {
    return '';
  }
}

String _initials(String s) {
  final parts = s.trim().split(RegExp(r'\s+')).where((p) => p.isNotEmpty).toList();
  if (parts.isEmpty) return '?';
  final a = parts.first.characters.first.toUpperCase();
  final b = parts.length > 1 ? parts.last.characters.first.toUpperCase() : '';
  return (a + b).trim();
}


// ===== SharedPreferences keys =====
const String kPrefsFamilyId = 'familyId';
const String kPrefsMemberName = 'memberName';

// ===== Helpers (Dates, Month filter, Sync, etc.) =====
DateTime? tsToDate(dynamic v) {
  if (v == null) return null;
  if (v is Timestamp) return v.toDate();
  if (v is DateTime) return v;
  if (v is int) return DateTime.fromMillisecondsSinceEpoch(v);
  if (v is String) {
    final parsed = DateTime.tryParse(v);
    return parsed;
  }
  return null;
}


double _asDouble(dynamic v) {
  if (v == null) return 0.0;
  if (v is num) return v.toDouble();
  if (v is String) {
    final s = v.trim().replaceAll(',', '.');
    return double.tryParse(s) ?? 0.0;
  }
  return 0.0;
}

int _asInt(dynamic v, {int fallback = 1}) {
  if (v == null) return fallback;
  if (v is num) return v.toInt();
  if (v is String) {
    final s = v.trim();
    return int.tryParse(s) ?? fallback;
  }
  return fallback;
}


bool isSameMonth(DateTime a, DateTime b) => a.year == b.year && a.month == b.month;

String yyyymmdd(DateTime d) {
  final y = d.year.toString().padLeft(4, '0');
  final m = d.month.toString().padLeft(2, '0');
  final day = d.day.toString().padLeft(2, '0');
  return '$y$m$day';
}

bool isSameDay(DateTime a, DateTime b) => a.year == b.year && a.month == b.month && a.day == b.day;

DateTime addInterval(DateTime d, String unit, int interval) {
  switch (unit) {
    case 'weekly':
      return d.add(Duration(days: 7 * interval));
    case 'yearly':
      return DateTime(d.year + interval, d.month, d.day);
    case 'monthly':
    default:
      return DateTime(d.year, d.month + interval, d.day);
  }
}

// ===== Month helpers =====
DateTime monthStart(DateTime d) => DateTime(d.year, d.month, 1);
DateTime monthEndExclusive(DateTime d) => DateTime(d.year, d.month + 1, 1);


// --- Unterkonten (Accounts) ---
final Set<String> _ensuredDefaultAccountForFamily = <String>{};

Future<void> ensureDefaultAccountForFamily(DocumentReference<Map<String, dynamic>> famRef) async {
  final key = famRef.path;
  if (_ensuredDefaultAccountForFamily.contains(key)) return;
  _ensuredDefaultAccountForFamily.add(key);

  try {
    final accRef = famRef.collection('accounts').doc('default');
    final snap = await accRef.get();
    if (!snap.exists) {
      await accRef.set({
        'name': 'Haushalt',
        'nameLower': 'haushalt',
        'createdAt': Timestamp.fromDate(DateTime.now()),
        'updatedAt': Timestamp.fromDate(DateTime.now()),
        'archived': false,
      }, SetOptions(merge: true));
    } else {
      // ensure not archived accidentally
      final data = snap.data() ?? {};
      // ensure sorting helper fields
      if ((data['nameLower'] ?? '').toString().trim().isEmpty) {
        await accRef.set({'nameLower': 'haushalt'}, SetOptions(merge: true));
      }
      await accRef.set({'updatedAt': Timestamp.fromDate(DateTime.now())}, SetOptions(merge: true));
      if (data['archived'] == true) {
        await accRef.set({'archived': false}, SetOptions(merge: true));
      }
      if ((data['name'] ?? '').toString().trim().isEmpty) {
        await accRef.set({'name': 'Haushalt'}, SetOptions(merge: true));
      }
    }

    // Best-effort migration: assign default account to existing payments that lack accountId.
    // (Firestore can't query for missing fields; keep this lightweight.)
    final txSnap = await famRef.collection('tx').orderBy('createdAt', descending: true).limit(200).get();
    final batch = FirebaseFirestore.instance.batch();
    var changed = 0;
    for (final d in txSnap.docs) {
      final data = d.data();
      final has = data.containsKey('accountId') && (data['accountId'] ?? '').toString().trim().isNotEmpty;
      if (!has) {
        batch.set(d.reference, {'accountId': 'default'}, SetOptions(merge: true));
        changed++;
      }
    }
    if (changed > 0) {
      await batch.commit();
    }
  } catch (e) {
    debugPrint('ensureDefaultAccountForFamily error: $e');
  }
}


// ===============================
// Konten/Kategorien Helpers
// ===============================

Future<void> showCreateSimpleItemDialog({
  required BuildContext context,
  required String title,
  required String label,
  required Future<void> Function(String nameLower, String name) onSave,
}) async {
  final c = TextEditingController();
  String? err;
  bool saving = false;

  await showDialog<void>(
    context: context,
    barrierDismissible: false,
    builder: (ctx) => StatefulBuilder(
      builder: (ctx, setS) {
        void close() {
          if (Navigator.of(ctx).canPop()) {
            Navigator.of(ctx).pop();
          }
        }

        return AlertDialog(
          title: Text(title),
          content: SingleChildScrollView(
            child: Column(
              mainAxisSize: MainAxisSize.min,
            children: [
              if (err != null) ...[
                Text(err!, style: const TextStyle(color: Colors.red)),
                const SizedBox(height: 8),
              ],
              TextField(
                controller: c,
                decoration: InputDecoration(labelText: label),
                autofocus: true,
              ),
            ],
            ),
          ),
          actions: [
            TextButton(
              onPressed: saving ? null : close,
              child: const Text('Abbrechen'),
            ),
            FilledButton(
              onPressed: saving
                  ? null
                  : () async {
                      final name = c.text.trim();
                      if (name.isEmpty) {
                        setS(() => err = 'Bitte einen Namen eingeben.');
                        return;
                      }
                      setS(() {
                        err = null;
                        saving = true;
                      });

                      try {
                        await onSave(name.toLowerCase(), name);
                        close();
                      } catch (e) {
                        setS(() {
                          err = 'Speichern fehlgeschlagen: $e';
                          saving = false;
                        });
                      }
                    },
              child: const Text('Speichern'),
            ),
          ],
        );
      },
    ),
  );

  await Future.delayed(const Duration(milliseconds: 600));
  c.dispose();
}

Future<void> showManageCollectionDialog({
  required BuildContext context,
  required String title,
  required CollectionReference<Map<String, dynamic>> col,
  required CollectionReference<Map<String, dynamic>> txCol,
  required String txField,
  required String itemLabelSingular,
}) async {
  await showDialog<void>(
    context: context,
    builder: (ctx) {
      return AlertDialog(
        title: Text(title),
        content: SizedBox(
          width: 420,
          height: 420,
          child: StreamBuilder<QuerySnapshot<Map<String, dynamic>>>(
            stream: col.orderBy('nameLower').snapshots(),
            builder: (c, snap) {
              if (snap.hasError) {
                return Padding(
                  padding: const EdgeInsets.all(12),
                  child: Text('Fehler beim Laden: ${snap.error}', style: const TextStyle(color: Colors.red)),
                );
              }
              if (!snap.hasData) {
                return const Padding(
                  padding: EdgeInsets.all(12),
                  child: CircularProgressIndicator(),
                );
              }
              final docs = snap.data!.docs;

              return ListView.separated(
                shrinkWrap: true,
                itemCount: docs.length,
                separatorBuilder: (_, __) => const Divider(height: 1),
                itemBuilder: (_, i) {
                  final d = docs[i];
                  final data = d.data();
                  final name = (data['name'] ?? '').toString();
                  final archived = data['archived'] == true;

                  return ListTile(
                    title: Text(name.isEmpty ? '(ohne Name)' : name),
                    subtitle: archived ? const Text('Archiviert') : null,
                    trailing: Row(
                      mainAxisSize: MainAxisSize.min,
                      children: [
                        IconButton(
                          tooltip: 'Umbenennen',
                          icon: const Icon(Icons.edit),
                          onPressed: () async {
                            final ren = TextEditingController(text: name);
                            String? err;
                            bool saving = false;

                            await showDialog<void>(
                              context: ctx,
                              barrierDismissible: false,
                              builder: (ctx2) => StatefulBuilder(
                                builder: (ctx2, setS) {
                                  void close() {
                                    if (Navigator.of(ctx2).canPop()) {
                                      Navigator.of(ctx2).pop();
                                    }
                                  }

                                  return AlertDialog(
                                    title: Text('$itemLabelSingular umbenennen'),
                                    content: SingleChildScrollView(
                                      child: Column(
                                        mainAxisSize: MainAxisSize.min,
                                        children: [
                                          if (err != null) ...[
                                            Text(err!, style: const TextStyle(color: Colors.red)),
                                            const SizedBox(height: 8),
                                          ],
                                          TextField(
                                            controller: ren,
                                            autofocus: true,
                                            decoration: const InputDecoration(labelText: 'Name'),
                                          ),
                                        ],
                                      ),
                                    ),
                                    actions: [
                                      TextButton(
                                        onPressed: saving ? null : close,
                                        child: const Text('Abbrechen'),
                                      ),
                                      FilledButton(
                                        onPressed: saving
                                            ? null
                                            : () async {
                                                final newName = ren.text.trim();
                                                if (newName.isEmpty) {
                                                  setS(() => err = 'Bitte einen Namen eingeben.');
                                                  return;
                                                }
                                                setS(() {
                                                  err = null;
                                                  saving = true;
                                                });
                                                try {
                                                  await d.reference.set({
                                                    'name': newName,
                                                    'nameLower': newName.toLowerCase(),
                                                    'updatedAt': Timestamp.fromDate(DateTime.now()),
                                                  }, SetOptions(merge: true));
                                                  close();
                                                } catch (e) {
                                                  setS(() {
                                                    err = 'Fehler: $e';
                                                    saving = false;
                                                  });
                                                }
                                              },
                                        child: const Text('Speichern'),
                                      ),
                                    ],
                                  );
                                },
                              ),
                            );
                            await Future.delayed(const Duration(milliseconds: 600));
                            ren.dispose();
                          },
                        ),
                        IconButton(
                          tooltip: archived ? 'Reaktivieren' : 'Archivieren',
                          icon: Icon(archived ? Icons.unarchive : Icons.archive),
                          onPressed: () async {
                            await d.reference.set({
                              'archived': !archived,
                              'updatedAt': Timestamp.fromDate(DateTime.now()),
                            }, SetOptions(merge: true));
                          },
                        ),
                        IconButton(
                          tooltip: 'Löschen',
                          icon: const Icon(Icons.delete),
                          onPressed: () async {
                            final ok = await showDialog<bool>(
                              context: ctx,
                              builder: (c2) => AlertDialog(
                                title: const Text('Wirklich löschen?'),
                                content: Text('$itemLabelSingular „$name“ wirklich löschen?\n\nHinweis: Löschen geht nur, wenn keine Buchungen darauf verweisen.'),
                                actions: [
                                  TextButton(
                                    onPressed: () => Navigator.of(c2).pop(false),
                                    child: const Text('Abbrechen'),
                                  ),
                                  FilledButton(
                                    onPressed: () => Navigator.of(c2).pop(true),
                                    child: const Text('Löschen'),
                                  ),
                                ],
                              ),
                            );

                            if (ok != true) return;

                            // Guard: prüfen ob genutzt (Option A)
                            try {
                              final q = await txCol.where(txField, isEqualTo: d.id).limit(1).get();
                              if (q.docs.isNotEmpty) {
                                final sm = ScaffoldMessenger.maybeOf(context);
                                sm?.showSnackBar(
                                  SnackBar(
                                    content: Text('Kann nicht gelöscht werden: $itemLabelSingular wird noch in Buchungen verwendet. Bitte erst umstellen oder archivieren.'),
                                  ),
                                );
                                return;
                              }
                            } catch (e) {
                              final sm = ScaffoldMessenger.maybeOf(context);
                              sm?.showSnackBar(
                                SnackBar(content: Text('Prüfung fehlgeschlagen: $e')),
                              );
                              return;
                            }

                            try {
                              await d.reference.delete();
                            } catch (e) {
                              final sm = ScaffoldMessenger.maybeOf(context);
                              sm?.showSnackBar(
                                SnackBar(content: Text('Löschen fehlgeschlagen: $e')),
                              );
                            }
                          },
                        ),
                      ],
                    ),
                  );
                },
              );
            },
          ),
        ),
        actions: [
          FilledButton.icon(
            onPressed: () async {
              await showCreateSimpleItemDialog(
                context: ctx,
                title: 'Neues $itemLabelSingular',
                label: 'Name',
                onSave: (nameLower, name) async {
                  await col.doc().set({
                    'name': name,
                    'nameLower': nameLower,
                    'archived': false,
                    'createdAt': Timestamp.fromDate(DateTime.now()),
                    'updatedAt': Timestamp.fromDate(DateTime.now()),
                  }, SetOptions(merge: true));
                },
              );
            },
            icon: const Icon(Icons.add),
            label: const Text('Neu'),
          ),
          TextButton(
            onPressed: () => Navigator.of(ctx).pop(),
            child: const Text('Schließen'),
          ),
        ],
      );
    },
  );
}

String monthKey(DateTime d) => '${d.year}-${d.month.toString().padLeft(2, '0')}';
String monthLabel(DateTime d) {
  const names = ['Januar','Februar','März','April','Mai','Juni','Juli','August','September','Oktober','November','Dezember'];
  return '${names[d.month - 1]} ${d.year}';
}

/// Prüft (optional) Budget-Grenzen bevor eine Zahlung gespeichert wird.
/// Wenn Budgets nicht erzwungen werden, gibt die Funktion immer `true` zurück.
/// Wenn Budgets erzwungen werden und das Budget überschritten wird, fragt die Funktion nach Bestätigung.
Future<bool> checkBudgetBeforeAdd({
  required BuildContext context,
  required DocumentReference<Map<String, dynamic>> famRef,
  required String? category,
  required double amount,
  required bool isExpense,
  required DateTime selectedMonth,
}) async {
  if (!isExpense) return true; // Budgets nur für Ausgaben

  try {
    final famSnap = await famRef.get(const GetOptions(source: Source.serverAndCache));
    final fam = famSnap.data() ?? <String, dynamic>{};
    final enforce = (fam['enforceBudgets'] as bool?) ?? false;
    if (!enforce) return true;

    final monthlyBudget = (fam['monthlyBudget'] as num?)?.toDouble();
    final Map<String, dynamic> catBudgetsRaw = (fam['categoryBudgets'] as Map?)?.cast<String, dynamic>() ?? {};
    final double? catBudget = category != null ? (catBudgetsRaw[category] as num?)?.toDouble() : null;

    final mStart = DateTime(selectedMonth.year, selectedMonth.month, 1);
    final mEnd = DateTime(selectedMonth.year, selectedMonth.month + 1, 1);

    // Summe für Monat
    final q = await famRef
        .collection('tx')
        .where('type', isEqualTo: 'expense')
        .where('date', isGreaterThanOrEqualTo: Timestamp.fromDate(mStart))
        .where('date', isLessThan: Timestamp.fromDate(mEnd))
        .get(const GetOptions(source: Source.serverAndCache));

    double spentTotal = 0;
    double spentCat = 0;

    for (final d in q.docs) {
      final data = d.data();
      final v = (data['amount'] as num?)?.toDouble() ?? 0.0;
      spentTotal += v;
      if (category != null && (data['category']?.toString() ?? '') == category) {
        spentCat += v;
      }
    }

    final nextTotal = spentTotal + amount;
    final nextCat = spentCat + amount;

    String? warning;
    if (monthlyBudget != null && monthlyBudget > 0 && nextTotal > monthlyBudget) {
      warning = 'Monatsbudget überschritten: ${nextTotal.toStringAsFixed(2)} € / ${monthlyBudget.toStringAsFixed(2)} €';
    }
    if (warning == null && catBudget != null && catBudget > 0 && nextCat > catBudget) {
      warning = 'Kategorie-Budget überschritten: ${nextCat.toStringAsFixed(2)} € / ${catBudget.toStringAsFixed(2)} €';
    }

    if (warning == null) return true;

    final res = await showDialog<bool>(
      context: context,
      builder: (ctx) => AlertDialog(
        title: const Text('Budget-Warnung'),
        content: Text('$warning\n\nTrotzdem speichern?'),
        actions: [
          TextButton(onPressed: () => Navigator.pop(ctx, false), child: const Text('Abbrechen')),
          FilledButton(onPressed: () => Navigator.pop(ctx, true), child: const Text('Trotzdem speichern')),
        ],
      ),
    );

    return res == true;
  } catch (_) {
    // Im Zweifel nicht blockieren
    return true;
  }
}






/// Lokaler Beleg-/Anhang-Speicher (pro Gerät, NICHT via Firestore geteilt).
/// Speichert für jede Zahlung (paymentId) eine Liste lokaler Datei-Pfade.
class AttachmentStore {
  static const _prefsKey = 'attachment_store_v1';

  static Future<Map<String, List<String>>> _loadAll() async {
    final sp = await SharedPreferences.getInstance();
    final raw = sp.getString(_prefsKey);
    if (raw == null || raw.trim().isEmpty) return {};
    try {
      final decoded = jsonDecode(raw) as Map<String, dynamic>;
      return decoded.map((k, v) {
        final list = (v as List).map((e) => e.toString()).toList();
        return MapEntry(k, list);
      });
    } catch (_) {
      return {};
    }
  }

  static Future<void> _saveAll(Map<String, List<String>> map) async {
    final sp = await SharedPreferences.getInstance();
    await sp.setString(_prefsKey, jsonEncode(map));
  }

  static Future<List<String>> getForPayment(String paymentId) async {
    final all = await _loadAll();
    return all[paymentId] ?? <String>[];
  }

  static Future<void> addForPayment(String paymentId, String path) async {
    final all = await _loadAll();
    final list = List<String>.from(all[paymentId] ?? const <String>[]);
    if (!list.contains(path)) list.add(path);
    all[paymentId] = list;
    await _saveAll(all);
  }

  static Future<void> removeForPayment(String paymentId, String path) async {
    final all = await _loadAll();
    final list = List<String>.from(all[paymentId] ?? const <String>[]);
    list.removeWhere((e) => e == path);
    all[paymentId] = list;
    await _saveAll(all);
  }
}

/// Speichert ein Bild aus Kamera/Galerie als lokale Datei im App-Ordner und gibt den neuen Pfad zurück.
Future<String?> saveImageToAppDir(XFile? file, {required String prefix}) async {
  if (file == null) return null;
  final dir = await getApplicationDocumentsDirectory();
  final attDir = Directory(p.join(dir.path, 'attachments'));
  if (!await attDir.exists()) {
    await attDir.create(recursive: true);
  }
  final ext = p.extension(file.path);
  final name = '${prefix}_${DateTime.now().millisecondsSinceEpoch}$ext';
  final dest = p.join(attDir.path, name);
  await File(file.path).copy(dest);
  return dest;
}

/// Exportiert Zahlungen (Monat oder Jahr) als ZIP: CSV + alle lokal vorhandenen Belege.
/// - CSV enthält pro Zahlung die wichtigsten Felder + Anzahl Belege.
/// - Belege werden in /belege/<paymentId>/... abgelegt.
Future<void> exportPaymentsZip({
  required BuildContext context,
  required DocumentReference<Map<String, dynamic>> famRef,
  required DateTime fromInclusive,
  required DateTime toExclusive,
  required String filenameBase,
}) async {
  final txCol = famRef.collection('tx');

  final snap = await txCol
      .where('dueDate', isGreaterThanOrEqualTo: Timestamp.fromDate(fromInclusive))
      .where('dueDate', isLessThan: Timestamp.fromDate(toExclusive))
      .get();

  final tmpDir = await getTemporaryDirectory();
  final workDir = Directory(p.join(tmpDir.path, 'export_${DateTime.now().millisecondsSinceEpoch}'));
  await workDir.create(recursive: true);

  final csvFile = File(p.join(workDir.path, '$filenameBase.csv'));
  final sb = StringBuffer();
  // CSV Header (Semikolon, deutsch-kompatibel)
  sb.writeln('Datum;Titel;Kategorie;Typ;Status;Betrag;Notiz;Belege');

  for (final doc in snap.docs) {
    final data = doc.data();
    final due = tsToDate(data['dueDate']) ?? tsToDate(data['createdAt']) ?? DateTime.now();
    final title = (data['title'] ?? '').toString().replaceAll(';', ',');
    final cat = (data['category'] ?? 'Sonstiges').toString().replaceAll(';', ',');
    final type = (data['type'] ?? 'expense').toString();
    final status = (data['status'] ?? 'open').toString();
    final amount = ((data['amount'] as num?)?.toDouble() ?? 0.0);
    final note = (data['note'] ?? '').toString().replaceAll(';', ',').replaceAll('\n', ' ');

    // Lokale Belege: receiptPath (alt) + AttachmentStore
    final attachments = <String>[];
    final rp = data['receiptPath'] as String?;
    if (rp != null && rp.isNotEmpty && await File(rp).exists()) attachments.add(rp);
    final extra = await AttachmentStore.getForPayment(doc.id);
    for (final e in extra) {
      if (e.isNotEmpty && await File(e).exists()) attachments.add(e);
    }

    sb.writeln('${due.toIso8601String().substring(0,10)};$title;$cat;$type;$status;${amount.toStringAsFixed(2)};$note;${attachments.length}');

    // Belege kopieren
    if (attachments.isNotEmpty) {
      final perDir = Directory(p.join(workDir.path, 'belege', doc.id));
      await perDir.create(recursive: true);
      for (final ap in attachments) {
        final base = p.basename(ap);
        final dest = p.join(perDir.path, base);
        try {
          await File(ap).copy(dest);
        } catch (_) {}
      }
    }
  }

  await csvFile.writeAsString(sb.toString(), encoding: utf8);

  // ZIP bauen
  final zipPath = p.join(tmpDir.path, '$filenameBase.zip');
  final encoder = ZipFileEncoder();
  encoder.create(zipPath);
  encoder.addFile(csvFile);
  final belegeDir = Directory(p.join(workDir.path, 'belege'));
  if (await belegeDir.exists()) {
    encoder.addDirectory(belegeDir, includeDirName: true);
  }
  encoder.close();

  // Zuerst immer eine Kopie im App-Speicher ablegen (damit der Export nicht "verloren" geht)
  final appDir = await getApplicationDocumentsDirectory();
  final exportsDir = Directory(p.join(appDir.path, 'exports'));
  if (!await exportsDir.exists()) {
    await exportsDir.create(recursive: true);
  }
  final storedPath = p.join(exportsDir.path, '$filenameBase.zip');
  try {
    await File(zipPath).copy(storedPath);
  } catch (_) {}

  // Aktion wählen: Auf Gerät speichern oder Teilen
  if (context.mounted) {
    await showModalBottomSheet<void>(
      context: context,
      showDragHandle: true,
      builder: (ctx) {
        return SafeArea(
          child: Column(
            mainAxisSize: MainAxisSize.min,
            children: [
              const SizedBox(height: 6),
              ListTile(
                leading: const Icon(Icons.save_alt),
                title: const Text('Auf Gerät speichern'),
                subtitle: const Text('Ordner auswählen und ZIP dort ablegen'),
                onTap: () async {
                  Navigator.pop(ctx);
                  try {
                    final dirPath = await FilePicker.platform.getDirectoryPath(
                      dialogTitle: 'Ordner für Export auswählen',
                    );
                    if (dirPath == null) return;
                    var outPath = p.join(dirPath, '$filenameBase.zip');
                    if (await File(outPath).exists()) {
                      outPath = p.join(
                        dirPath,
                        '${filenameBase}_${DateTime.now().millisecondsSinceEpoch}.zip',
                      );
                    }
                    await File(zipPath).copy(outPath);
                    if (context.mounted) {
                      ScaffoldMessenger.of(context).showSnackBar(
                        SnackBar(content: Text('Export gespeichert: ${p.basename(outPath)}')),
                      );
                    }
                  } catch (e) {
                    if (context.mounted) {
                      ScaffoldMessenger.of(context).showSnackBar(
                        SnackBar(content: Text('Speichern fehlgeschlagen: $e')),
                      );
                    }
                  }
                },
              ),
              ListTile(
                leading: const Icon(Icons.share),
                title: const Text('Teilen'),
                subtitle: const Text('Über Share Sheet speichern/verschicken'),
                onTap: () async {
                  Navigator.pop(ctx);
                  await Share.shareXFiles([XFile(zipPath)], text: 'Export: $filenameBase');
                },
              ),
              ListTile(
                leading: const Icon(Icons.folder),
                title: const Text('Im App-Ordner gespeichert'),
                subtitle: Text('exports/${filenameBase}.zip'),
                onTap: () {
                  Navigator.pop(ctx);
                  if (context.mounted) {
                    ScaffoldMessenger.of(context).showSnackBar(
                      const SnackBar(content: Text('Hinweis: Die Datei liegt im App-Dokumente-Ordner unter /exports')),
                    );
                  }
                },
              ),
              const SizedBox(height: 8),
            ],
          ),
        );
      },
    );
  }

  // Cleanup best-effort
  try { await workDir.delete(recursive: true); } catch (_) {}
}
// ===== Encrypted Backup (Export + Import) =====

Map<String, dynamic> _encodeFirestore(dynamic v) {
  if (v == null) return {'__t': 'null'};
  if (v is Timestamp) return {'__t': 'ts', 'ms': v.millisecondsSinceEpoch};
  if (v is DateTime) return {'__t': 'dt', 'ms': v.millisecondsSinceEpoch};
  if (v is GeoPoint) return {'__t': 'geo', 'lat': v.latitude, 'lng': v.longitude};
  if (v is DocumentReference) return {'__t': 'ref', 'path': v.path};
  if (v is num || v is bool || v is String) return {'__t': 'p', 'v': v};
  if (v is List) return {'__t': 'list', 'v': v.map(_encodeFirestore).toList()};
  if (v is Map) {
    final out = <String, dynamic>{};
    v.forEach((k, val) => out[k.toString()] = _encodeFirestore(val));
    return {'__t': 'map', 'v': out};
  }
  return {'__t': 'str', 'v': v.toString()};
}

dynamic _decodeFirestore(dynamic wrapped, FirebaseFirestore db) {
  if (wrapped is! Map<String, dynamic>) return wrapped;
  final t = wrapped['__t'];
  switch (t) {
    case 'null':
        return;
    case 'ts':
      return Timestamp.fromMillisecondsSinceEpoch((wrapped['ms'] as num).toInt());
    case 'dt':
      return DateTime.fromMillisecondsSinceEpoch((wrapped['ms'] as num).toInt());
    case 'geo':
      return GeoPoint((wrapped['lat'] as num).toDouble(), (wrapped['lng'] as num).toDouble());
    case 'ref':
      return db.doc((wrapped['path'] as String));
    case 'p':
      return wrapped['v'];
    case 'str':
      return wrapped['v']?.toString();
    case 'list':
      return (wrapped['v'] as List).map((e) => _decodeFirestore(e, db)).toList();
    case 'map':
      final m = (wrapped['v'] as Map).map((k, v) => MapEntry(k.toString(), _decodeFirestore(v, db)));
      return m;
    default:
      return wrapped;
  }
}

Uint8List _randomBytes(int n) {
  final r = Random.secure();
  return Uint8List.fromList(List<int>.generate(n, (_) => r.nextInt(256)));
}

Future<SecretKey> _deriveKey(String password, Uint8List salt) async {
  final pbkdf2 = Pbkdf2(
    macAlgorithm: Hmac.sha256(),
    iterations: 150000,
    bits: 256,
  );
  return pbkdf2.deriveKey(
    secretKey: SecretKey(utf8.encode(password)),
    nonce: salt,
  );
}


Future<String?> _askText(BuildContext context, {required String title, required String hint, String okText = 'OK'}) async {
  final c = TextEditingController();
  return showDialog<String>(
    context: context,
    builder: (ctx) => AlertDialog(
      title: Text(title),
      content: TextField(
        controller: c,
        decoration: InputDecoration(hintText: hint),
        autofocus: true,
      ),
      actions: [
        TextButton(onPressed: () => Navigator.pop(ctx), child: const Text('Abbrechen')),
        FilledButton(onPressed: () => Navigator.pop(ctx, c.text.trim()), child: Text(okText)),
      ],
    ),
  );
}

Future<String?> _askPin(BuildContext context, {required String title}) async {
  final c = TextEditingController();
  return showDialog<String>(
    context: context,
    builder: (ctx) => AlertDialog(
      title: Text(title),
      content: TextField(
        controller: c,
        decoration: const InputDecoration(hintText: 'PIN'),
        keyboardType: TextInputType.number,
        obscureText: true,
        maxLength: 8,
        autofocus: true,
      ),
      actions: [
        TextButton(onPressed: () => Navigator.pop(ctx), child: const Text('Abbrechen')),
        FilledButton(onPressed: () => Navigator.pop(ctx, c.text.trim()), child: const Text('OK')),
      ],
    ),
  );
}

Future<String?> _askPassword(BuildContext context, {required String title, required String hint}) async {
  final c = TextEditingController();
  return showDialog<String>(
    context: context,
    builder: (ctx) => AlertDialog(
      title: Text(title),
      content: TextField(
        controller: c,
        obscureText: true,
        decoration: InputDecoration(hintText: hint),
      ),
      actions: [
        TextButton(onPressed: () => Navigator.pop(ctx), child: const Text('Abbrechen')),
        FilledButton(onPressed: () => Navigator.pop(ctx, c.text.trim()), child: const Text('OK')),
      ],
    ),
  );
}


// === Backup helpers (lokale Backup-Dateien finden/anzeigen) ===
Future<List<File>> _findLocalBackupFiles() async {
  final files = <File>[];

  // Internal app documents: <app>/backups
  try {
    final appDoc = await getApplicationDocumentsDirectory();
    final d = Directory(p.join(appDoc.path, 'backups'));
    if (await d.exists()) {
      await for (final e in d.list(followLinks: false)) {
        if (e is File && e.path.endsWith('.backup.json')) files.add(e);
      }
    }
  } catch (_) {}

  // External app-specific storage (Android): Android/data/<pkg>/files/FamilyBudget/backups
  try {
    final ext = await getExternalStorageDirectory();
    if (ext != null) {
      final d = Directory(p.join(ext.path, 'FamilyBudget', 'backups'));
      if (await d.exists()) {
        await for (final e in d.list(followLinks: false)) {
          if (e is File && e.path.endsWith('.backup.json')) files.add(e);
        }
      }
    }
  } catch (_) {}

  // newest first
  files.sort((a, b) => b.statSync().modified.compareTo(a.statSync().modified));
  return files;
}

Future<Uint8List?> _pickBackupBytesPreferLocal(BuildContext context) async {
  final local = await _findLocalBackupFiles();

  if (!context.mounted) return null;

  // Wenn wir lokale Backups haben, erst eine Liste anbieten (das ist "in dem Ordner wo das Backup ist").
  if (local.isNotEmpty) {
    final pickedPath = await showModalBottomSheet<String>(
      context: context,
      showDragHandle: true,
      builder: (ctx) {
        return SafeArea(
          child: Column(
            mainAxisSize: MainAxisSize.min,
            children: [
              const SizedBox(height: 8),
              const Text('Backup auswählen', style: TextStyle(fontSize: 16, fontWeight: FontWeight.w600)),
              const SizedBox(height: 8),
              Flexible(
                child: ListView.separated(
                  shrinkWrap: true,
                  itemCount: local.length + 1,
                  separatorBuilder: (_, __) => const Divider(height: 1),
                  itemBuilder: (c, i) {
                    if (i == 0) {
                      return ListTile(
                        leading: const Icon(Icons.folder_open),
                        title: const Text('Datei auswählen…'),
                        subtitle: const Text('Öffnet den System-Dateiauswahldialog'),
                        onTap: () => Navigator.pop(c, '__PICKER__'),
                      );
                    }
                    final f = local[i - 1];
                    final name = p.basename(f.path);
                    final mod = f.statSync().modified;
                    return ListTile(
                      leading: const Icon(Icons.lock),
                      title: Text(name),
                      subtitle: Text('${mod.toLocal()}'),
                      onTap: () => Navigator.pop(c, f.path),
                    );
                  },
                ),
              ),
              const SizedBox(height: 8),
            ],
          ),
        );
      },
    );

    if (pickedPath == null) return null;
    if (pickedPath != '__PICKER__') {
      try {
        return await File(pickedPath).readAsBytes();
      } catch (_) {
        return null;
      }
    }
    // else: fall through to picker
  }

  final picked = await FilePicker.platform.pickFiles(
    type: FileType.custom,
    allowedExtensions: const ['json'],
    withData: true,
  );
  if (picked == null || picked.files.isEmpty) return null;

  Uint8List? bytes = picked.files.first.bytes;
  final path = picked.files.first.path;
  if (bytes == null && path != null) {
    try {
      bytes = await File(path).readAsBytes();
    } catch (_) {}
  }
  return bytes;
}


Future<void> exportEncryptedBackup({
  required BuildContext context,
  required DocumentReference<Map<String, dynamic>> famRef,
}) async {
  final db = FirebaseFirestore.instance;

  // Collect data
  final famSnap = await famRef.get(const GetOptions(source: Source.serverAndCache));
  final famData = famSnap.data() ?? {};

  Future<List<Map<String, dynamic>>> dumpCol(String name) async {
    final snap = await famRef.collection(name).get();
    return snap.docs
        .map((d) => {'id': d.id, 'data': _encodeFirestore(d.data())})
        .toList();
  }

  final tx = await dumpCol('tx');
  final budgets = await dumpCol('budgets');
  final categories = await dumpCol('categories');
  final rules = await dumpCol('recurring_rules');
  final accounts = await dumpCol('accounts');
  final members = await dumpCol('members');


  final payload = {
    'schema': 2,
    'createdAt': DateTime.now().toIso8601String(),
    'familyPath': famRef.path,
    'family': _encodeFirestore(famData),
    'tx': tx,
    'budgets': budgets,
    'categories': categories,
    'recurring_rules': rules,
    'accounts': accounts,
    'members': members,
  };

  final password = await _askPassword(context, title: 'Backup verschlüsseln', hint: 'Passwort eingeben');
  if (password == null || password.isEmpty) return;

  final salt = _randomBytes(16);
  final nonce = _randomBytes(12);
  final key = await _deriveKey(password, salt);

  final algo = AesGcm.with256bits();
  final secretBox = await algo.encrypt(
    utf8.encode(jsonEncode(payload)),
    secretKey: key,
    nonce: nonce,
  );

  final out = {
    'v': 1,
    'salt': base64Encode(salt),
    'nonce': base64Encode(nonce),
    'cipher': base64Encode(secretBox.cipherText),
    'mac': base64Encode(secretBox.mac.bytes),
  };

  final dir = await getTemporaryDirectory();
  final file = File(p.join(dir.path, 'familybudget_backup_${DateTime.now().millisecondsSinceEpoch}.backup.json'));
  await file.writeAsString(jsonEncode(out));

  // Zusätzlich immer lokal speichern (intern + extern), damit du es später wiederfinden kannst.
  String? internalSavedPath;
  String? externalSavedPath;

  // Internal app-docs (nicht unbedingt im Dateimanager sichtbar, aber für die App immer vorhanden)
  try {
    final appDoc = await getApplicationDocumentsDirectory();
    final backupsDir = Directory(p.join(appDoc.path, 'backups'));
    if (!await backupsDir.exists()) {
      await backupsDir.create(recursive: true);
    }
    internalSavedPath = p.join(backupsDir.path, p.basename(file.path));
    await file.copy(internalSavedPath);
  } catch (_) {
    // ignore
  }

  // External app-specific (Android): Android/data/<pkg>/files/FamilyBudget/backups
  try {
    final ext = await getExternalStorageDirectory();
    if (ext != null) {
      final backupsDir = Directory(p.join(ext.path, 'FamilyBudget', 'backups'));
      if (!await backupsDir.exists()) {
        await backupsDir.create(recursive: true);
      }
      externalSavedPath = p.join(backupsDir.path, p.basename(file.path));
      await file.copy(externalSavedPath);
    }
  } catch (_) {
    // ignore
  }

  if (context.mounted) {
    final msg = (externalSavedPath != null)
        ? 'Backup gespeichert. Import findet es automatisch wieder.'
        : 'Backup gespeichert (intern). Import findet es automatisch wieder.';
    ScaffoldMessenger.of(context).showSnackBar(SnackBar(content: Text(msg)));
  }



  
// Optional: direkt teilen (empfohlen auf GrapheneOS, weil "Ordner auswählen" die App in den Hintergrund schickt).
  if (!context.mounted) return;

  final shareNow = await showModalBottomSheet<bool>(
    context: context,
    showDragHandle: true,
    builder: (ctx) => SafeArea(
      child: Column(
        mainAxisSize: MainAxisSize.min,
        children: [
          ListTile(
            leading: const Icon(Icons.share),
            title: const Text('Backup teilen / speichern'),
            subtitle: const Text('Über „Teilen“ kannst du es in Dateien/Downloads ablegen.'),
            onTap: () => Navigator.pop(ctx, true),
          ),
          ListTile(
            leading: const Icon(Icons.check),
            title: const Text('Fertig'),
            onTap: () => Navigator.pop(ctx, false),
          ),
          const SizedBox(height: 6),
        ],
      ),
    ),
  );

  if (shareNow == true) {
    final pathToShare = internalSavedPath ?? file.path;
    await Share.shareXFiles([XFile(pathToShare)], text: 'FamilyBudget Backup (verschlüsselt)');
  }
}


Future<String?> importEncryptedBackup({
  required BuildContext context,
  DocumentReference<Map<String, dynamic>>? targetFamRef,
}) async {
  final db = FirebaseFirestore.instance;

  final bytes = await _pickBackupBytesPreferLocal(context);
  if (bytes == null) return null;

  final password = await _askPassword(context, title: 'Backup Passwort', hint: 'Passwort eingeben');
  if (password == null || password.isEmpty) return null;

  try {
    final raw = jsonDecode(utf8.decode(bytes));
    if (raw is! Map<String, dynamic>) return null;

    final salt = base64Decode(raw['salt'] as String);
    final nonce = base64Decode(raw['nonce'] as String);
    final cipher = base64Decode(raw['cipher'] as String);
    final mac = base64Decode(raw['mac'] as String);

    final key = await _deriveKey(password, salt);
    final algo = AesGcm.with256bits();

    final clear = await algo.decrypt(
      SecretBox(cipher, nonce: nonce, mac: Mac(mac)),
      secretKey: key,
    );

    final payload = jsonDecode(utf8.decode(clear));
    if (payload is! Map<String, dynamic>) return null;

    final fp = (payload['familyPath'] as String? ?? '').trim();

    // Prefer familyPath from backup; only fallback to passed ref if missing.
    final famRef = fp.isNotEmpty
        ? db.doc(fp) as DocumentReference<Map<String, dynamic>>
        : targetFamRef;

    if (famRef == null) return null;

    // Overwrite Family document
    // Achtung: Import überschreibt Daten in dieser Familie.
    final ok = await showDialog<bool>(
      context: context,
      builder: (ctx) => AlertDialog(
        title: const Text('Backup einspielen?'),
        content: const Text(
          'Das spielt das Backup in diese Familie ein und überschreibt vorhandene Daten (Zahlungen, Budgets, Kategorien, Konten, Mitglieder, Regeln).\n\nTipp: Exportiere vorher ein Backup, falls du zurück willst.',
        ),
        actions: [
          TextButton(onPressed: () => Navigator.pop(ctx, false), child: const Text('Abbrechen')),
          FilledButton(onPressed: () => Navigator.pop(ctx, true), child: const Text('Einspielen')),
        ],
      ),
    );
    if (ok != true) return null;

    await famRef.set(_decodeFirestore(payload['family'], db) as Map<String, dynamic>, SetOptions(merge: false));

    Future<void> syncCol(String name, List docs) async {
      final col = famRef.collection(name);
      final existing = await col.get();
      for (final d in existing.docs) {
        await d.reference.delete();
      }
      for (final d in docs) {
        if (d is! Map) continue;
        final id = (d['id'] ?? '').toString();
        if (id.isEmpty) continue;
        await col.doc(id).set(_decodeFirestore(d['data'], db) as Map<String, dynamic>);
      }
    }

    await syncCol('tx', (payload['tx'] as List? ?? const []));
    await syncCol('budgets', (payload['budgets'] as List? ?? const []));
    await syncCol('categories', (payload['categories'] as List? ?? const []));
    await syncCol('recurring_rules', (payload['recurring_rules'] as List? ?? const []));
    await syncCol('accounts', (payload['accounts'] as List? ?? const []));
    await syncCol('members', (payload['members'] as List? ?? const []));

    // Switch app to imported family
    final prefs = await SharedPreferences.getInstance();
    await prefs.setString('familyId', famRef.id);

    // Notify BootGate (so UI switches immediately, no restart needed)
    try {
    } catch (_) {}

    return famRef.id;
  } catch (e) {
    debugPrint('Import Fehler: $e');
    return null;
  }
}

/// Erzeugt Instanzen aus wiederkehrenden Regeln bis zu einem Horizont (default: +90 Tage).
/// Idempotent: nutzt deterministische Payment-IDs (rr_<ruleId>_<yyyyMMdd>), damit keine Duplikate entstehen.
Future<void> ensureRecurringGeneratedForFamily({
  required DocumentReference<Map<String, dynamic>> famRef,
  Duration horizon = const Duration(days: 90),
}) async {
  final rulesCol = famRef.collection('recurring_rules');
  final txCol = famRef.collection('tx');

  final rulesSnap = await rulesCol.where('active', isEqualTo: true).get();

  final now = DateTime.now();
  final maxDate = now.add(horizon);

  for (final rDoc in rulesSnap.docs) {
    final r = rDoc.data();

    DateTime next = tsToDate(r['nextDueDate']) ?? tsToDate(r['startDate']) ?? now;
    final unit = (r['unit'] as String?) ?? ((r['interval'] is String) ? (r['interval'] as String) : null) ?? 'monthly';
    final interval = (r['interval'] is num) ? (r['interval'] as num).toInt() : ((r['every'] is num) ? (r['every'] as num).toInt() : (int.tryParse((r['every'] ?? '').toString()) ?? 1));
    final endDate = tsToDate(r['endDate']);
    int? remaining = (r['remainingCount'] as num?)?.toInt();

    // Sicherheitsbremse (falls Daten kaputt): max 200 Iterationen pro Lauf
    int guard = 0;

    while ((next.isBefore(maxDate) || isSameDay(next, maxDate)) && guard < 200) {
      guard++;

      if (endDate != null && next.isAfter(endDate)) break;
      if (remaining != null && remaining <= 0) break;

      final pid = 'rr_${rDoc.id}_${yyyymmdd(next)}';

      // set(..., merge:false) ist okay; falls schon existiert -> wir ignorieren den Fehler
      try {
        await txCol.doc(pid).set({
          'title': r['title'] ?? 'Wiederkehrend',
          'amount': (r['amount'] as num?)?.toDouble() ?? 0.0,
          'type': (r['type'] as String?) ?? 'expense', // expense|income
          'status': ((r['type'] as String?) ?? 'expense') == 'income' ? 'paid' : 'open',
          'category': r['category'] ?? 'Sonstiges',
          'dueDate': Timestamp.fromDate(next),
          'date': Timestamp.fromDate(next),
          'createdAt': FieldValue.serverTimestamp(),
          'sourceRecurringRuleId': rDoc.id,
          'note': r['note'],
        }, SetOptions(merge: false));
      } catch (_) {
        // bereits vorhanden -> ok
      }

      // Nächste Fälligkeit
      next = addInterval(next, unit, interval);
      if (remaining != null) remaining -= 1;

      await rulesCol.doc(rDoc.id).update({
        'nextDueDate': Timestamp.fromDate(next),
        if (r.containsKey('remainingCount')) 'remainingCount': remaining,
        if (remaining != null && remaining <= 0) 'active': false,
      });
    }
  }
}

String ymKey(DateTime d) => '${d.year}-${d.month.toString().padLeft(2, '0')}';

(String, int) _presetToUnitInterval(String preset, String customUnit, int customInterval) {
  switch (preset) {
    case 'quarterly':
      return ('monthly', 3);
    case 'semiannual':
      return ('monthly', 6);
    case 'yearly':
      return ('yearly', 1);
    case '4weekly':
      return ('weekly', 4);
    case 'custom':
      final unit = (customUnit == 'weekly' || customUnit == 'monthly' || customUnit == 'yearly') ? customUnit : 'monthly';
      final interval = customInterval < 1 ? 1 : customInterval;
      return (unit, interval);
    case 'monthly':
    default:
      return ('monthly', 1);
  }
}

String _unitIntervalLabel(String unit, int interval) {
  if (unit == 'weekly' && interval == 4) return 'alle 4 Wochen';
  if (unit == 'weekly') return 'alle $interval Woche(n)';
  if (unit == 'yearly') return interval == 1 ? 'jährlich' : 'alle $interval Jahre';
  // monthly
  if (interval == 1) return 'monatlich';
  if (interval == 3) return 'vierteljährlich';
  if (interval == 6) return 'halbjährlich';
  return 'alle $interval Monate';
}



Future<void> togglePaid(DocumentSnapshot<Map<String, dynamic>> doc) async {
  final data = doc.data() ?? {};
  final type = data['type'] as String? ?? 'expense';
  if (type == 'income') return;

  final status = data['status'] as String? ?? 'open';
  final newStatus = status == 'paid' ? 'open' : 'paid';
  await doc.reference.update({'status': newStatus});
}

final ImagePicker _picker = ImagePicker();

Future<String?> takeReceiptPhotoAndSaveLocal() async {
  final XFile? file = await _picker.pickImage(source: ImageSource.camera, imageQuality: 85);
  if (file == null) return null;

  final dir = await getApplicationDocumentsDirectory();
  final filename = 'receipt_${DateTime.now().millisecondsSinceEpoch}${p.extension(file.path)}';
  final savedPath = p.join(dir.path, filename);
  await File(file.path).copy(savedPath);
  return savedPath;
}


Future<void> openTxDetails(BuildContext context, DocumentSnapshot<Map<String, dynamic>> doc) async {
  final data = doc.data() ?? {};
  final title0 = (data['title'] ?? '').toString();
  final amount0 = (data['amount'] as num?)?.toDouble() ?? 0.0;
  final paid0 = (data['paid'] as bool?) ?? false;
  final type0 = (data['type'] ?? 'expense').toString();
  final category0 = (data['category'] ?? 'Sonstiges').toString();
  final due0 = tsToDate(data['dueDate']) ?? DateTime.now();
  final note0 = (data['note'] ?? '').toString();
  final receipt0 = (data['receiptPath'] ?? '').toString().trim();
  final proof0 = (data['proofPath'] ?? '').toString().trim();
  final recurringRuleId = (data['recurringRuleId'] ?? '').toString().trim();
  final accountId0 = (data['accountId'] ?? 'default').toString().trim().isEmpty ? 'default' : (data['accountId'] ?? 'default').toString().trim();
  final famRef = doc.reference.parent.parent as DocumentReference<Map<String, dynamic>>?;

  Future<String?> _pickAndSave(ImageSource src, String prefix) async {
    try {
      final picker = ImagePicker();
      final x = await picker.pickImage(source: src, imageQuality: 85);
      if (x == null) return null;

      final dir = await getApplicationDocumentsDirectory();
      final fileName = '${prefix}_${DateTime.now().millisecondsSinceEpoch}${p.extension(x.path)}';
      final dest = File(p.join(dir.path, fileName));
      await dest.writeAsBytes(await x.readAsBytes());
      return dest.path;
    } catch (e) {
      debugPrint('PickImage error: $e');
      return null;
    }
  }

  Future<void> _editTx(BuildContext parentCtx) async {
  final titleC = TextEditingController(text: title0);
  final amountC = TextEditingController(text: amount0.toStringAsFixed(2));
  final noteC = TextEditingController(text: note0);
  String type = type0;
  String category = category0;
  DateTime due = DateTime(due0.year, due0.month, due0.day);
  String? receiptPath = receipt0.isEmpty ? null : receipt0;
  String? proofPath = proof0.isEmpty ? null : proof0;

  String accountId = accountId0;

  bool applyToFuture = recurringRuleId.isNotEmpty;
  DateTime effectiveFrom = DateTime(due.year, due.month, due.day);

  bool saving = false;
  String? errorMsg;

  final editOk = await showDialog<bool>(
    context: parentCtx,
    barrierDismissible: false,
    builder: (ctx) => StatefulBuilder(
      builder: (ctx, setS) {
        void close([bool? result]) {
            if (!ctx.mounted) return;
            Navigator.of(ctx).pop(result);
          }

        Future<void> pickReceipt(ImageSource src) async {
          final p = await _pickAndSave(src, 'invoice');
          if (p != null) setS(() => receiptPath = p);
        }

        Future<void> pickProof(ImageSource src) async {
          final p = await _pickAndSave(src, 'proof');
          if (p != null) setS(() => proofPath = p);
        }

        return AlertDialog(
          title: const Text('Zahlung bearbeiten'),
          content: SingleChildScrollView(
            child: Column(
              mainAxisSize: MainAxisSize.min,
              children: [
                if (errorMsg != null) ...[
                  Text(errorMsg!, style: const TextStyle(color: Colors.red)),
                  const SizedBox(height: 8),
                ],

                // Unterkonto
                StreamBuilder<QuerySnapshot<Map<String, dynamic>>>(
                  stream: famRef!.collection('accounts').orderBy('name').snapshots(),
                  builder: (c, snap) {
                    final items = <MapEntry<String, String>>[
                      const MapEntry('default', 'Haushalt'),
                    ];
                    if (snap.hasData) {
                      for (final d in snap.data!.docs) {
                        final data = d.data();
                        if (data['archived'] == true) continue;
                        final name = (data['name'] ?? '').toString().trim();
                        if (name.isEmpty) continue;
                        if (d.id == 'default') {
                          items[0] = MapEntry('default', name);
                        } else {
                          items.add(MapEntry(d.id, name));
                        }
                      }
                    }
                    final ids = items.map((e) => e.key).toSet();
                    final effectiveAccountId = ids.contains(accountId) ? accountId : 'default';

                    return DropdownButtonFormField<String>(
                      value: effectiveAccountId,
                      decoration: const InputDecoration(labelText: 'Konto'),
                      items: items.map((e) => DropdownMenuItem(value: e.key, child: Text(e.value))).toList(),
                      onChanged: saving ? null : (v) => setS(() => accountId = v ?? 'default'),
                    );
                  },
                ),
                const SizedBox(height: 8),

                TextField(controller: titleC, decoration: const InputDecoration(labelText: 'Titel')),
                const SizedBox(height: 8),
                TextField(
                  controller: amountC,
                  decoration: const InputDecoration(labelText: 'Betrag (€)'),
                  keyboardType: const TextInputType.numberWithOptions(decimal: true),
                ),
                const SizedBox(height: 8),
                DropdownButtonFormField<String>(
                  value: type,
                  decoration: const InputDecoration(labelText: 'Typ'),
                  items: const [
                    DropdownMenuItem(value: 'expense', child: Text('Ausgabe')),
                    DropdownMenuItem(value: 'income', child: Text('Einnahme')),
                  ],
                  onChanged: saving ? null : (v) => setS(() => type = v ?? 'expense'),
                ),
                const SizedBox(height: 8),

                // Kategorie aus Firestore
                StreamBuilder<QuerySnapshot<Map<String, dynamic>>>(
                  stream: famRef!
                      .collection('categories')
                      .orderBy('nameLower')
                      .snapshots(),
                  builder: (c, snap) {
                    final items = <String>{'Sonstiges'};
                    if (snap.hasError) {
                      debugPrint('Kategorie-Stream Fehler: ${snap.error}');
                    }
                    if (snap.hasData) {
                      for (final d in snap.data!.docs) {
                        final data = d.data();
                        if (data['archived'] == true) continue;
                        final n = (data['name'] ?? '').toString().trim();
                        if (n.isNotEmpty) items.add(n);
                      }
                    }
                    final list = items.toList()..sort();
                    if (!list.contains(category)) category = list.first;

                    return DropdownButtonFormField<String>(
                      value: category,
                      decoration: const InputDecoration(labelText: 'Kategorie'),
                      items: list.map((e) => DropdownMenuItem(value: e, child: Text(e))).toList(),
                      onChanged: saving ? null : (v) => setS(() => category = v ?? 'Sonstiges'),
                    );
                  },
                ),

                const SizedBox(height: 8),
                TextField(
                  controller: noteC,
                  decoration: const InputDecoration(labelText: 'Notiz (optional)'),
                  minLines: 1,
                  maxLines: 4,
                ),
                const SizedBox(height: 8),

                ListTile(
                  contentPadding: EdgeInsets.zero,
                  title: const Text('Fällig am'),
                  subtitle: Text('${due.day.toString().padLeft(2,'0')}.${due.month.toString().padLeft(2,'0')}.${due.year}'),
                  trailing: const Icon(Icons.calendar_month),
                  onTap: saving
                      ? null
                      : () async {
                          final d = await showDatePicker(
                            context: ctx,
                            firstDate: DateTime(2020),
                            lastDate: DateTime(2100),
                            initialDate: due,
                          );
                          if (d != null) setS(() => due = d);
                        },
                ),

                if (recurringRuleId.isNotEmpty) ...[
                  const Divider(),
                  SwitchListTile(
                    contentPadding: EdgeInsets.zero,
                    title: const Text('Auch zukünftige Instanzen ändern'),
                    subtitle: const Text('Ändert die Wiederholungs-Regel ab einem Datum'),
                    value: applyToFuture,
                    onChanged: saving ? null : (v) => setS(() => applyToFuture = v),
                  ),
                  if (applyToFuture)
                    ListTile(
                      contentPadding: EdgeInsets.zero,
                      title: const Text('Gültig ab'),
                      subtitle: Text('${effectiveFrom.day.toString().padLeft(2,'0')}.${effectiveFrom.month.toString().padLeft(2,'0')}.${effectiveFrom.year}'),
                      trailing: const Icon(Icons.event),
                      onTap: saving
                          ? null
                          : () async {
                              final d = await showDatePicker(
                                context: ctx,
                                firstDate: DateTime(2020),
                                lastDate: DateTime(2100),
                                initialDate: effectiveFrom,
                              );
                              if (d != null) setS(() => effectiveFrom = d);
                            },
                    ),
                ],

                const Divider(),
                Row(
                  children: [
                    Expanded(
                      child: OutlinedButton.icon(
                        onPressed: saving ? null : () => pickReceipt(ImageSource.camera),
                        icon: const Icon(Icons.photo_camera),
                        label: const Text('Rechnung Kamera'),
                      ),
                    ),
                    const SizedBox(width: 8),
                    Expanded(
                      child: OutlinedButton.icon(
                        onPressed: saving ? null : () => pickReceipt(ImageSource.gallery),
                        icon: const Icon(Icons.photo_library),
                        label: const Text('Rechnung Galerie'),
                      ),
                    ),
                  ],
                ),
                const SizedBox(height: 8),
                Row(
                  children: [
                    Expanded(
                      child: OutlinedButton.icon(
                        onPressed: saving ? null : () => pickProof(ImageSource.camera),
                        icon: const Icon(Icons.receipt_long),
                        label: const Text('Beleg Kamera'),
                      ),
                    ),
                    const SizedBox(width: 8),
                    Expanded(
                      child: OutlinedButton.icon(
                        onPressed: saving ? null : () => pickProof(ImageSource.gallery),
                        icon: const Icon(Icons.collections),
                        label: const Text('Beleg Galerie'),
                      ),
                    ),
                  ],
                ),
              ],
            ),
            ),
          actions: [
            TextButton(
              onPressed: saving ? null : () => close(false),
              child: const Text('Abbrechen'),
            ),
            FilledButton(
              onPressed: saving
                  ? null
                  : () async {
                      final title = titleC.text.trim();
                      final amount = double.tryParse(amountC.text.replaceAll(',', '.')) ?? 0.0;

                      if (title.isEmpty || amount <= 0) {
                        setS(() => errorMsg = 'Bitte Titel und Betrag eingeben.');
                        return;
                      }
                      setS(() {
                        errorMsg = null;
                        saving = true;
                      });

                      try {
                        final now = DateTime.now();
                        final dueNorm = DateTime(due.year, due.month, due.day);

                        // Update current tx
                        await doc.reference.set({
                          'accountId': accountId,
                          'title': title,
                          'amount': amount,
                          'type': type,
                          'category': category,
                          'dueDate': Timestamp.fromDate(dueNorm),
                          'note': noteC.text.trim(),
                          if (receiptPath != null) 'receiptPath': receiptPath,
                          if (proofPath != null) 'proofPath': proofPath,
                          'updatedAt': Timestamp.fromDate(now),
                        }, SetOptions(merge: true));

                        // Optionally update recurring rule for future
                        if (applyToFuture && recurringRuleId.isNotEmpty) {
                          await famRef!.collection('recurring_rules').doc(recurringRuleId).set({
                            'accountId': accountId,
                            'title': title,
                            'amount': amount,
                            'type': type,
                            'category': category,
                            'note': noteC.text.trim(),
                            'updatedAt': Timestamp.fromDate(now),
                            'effectiveFrom': Timestamp.fromDate(DateTime(effectiveFrom.year, effectiveFrom.month, effectiveFrom.day)),
                          }, SetOptions(merge: true));
                        }

                        close(true);
                      } catch (e) {
                        debugPrint('Edit payment error: $e');
                        setS(() {
                          errorMsg = 'Speichern fehlgeschlagen: $e';
                          saving = false;
                        });
                      }
                    },
              child: const Text('Speichern'),
            ),
          ],
        );
      },
    ),
  );

  titleC.dispose();
  amountC.dispose();
  noteC.dispose();

  if (editOk == true && parentCtx.mounted) {
    ScaffoldMessenger.of(parentCtx)
      ..hideCurrentSnackBar()
      ..showSnackBar(const SnackBar(content: Text('Erfolgreich bearbeitet.')));
  }
}

  await showModalBottomSheet<void>(
    context: context,
    isScrollControlled: true,
    showDragHandle: true,
    builder: (ctx) => SafeArea(
      child: Padding(
        padding: EdgeInsets.only(bottom: MediaQuery.of(ctx).viewInsets.bottom),
        child: StatefulBuilder(
          builder: (ctx, setS) => FutureBuilder<List<String>>(
            future: AttachmentStore.getForPayment(doc.id),
            builder: (ctx, snap) {
              final attachments = snap.data ?? const <String>[];
              final receiptFile = receipt0.isNotEmpty ? File(receipt0) : null;
              final proofFile = proof0.isNotEmpty ? File(proof0) : null;

              return ListView(
                shrinkWrap: true,
                padding: const EdgeInsets.all(16),
                children: [
                  Row(
                    children: [
                      Expanded(
                        child: Text(title0, style: const TextStyle(fontSize: 18, fontWeight: FontWeight.w700)),
                      ),
                      IconButton(
                        tooltip: 'Bearbeiten',
                        icon: const Icon(Icons.edit),
                        onPressed: () => _editTx(ctx),
                      ),
                      IconButton(
                        tooltip: 'Löschen',
                        icon: const Icon(Icons.delete, color: Colors.red),
                        onPressed: () async {
                          final ok = await showDialog<bool>(
                            context: ctx,
                            builder: (c) => AlertDialog(
                              title: const Text('Wirklich löschen?'),
                              content: const Text('Diese Zahlung wird dauerhaft gelöscht.'),
                              actions: [
                                TextButton(onPressed: () => Navigator.pop(c, false), child: const Text('Abbrechen')),
                                FilledButton(onPressed: () => Navigator.pop(c, true), child: const Text('Löschen')),
                              ],
                            ),
                          );
                          if (ok == true) {
                            await doc.reference.delete();
                            if (ctx.mounted) Navigator.pop(ctx);
                            if (context.mounted) ScaffoldMessenger.of(context).showSnackBar(const SnackBar(content: Text('Gelöscht.')));
                          }
                        },
                      ),
                    ],
                  ),
                  const SizedBox(height: 8),
                  Text('${type0 == 'income' ? 'Einnahme' : 'Ausgabe'} • ${category0}'),
                  const SizedBox(height: 4),
                  Text('Fällig: ${due0.day.toString().padLeft(2,'0')}.${due0.month.toString().padLeft(2,'0')}.${due0.year}'),
                  const SizedBox(height: 8),
                  Text('${paid0 ? '✅ Bezahlt' : '⏳ Offen'}  •  ${amount0.toStringAsFixed(2)} €',
                      style: const TextStyle(fontWeight: FontWeight.w600)),
                  if (note0.trim().isNotEmpty) ...[
                    const SizedBox(height: 12),
                    const Text('Notiz', style: TextStyle(fontWeight: FontWeight.w700)),
                    const SizedBox(height: 6),
                    Text(note0),
                  ],
                  const SizedBox(height: 12),
                  Row(
                    children: [
                      Expanded(
                        child: FilledButton.icon(
                          onPressed: () async {
                            await doc.reference.set({'paid': !paid0, 'updatedAt': Timestamp.fromDate(DateTime.now())}, SetOptions(merge: true));
                            if (ctx.mounted) Navigator.pop(ctx);
                          },
                          icon: Icon(paid0 ? Icons.undo : Icons.check),
                          label: Text(paid0 ? 'Als offen' : 'Als bezahlt'),
                        ),
                      ),
                    ],
                  ),
                  const SizedBox(height: 12),

                  const Text('Rechnung', style: TextStyle(fontWeight: FontWeight.w700)),
                  const SizedBox(height: 6),
                  if (receiptFile != null && receiptFile.existsSync())
                    ClipRRect(
                      borderRadius: BorderRadius.circular(12),
                      child: Image.file(receiptFile, height: 180, fit: BoxFit.cover),
                    )
                  else
                    const Text('Keine Rechnung gespeichert.'),
                  const SizedBox(height: 8),
                  Row(
                    children: [
                      OutlinedButton.icon(
                        onPressed: () async {
                          final pth = await _pickAndSave(ImageSource.camera, 'invoice');
                          if (pth != null) {
                            await doc.reference.set({'receiptPath': pth}, SetOptions(merge: true));
                            if (ctx.mounted) Navigator.pop(ctx);
                            if (context.mounted) ScaffoldMessenger.of(context).showSnackBar(const SnackBar(content: Text('Rechnung gespeichert.')));
                          }
                        },
                        icon: const Icon(Icons.photo_camera),
                        label: const Text('Kamera'),
                      ),
                      const SizedBox(width: 8),
                      OutlinedButton.icon(
                        onPressed: () async {
                          final pth = await _pickAndSave(ImageSource.gallery, 'invoice');
                          if (pth != null) {
                            await doc.reference.set({'receiptPath': pth}, SetOptions(merge: true));
                            if (ctx.mounted) Navigator.pop(ctx);
                            if (context.mounted) ScaffoldMessenger.of(context).showSnackBar(const SnackBar(content: Text('Rechnung gespeichert.')));
                          }
                        },
                        icon: const Icon(Icons.photo_library),
                        label: const Text('Galerie'),
                      ),
                    ],
                  ),

                  const SizedBox(height: 16),
                  const Text('Zahlungsbeleg', style: TextStyle(fontWeight: FontWeight.w700)),
                  const SizedBox(height: 6),
                  if (proofFile != null && proofFile.existsSync())
                    ClipRRect(
                      borderRadius: BorderRadius.circular(12),
                      child: Image.file(proofFile, height: 180, fit: BoxFit.cover),
                    )
                  else
                    const Text('Kein Zahlungsbeleg gespeichert.'),
                  const SizedBox(height: 8),
                  Row(
                    children: [
                      OutlinedButton.icon(
                        onPressed: () async {
                          final pth = await _pickAndSave(ImageSource.camera, 'proof');
                          if (pth != null) {
                            await doc.reference.set({'proofPath': pth}, SetOptions(merge: true));
                            if (ctx.mounted) Navigator.pop(ctx);
                            if (context.mounted) ScaffoldMessenger.of(context).showSnackBar(const SnackBar(content: Text('Beleg gespeichert.')));
                          }
                        },
                        icon: const Icon(Icons.receipt_long),
                        label: const Text('Kamera'),
                      ),
                      const SizedBox(width: 8),
                      OutlinedButton.icon(
                        onPressed: () async {
                          final pth = await _pickAndSave(ImageSource.gallery, 'proof');
                          if (pth != null) {
                            await doc.reference.set({'proofPath': pth}, SetOptions(merge: true));
                            if (ctx.mounted) Navigator.pop(ctx);
                            if (context.mounted) ScaffoldMessenger.of(context).showSnackBar(const SnackBar(content: Text('Beleg gespeichert.')));
                          }
                        },
                        icon: const Icon(Icons.collections),
                        label: const Text('Galerie'),
                      ),
                    ],
                  ),

                  const SizedBox(height: 16),
                  const Text('Weitere Belege', style: TextStyle(fontWeight: FontWeight.w700)),
                  const SizedBox(height: 6),
                  if (attachments.isEmpty)
                    const Text('Keine weiteren Belege gespeichert.')
                  else
                    Wrap(
                      spacing: 8,
                      runSpacing: 8,
                      children: [
                        for (final a in attachments)
                          ClipRRect(
                            borderRadius: BorderRadius.circular(12),
                            child: Image.file(File(a), width: 96, height: 96, fit: BoxFit.cover),
                          )
                      ],
                    ),
                  const SizedBox(height: 8),
                  Row(
                    children: [
                      OutlinedButton.icon(
                        onPressed: () async {
                          final pth = await _pickAndSave(ImageSource.camera, 'att');
                          if (pth != null) {
                            await AttachmentStore.addForPayment(doc.id, pth);
                            setS(() {});
                          }
                        },
                        icon: const Icon(Icons.add_a_photo),
                        label: const Text('Kamera'),
                      ),
                      const SizedBox(width: 8),
                      OutlinedButton.icon(
                        onPressed: () async {
                          final pth = await _pickAndSave(ImageSource.gallery, 'att');
                          if (pth != null) {
                            await AttachmentStore.addForPayment(doc.id, pth);
                            setS(() {});
                          }
                        },
                        icon: const Icon(Icons.photo_library),
                        label: const Text('Galerie'),
                      ),
                    ],
                  ),
                ],
              );
            },
          ),
        ),
      ),
    ),
  );
}
class AppLockService {
  static final AppLockService instance = AppLockService();

  static const _enabledKey = 'app_lock_enabled';
  static const _preferBioKey = 'app_lock_prefer_bio';
  static const _pinHashKey = 'app_lock_pin_hash_v1';

  final LocalAuthentication _auth = LocalAuthentication();
  final FlutterSecureStorage _secure = const FlutterSecureStorage();

  Future<bool> isEnabled() async {
    final sp = await SharedPreferences.getInstance();
    return sp.getBool(_enabledKey) ?? false;
  }

  Future<void> setEnabled(bool v) async {
    final sp = await SharedPreferences.getInstance();
    await sp.setBool(_enabledKey, v);
  }

  Future<bool> preferBiometrics() async {
    final sp = await SharedPreferences.getInstance();
    return sp.getBool(_preferBioKey) ?? true;
  }

  Future<void> setPreferBiometrics(bool v) async {
    final sp = await SharedPreferences.getInstance();
    await sp.setBool(_preferBioKey, v);
  }

  Future<bool> hasPin() async {
    final stored = await _secure.read(key: _pinHashKey);
    return stored != null && stored.isNotEmpty;
  }

  Future<void> setPin(String pin) async {
    // Use package:crypto for a stable hash, store in secure storage.
    final hash = crypto.sha256.convert(utf8.encode(pin)).toString();
    await _secure.write(key: _pinHashKey, value: hash);
  }

  Future<void> clearPin() async {
    await _secure.delete(key: _pinHashKey);
  }

  Future<bool> verifyPin(String pin) async {
    final stored = await _secure.read(key: _pinHashKey);
    if (stored == null || stored.isEmpty) return false;
    final hash = crypto.sha256.convert(utf8.encode(pin)).toString();
    return hash == stored;
  }

  Future<bool> canUseBiometrics() async {
    try {
      final supported = await _auth.isDeviceSupported();
      final canCheck = await _auth.canCheckBiometrics;
      return supported && canCheck;
    } catch (_) {
      return false;
    }
  }

  Future<bool> authenticateBiometric() async {
    try {
      if (!await canUseBiometrics()) return false;
      return await _auth.authenticate(
        localizedReason: 'Bitte identifizieren Sie sich',
      );
} catch (_) {
      return false;
    }
  }

  // --- Backwards compatible method names used elsewhere in the file ---
  Future<bool> isLockEnabled() => isEnabled();
  Future<bool> isBiometricsPreferred() => preferBiometrics();
  Future<void> setLockEnabled(bool v) => setEnabled(v);
  Future<void> setBiometricsPreferred(bool v) => setPreferBiometrics(v);
}


class AppLockGate extends StatefulWidget {
  final Widget child;
  const AppLockGate({super.key, required this.child});

  @override
  State<AppLockGate> createState() => _AppLockGateState();
}

class _AppLockGateState extends State<AppLockGate> with WidgetsBindingObserver {
  bool _checked = false;
  bool _unlocked = false;
  bool _needsPin = false;
  String? _error;
  bool _authInProgress = false;

  @override
  void initState() {
    super.initState();
    WidgetsBinding.instance.addObserver(this);
    _checkAndLock();
  }

  @override
  void dispose() {
    WidgetsBinding.instance.removeObserver(this);
    super.dispose();
  }

  @override
  void didChangeAppLifecycleState(AppLifecycleState state) {
    // WICHTIG: 'inactive' kommt beim Biometrie-Dialog -> NICHT sperren,
    // sonst gibt's einen Loop. Wir sperren nur wenn wirklich im Hintergrund.
    if (state == AppLifecycleState.paused || state == AppLifecycleState.detached) {
      if (context.mounted) {
        setState(() {
          _unlocked = false;
          _checked = false;
          _needsPin = false;
          _error = null;
        });
      }
      return;
    }

    if (state == AppLifecycleState.resumed) {
      // Nach dem Resumen kurz warten, damit UI sauber aufgebaut ist.
      WidgetsBinding.instance.addPostFrameCallback((_) => _checkAndLock());
    }
  }

  Future<void> _checkAndLock() async {
    if (_authInProgress) return;
    _authInProgress = true;

// If already unlocked, don't re-run auth on resume.
if (_unlocked) {
  _authInProgress = false;
  return;
}

    try {
      final svc = AppLockService.instance;

      if (!(await svc.isEnabled())) {
        if (context.mounted) {
          setState(() {
            _checked = true;
            _unlocked = true;
            _needsPin = false;
            _error = null;
          });
        }
        return;
      }

      if (context.mounted) {
        setState(() {
          _checked = true;
          _unlocked = false;
          _error = null;
        });
      }

      // 1) Biometrie bevorzugt
      final preferBio = await svc.preferBiometrics();
      if (preferBio && await svc.canUseBiometrics()) {
        final ok = await svc.authenticateBiometric();
        if (ok) {
          if (context.mounted) {
            setState(() {
              _unlocked = true;
              _needsPin = false;
              _error = null;
            });
          }
          return;
        }
        // Wenn Biometrie fehlschlägt, fallen wir auf PIN zurück (falls gesetzt)
      }

      // 2) PIN Fallback
      final hasPin = await svc.hasPin();
      if (context.mounted) {
        setState(() {
          _needsPin = hasPin;
          _error = hasPin ? null : 'Kein App-PIN gesetzt. Bitte in Einstellungen einen PIN festlegen.';
        });
      }
    } finally {
      _authInProgress = false;
    }
  }

  Future<void> _promptPin() async {
    final ok = await showDialog<bool>(
      context: context,
      barrierDismissible: false,
      builder: (ctx) => _PinDialog(
        title: 'Entsperren',
        onVerify: (pin) => AppLockService.instance.verifyPin(pin),
      ),
    );

    if (!context.mounted) return;

    setState(() {
      _unlocked = ok == true;
      _error = (ok == true) ? null : 'PIN falsch. Bitte erneut versuchen.';
    });
  }

  @override
  Widget build(BuildContext context) {
    if (!_checked) {
      return const Scaffold(body: Center(child: CircularProgressIndicator()));
    }
    if (_unlocked) return widget.child;

    return Scaffold(
      body: Center(
        child: ConstrainedBox(
          constraints: const BoxConstraints(maxWidth: 420),
          child: Padding(
            padding: const EdgeInsets.all(24),
            child: Column(
              mainAxisAlignment: MainAxisAlignment.center,
              children: [
                const Icon(Icons.lock, size: 64),
                const SizedBox(height: 16),
                Text(
                  'App gesperrt',
                  style: Theme.of(context).textTheme.titleLarge,
                  textAlign: TextAlign.center,
                ),
                const SizedBox(height: 8),
                if (_error != null) ...[
                  Text(_error!, textAlign: TextAlign.center),
                  const SizedBox(height: 12),
                ],
                Row(
                  mainAxisAlignment: MainAxisAlignment.center,
                  children: [
                    FilledButton(
                      onPressed: _checkAndLock,
                      child: const Text('Biometrie'),
                    ),
                    const SizedBox(width: 12),
                    FilledButton.tonal(
                      onPressed: _needsPin ? _promptPin : null,
                      child: const Text('PIN'),
                    ),
                  ],
                ),
                const SizedBox(height: 12),
                TextButton(
                  onPressed: _checkAndLock,
                  child: const Text('Erneut versuchen'),
                ),
              ],
            ),
          ),
        ),
      ),
    );
  }
}

class _PinDialog extends StatefulWidget {
  final String title;
  final Future<bool> Function(String) onVerify;
  const _PinDialog({required this.title, required this.onVerify});

  @override
  State<_PinDialog> createState() => _PinDialogState();
}

class _PinDialogState extends State<_PinDialog> {
  final _c = TextEditingController();
  String? _err;

  @override
  void dispose() {
    _c.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return AlertDialog(
      title: Text(widget.title),
      content: TextField(
        controller: _c,
        obscureText: true,
        keyboardType: TextInputType.number,
        decoration: InputDecoration(
          labelText: 'PIN',
          errorText: _err,
        ),
      ),
      actions: [
        TextButton(onPressed: () => Navigator.pop(context, false), child: const Text('Abbrechen')),
        FilledButton(
          onPressed: () async {
            final ok = await widget.onVerify(_c.text.trim());
            if (!context.mounted) return;
            if (ok) {
              Navigator.pop(context, true);
            } else {
              setState(() => _err = 'Falscher PIN');
            }
          },
          child: const Text('OK'),
        ),
      ],
    );
  }
}

class _CreatePinDialog extends StatefulWidget {
  final String title;
  const _CreatePinDialog({this.title = 'PIN setzen'});

  @override
  State<_CreatePinDialog> createState() => _CreatePinDialogState();
}

class _CreatePinDialogState extends State<_CreatePinDialog> {
  final _c1 = TextEditingController();
  final _c2 = TextEditingController();
  String? _err;
  bool _busy = false;

  @override
  void dispose() {
    _c1.dispose();
    _c2.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return AlertDialog(
      title: Text(widget.title),
      content: Column(
        mainAxisSize: MainAxisSize.min,
        children: [
          TextField(
            controller: _c1,
            obscureText: true,
            keyboardType: TextInputType.number,
            decoration: const InputDecoration(labelText: 'Neuer PIN'),
          ),
          const SizedBox(height: 12),
          TextField(
            controller: _c2,
            obscureText: true,
            keyboardType: TextInputType.number,
            decoration: InputDecoration(labelText: 'PIN wiederholen', errorText: _err),
          ),
        ],
      ),
      actions: [
        TextButton(
          onPressed: _busy ? null : () => Navigator.pop(context),
          child: const Text('Abbrechen'),
        ),
        FilledButton(
          onPressed: _busy
              ? null
              : () async {
                  setState(() {
                    _busy = true;
                    _err = null;
                  });
            final p1 = _c1.text.trim();
            final p2 = _c2.text.trim();
            if (p1.length < 4) {
              setState(() {
                _err = 'Mindestens 4 Ziffern';
                _busy = false;
              });
              return;
            }
            if (p1 != p2) {
              setState(() {
                _err = 'PIN stimmt nicht überein';
                _busy = false;
              });
              return;
            }
            try {
              await AppLockService.instance.setPin(p1);
            } catch (e) {
              if (!context.mounted) return;
              setState(() {
                _err = 'Speichern fehlgeschlagen';
                _busy = false;
              });
              return;
            }
            if (!context.mounted) return;
            // Navigator kann bei schnellen Doppeltaps "locked" sein – pop sicher im nächsten Microtask.
            Future.microtask(() {
              if (!context.mounted) return;
              if (Navigator.of(context).canPop()) {
                Navigator.of(context).pop(p1);
              }
            });
          },
          child: const Text('Speichern'),
        ),
      ],
    );
  }
}

enum AppTheme { light, sand, ocean, teal, sakura, dark, neonBlue, obsidian, amethyst, crimson }

class ThemeStore {
  // New single-theme key (no separate accent anymore)
  static const _kTheme = 'ui_theme'; // light|dark|sand|ocean|teal|neonBlue

  // Legacy keys (kept for migration from older versions)
  static const _kMode = 'ui_theme_mode'; // system|light|dark
  static const _kAccent = 'ui_theme_accent'; // sand|ocean|midnight|teal

  static Future<void> setTheme(AppTheme theme) async {
    final p = await SharedPreferences.getInstance();
    await p.setString(_kTheme, theme.name);
  }

  static Future<AppTheme> load() async {
    final p = await SharedPreferences.getInstance();

    // New format
    final s = p.getString(_kTheme);
    if (s != null && s.isNotEmpty) {
      return AppTheme.values.firstWhere((e) => e.name == s, orElse: () => AppTheme.ocean);
    }

    // Legacy migration (mode + accent)
    final modeS = p.getString(_kMode) ?? 'system';
    final accS = p.getString(_kAccent) ?? 'ocean';

    AppTheme out = AppTheme.ocean;

    if (modeS == 'dark') {
      // Old "midnight" became our neon-blue dark option; otherwise just dark.
      out = (accS == 'midnight') ? AppTheme.neonBlue : AppTheme.dark;
    } else if (modeS == 'light') {
      out = switch (accS) {
        'sand' => AppTheme.sand,
        'teal' => AppTheme.teal,
        'ocean' => AppTheme.ocean,
        'midnight' => AppTheme.dark,
        _ => AppTheme.light,
      };
    } else {
      // system -> choose ocean light as a safe default
      out = switch (accS) {
        'sand' => AppTheme.sand,
        'teal' => AppTheme.teal,
        'midnight' => AppTheme.neonBlue,
        _ => AppTheme.ocean,
      };
    }

    // Persist new key so we don't need legacy keys anymore.
    await p.setString(_kTheme, out.name);
    return out;
  }
}

final ValueNotifier<AppTheme> themeNotifier = ValueNotifier<AppTheme>(AppTheme.ocean);

ThemeData _buildTheme(AppTheme theme) {
  late final Brightness brightness;
  late final Color seed;
  Color? scaffoldBg;
  Color? surface;

  switch (theme) {
    // ===== Light themes (5) =====
    case AppTheme.light:
      brightness = Brightness.light;
      seed = const Color(0xFF1E88E5); // kräftiges Blau
      break;
    case AppTheme.sand:
      brightness = Brightness.light;
      seed = const Color(0xFFFFA000); // richtig warm (orange/gelb)
      scaffoldBg = const Color(0xFFFFF3D6); // sandig hell
      break;
    case AppTheme.ocean:
      brightness = Brightness.light;
      seed = const Color(0xFF0077B6); // schönes Meer-Blau
      scaffoldBg = const Color(0xFFE6F6FF); // very light ocean tint
      break;
    case AppTheme.teal:
      brightness = Brightness.light;
      seed = const Color(0xFF2A9D8F); // grünlich weich
      scaffoldBg = const Color(0xFFE9FFF7);
      break;
    case AppTheme.sakura:
      brightness = Brightness.light;
      seed = const Color(0xFFE91E63); // kräftiges Pink (Sakura)
      scaffoldBg = const Color(0xFFFFF0F6); // sehr helles Rosa
      break;

    // ===== Dark themes (5) =====
    case AppTheme.dark:
      brightness = Brightness.dark;
      seed = const Color(0xFF4FC3F7); // weiches Blau
      break;
    case AppTheme.neonBlue:
      brightness = Brightness.dark;
      seed = const Color(0xFF00A3FF); // neon blue
      scaffoldBg = const Color(0xFF050814); // richtig dunkel
      surface = const Color(0xFF0B1020);
      break;
    case AppTheme.obsidian:
      brightness = Brightness.dark;
      seed = const Color(0xFFFFC107); // Gold-Akzent auf Obsidian
      scaffoldBg = const Color(0xFF070707);
      surface = const Color(0xFF101010);
      break;
    case AppTheme.amethyst:
      brightness = Brightness.dark;
      seed = const Color(0xFF9C27B0); // Amethyst/Violett
      scaffoldBg = const Color(0xFF070311);
      surface = const Color(0xFF120A22);
      break;
    case AppTheme.crimson:
      brightness = Brightness.dark;
      seed = const Color(0xFFE53935); // Crimson/Rot
      scaffoldBg = const Color(0xFF120607);
      surface = const Color(0xFF1B0A0C);
      break;
  }

  final baseScheme = ColorScheme.fromSeed(seedColor: seed, brightness: brightness);
  final scheme = baseScheme.copyWith(
    surface: surface ?? baseScheme.surface,
  );

  // Inputs etwas besser lesbar, besonders in Neon/Dark
  final fill = scheme.surfaceContainerHighest.withOpacity(brightness == Brightness.dark ? 0.35 : 1.0);

  return ThemeData(
    useMaterial3: true,
    colorScheme: scheme,
    scaffoldBackgroundColor: scaffoldBg ?? scheme.surface,
    appBarTheme: AppBarTheme(backgroundColor: scheme.surface, foregroundColor: scheme.onSurface),
    snackBarTheme: SnackBarThemeData(
      backgroundColor: scheme.inverseSurface,
      contentTextStyle: TextStyle(color: scheme.onInverseSurface),
    ),
    inputDecorationTheme: InputDecorationTheme(
      filled: true,
      fillColor: fill,
      border: const OutlineInputBorder(),
    ),
  );
}


Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();
  await Firebase.initializeApp(options: DefaultFirebaseOptions.currentPlatform);

  // Unsichtbar einloggen (keine Emails, kein Login-Stress)
  await _ensureAnonAuth();

  runApp(const FamilyBudgetApp());
}

Future<void> _ensureAnonAuth() async {
  final auth = FirebaseAuth.instance;
  if (auth.currentUser == null) {
    await auth.signInAnonymously();
  }
}

class FamilyBudgetApp extends StatefulWidget {
  const FamilyBudgetApp({super.key});
  @override
  State<FamilyBudgetApp> createState() => _FamilyBudgetAppState();
}

class _FamilyBudgetAppState extends State<FamilyBudgetApp> {
  @override
  void initState() {
    super.initState();
    ThemeStore.load().then((v) {
      themeNotifier.value = v;
    });
  }

  @override
  Widget build(BuildContext context) {
    return ValueListenableBuilder<AppTheme>(
      valueListenable: themeNotifier,
      builder: (context, theme, _) {
        final t = _buildTheme(theme);
        return MaterialApp(
          title: 'FamilyBudget',
          debugShowCheckedModeBanner: false,
          // Wir liefern ein vollständiges Theme (inkl. Helligkeit) direkt aus.
          themeMode: ThemeMode.light,
          theme: t,
          darkTheme: t,
          home: const AppLockGate(child: BootGate()),
        );
      },
    );
  }
}


/// BootGate entscheidet:
/// - wenn noch keine Family verbunden -> Join/Create Screen
/// - sonst -> Home
class BootGate extends StatefulWidget {
  const BootGate({super.key});

  @override
  State<BootGate> createState() => _BootGateState();
}

class _BootGateState extends State<BootGate> {
  String? familyId;
  String? memberName;
  bool loading = true;

  @override
  void initState() {
    super.initState();
    _load();
  }

  Future<void> _load() async {
    final prefs = await SharedPreferences.getInstance();
    familyId = prefs.getString(kPrefsFamilyId);
    memberName = prefs.getString(kPrefsMemberName);
    setState(() => loading = false);
  }

  Future<void> _setFamily(String id, {String? name}) async {
    final prefs = await SharedPreferences.getInstance();
    await prefs.setString(kPrefsFamilyId, id);
    if (name != null && name.trim().isNotEmpty) {
      await prefs.setString(kPrefsMemberName, name.trim());
      memberName = name.trim();
    }
    setState(() => familyId = id);
  }

  Future<void> _leaveFamily() async {
    final prefs = await SharedPreferences.getInstance();
    await prefs.remove(kPrefsFamilyId);
    setState(() => familyId = null);
  }

  @override
  Widget build(BuildContext context) {
    if (loading) {
      return const Scaffold(body: Center(child: CircularProgressIndicator()));
    }
    if (familyId == null) {
      return FamilyJoinScreen(
        onJoined: (id, name) => _setFamily(id, name: name),
      );
    }
    return HomeScreen(
      familyId: familyId!,
      memberName: memberName,
      onLeaveFamily: _leaveFamily,
    );
  }
}

/// =======================
///  FAMILY JOIN / CREATE
/// =======================
class FamilyJoinScreen extends StatefulWidget {
  const FamilyJoinScreen({super.key, required this.onJoined});

  final void Function(String familyId, String memberName) onJoined;

  @override
  State<FamilyJoinScreen> createState() => _FamilyJoinScreenState();
}

class _FamilyJoinScreenState extends State<FamilyJoinScreen> {
  final nameC = TextEditingController();
  final codeC = TextEditingController();
  bool busy = false;

  final db = FirebaseFirestore.instance;
  final auth = FirebaseAuth.instance;

  @override
  void dispose() {
    nameC.dispose();
    codeC.dispose();
    super.dispose();
  }

  String _genCode8() {
    const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // keine 0/O/1/I
    final r = Random.secure();
    return List.generate(8, (_) => chars[r.nextInt(chars.length)]).join();
  }


  Future<void> _showFamilyCreatedDialog(String code) async {
    if (!mounted) return;
    await showDialog(
      context: context,
      builder: (ctx) => AlertDialog(
        title: const Text('Familie erstellt'),
        content: Column(
          mainAxisSize: MainAxisSize.min,
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            const Text('Dein Familien-Code:'),
            const SizedBox(height: 8),
            SelectableText(
              code,
              style: const TextStyle(fontSize: 22, fontWeight: FontWeight.w700, letterSpacing: 2),
            ),
            const SizedBox(height: 12),
            QrImageView(
              data: code,
              size: 180,
            ),
            const SizedBox(height: 8),
            const Text(
              'Teile den Code mit der Person, die beitreten möchte. Danach musst du die Anfrage bestätigen.',
              style: TextStyle(color: Colors.black54),
            ),
          ],
        ),
        actions: [
          TextButton(
            onPressed: () async {
              await Clipboard.setData(ClipboardData(text: code));
              if (ctx.mounted) {
                ScaffoldMessenger.of(ctx).showSnackBar(const SnackBar(content: Text('Code kopiert')));
              }
            },
            child: const Text('Kopieren'),
          ),
          TextButton(
            onPressed: () => Navigator.of(ctx).pop(),
            child: const Text('OK'),
          ),
        ],
      ),
    );
  }

  Future<void> _createFamily() async {
    final name = nameC.text.trim();
    if (name.isEmpty) return _toast('Bitte deinen Namen eingeben.');

    setState(() => busy = true);
    try {
      final uid = auth.currentUser!.uid;
      final code = _genCode8();

      final famRef = db.collection('families').doc(); // random id
      final batch = db.batch();

      batch.set(famRef, {
        'code': code,
        'createdAt': FieldValue.serverTimestamp(),
        'createdBy': uid,
      });

      batch.set(famRef.collection('members').doc(uid), {
        'uid': uid,
        'name': name,
        'role': 'owner',
        'joinedAt': FieldValue.serverTimestamp(),
      });

      batch.set(db.collection('familyCodes').doc(code), {
        'familyId': famRef.id,
        'createdAt': FieldValue.serverTimestamp(),
        'createdBy': uid,
      });

      await batch.commit();

      widget.onJoined(famRef.id, name);
      await _showFamilyCreatedDialog(code);
    } catch (e) {
      _toast('Fehler: $e');
    } finally {
      if (context.mounted) setState(() => busy = false);
    }
  }

  Future<void> _joinByCode(String code) async {
    final name = nameC.text.trim();
    if (name.isEmpty) return _toast('Bitte deinen Namen eingeben.');
    code = code.trim().toUpperCase();
    if (code.length != 8) return _toast('Code muss 8 Zeichen haben.');

    setState(() => busy = true);
    try {
      final uid = auth.currentUser!.uid;

      final codeSnap = await db.collection('familyCodes').doc(code).get();
      if (!codeSnap.exists) {
        _toast('Code nicht gefunden.');
        return;
      }

      final famId = (codeSnap.data()!['familyId'] as String);
      final famRef = db.collection('families').doc(famId);

      await famRef.collection('joinRequests').doc(uid).set({
        'uid': uid,
        'name': name,
        'status': 'pending',
        'createdAt': FieldValue.serverTimestamp(),
      }, SetOptions(merge: true));

      if (!context.mounted) return;

      Navigator.of(context).push(
        MaterialPageRoute(
          builder: (_) => WaitingApprovalScreen(
            familyId: famId,
            memberName: name,
            onApproved: () => widget.onJoined(famId, name),
          ),
        ),
      );

      _toast('Anfrage gesendet – warte auf Bestätigung.');
    } catch (e) {
      _toast('Fehler: $e');
    } finally {
      if (context.mounted) setState(() => busy = false);
    }
  }


  Future<void> _joinByInviteToken(String token) async {
    final name = nameC.text.trim();
    if (name.isEmpty) return _toast('Bitte deinen Namen eingeben.');
    token = token.trim();
    if (token.length < 16) return _toast('Token ungültig.');

    setState(() => busy = true);
    try {
      final uid = auth.currentUser!.uid;
      final tokenRef = db.collection('inviteTokens').doc(token);

      // Read once so we can navigate with the familyId (transaction will re-check).
      final pre = await tokenRef.get();
      if (!pre.exists) {
        _toast('Token ungültig oder bereits benutzt.');
        return;
      }
      final preData = pre.data() as Map<String, dynamic>;
      final familyId = preData['familyId'] as String?;
      if (familyId == null) {
        _toast('Token defekt.');
        return;
      }

      await db.runTransaction((tx) async {
        final snap = await tx.get(tokenRef);
        if (!snap.exists) throw Exception('Token ungültig oder bereits benutzt.');

        final data = snap.data() as Map<String, dynamic>;
        final famId = data['familyId'] as String?;
        final expiresAt = tsToDate(data['expiresAt']);
        if (famId == null || expiresAt == null) throw Exception('Token defekt.');
        if (famId != familyId) throw Exception('Token defekt.');
        if (expiresAt.isBefore(DateTime.now())) {
          tx.delete(tokenRef);
          throw Exception('Token abgelaufen.');
        }

        final famRef = db.collection('families').doc(familyId);

        tx.set(
          famRef.collection('members').doc(uid),
          {
            'name': name,
            'role': 'member',
            'joinedAt': FieldValue.serverTimestamp(),
            'inviteToken': token,
          },
          SetOptions(merge: true),
        );

        // Single-use
        tx.delete(tokenRef);
      });

      widget.onJoined(familyId, name);
    } catch (e) {
      _toast('Fehler: $e');
    } finally {
      if (context.mounted) setState(() => busy = false);
    }
  }


  Future<void> _openScanner() async {
    final scanned = await Navigator.push<String?>(
      context,
      MaterialPageRoute(builder: (_) => const QrScanScreen()),
    );
    if (scanned == null) return;

    // QR kann entweder 8-stelligen Familien-Code ODER Invite-Token enthalten
    final raw = scanned.trim();
    if (raw.length == 8) {
      final code = raw.toUpperCase();
      codeC.text = code;
      await _joinByCode(code);
    } else {
      // Token (länger)
      codeC.text = raw;
      await _joinByInviteToken(raw);
    }
  }

  void _toast(String msg) {
    if (!context.mounted) return;
    ScaffoldMessenger.of(context).showSnackBar(SnackBar(content: Text(msg)));
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: const Text('Familie verbinden')),
      // Auf kleinen Displays kann die Column sonst "overflow" (gelb/schwarz) verursachen.
      body: SafeArea(
        child: SingleChildScrollView(
          padding: const EdgeInsets.all(16),
          child: Column(
            children: [
            TextField(
              controller: nameC,
              decoration: const InputDecoration(
                labelText: 'Dein Name',
                border: OutlineInputBorder(),
              ),
            ),
            const SizedBox(height: 12),
            // JOIN per Code
            TextField(
              controller: codeC,
              textCapitalization: TextCapitalization.characters,
              decoration: const InputDecoration(
                labelText: 'Familien-Code (8 Zeichen) oder Invite-Token',
                border: OutlineInputBorder(),
              ),
            ),
            const SizedBox(height: 10),
            Row(
              children: [
                Expanded(
                  child: ElevatedButton.icon(
                    onPressed: busy ? null : () => _joinByCode(codeC.text),
                    icon: const Icon(Icons.key),
                    label: const Text('Mit Code beitreten'),
                  ),
                ),
                const SizedBox(width: 10),
                ElevatedButton.icon(
                  onPressed: busy ? null : _openScanner,
                  icon: const Icon(Icons.qr_code_scanner),
                  label: const Text('QR'),
                ),
              ],
            ),
            const SizedBox(height: 12),
            SizedBox(
              width: double.infinity,
              child: OutlinedButton.icon(
                onPressed: busy ? null : () => _joinByInviteToken(codeC.text),
                icon: const Icon(Icons.lock),
                label: const Text('Mit sicherem Invite-Token beitreten'),
              ),
            ),
            const SizedBox(height: 18),
            const Divider(),
            const SizedBox(height: 10),
            // CREATE
            SizedBox(
              width: double.infinity,
              child: ElevatedButton.icon(
                onPressed: busy ? null : _createFamily,
                icon: const Icon(Icons.group_add),
                label: busy
                    ? const SizedBox(height: 18, width: 18, child: CircularProgressIndicator(strokeWidth: 2))
                    : const Text('Neue Familie erstellen'),
              ),
            ),
const SizedBox(height: 14),
OutlinedButton.icon(
  onPressed: busy
      ? null
      : () async {
          final fid = await importEncryptedBackup(context: context);
          if (fid != null && context.mounted) {
            Navigator.of(context).pushAndRemoveUntil(
              MaterialPageRoute(builder: (_) => const BootGate()),
              (_) => false,
            );
          }
        },
  icon: const Icon(Icons.upload_file),
  label: const Text('Backup importieren'),
),
            const SizedBox(height: 16),
            const Text(
              'Hinweis: Es gibt keinen E-Mail Login mehr.\n'
              'Du verbindest Geräte über Code / QR.',
              textAlign: TextAlign.center,
              style: TextStyle(color: Colors.black54),
            )
            ],
          ),
        ),
      ),
    );
  }
}

/// QR Scan Screen (mobile_scanner)


/// =======================
///  WAITING FOR OWNER APPROVAL
/// =======================
class WaitingApprovalScreen extends StatelessWidget {
  final String familyId;
  final String memberName;
  final VoidCallback onApproved;

  const WaitingApprovalScreen({
    super.key,
    required this.familyId,
    required this.memberName,
    required this.onApproved,
  });

  @override
  Widget build(BuildContext context) {
    final uid = FirebaseAuth.instance.currentUser!.uid;
    final memberDoc = FirebaseFirestore.instance
        .collection('families')
        .doc(familyId)
        .collection('members')
        .doc(uid);

    final reqDoc = FirebaseFirestore.instance
        .collection('families')
        .doc(familyId)
        .collection('joinRequests')
        .doc(uid);

    return Scaffold(
      appBar: AppBar(title: const Text('Beitritt wartet auf Bestätigung')),
      body: Padding(
        padding: const EdgeInsets.all(16),
        child: StreamBuilder<DocumentSnapshot<Map<String, dynamic>>>(
          stream: memberDoc.snapshots(),
          builder: (context, snap) {
            if (snap.hasData && snap.data!.exists) {
              WidgetsBinding.instance.addPostFrameCallback((_) {
                onApproved();
                Navigator.of(context).popUntil((r) => r.isFirst);
              });
              return const Center(child: CircularProgressIndicator());
            }

            return Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                const Text(
                  'Deine Anfrage wurde gesendet.',
                  style: TextStyle(fontSize: 18, fontWeight: FontWeight.w600),
                ),
                const SizedBox(height: 8),
                const Text('Der Familien-Owner muss dich bestätigen.'),
                const SizedBox(height: 16),
                StreamBuilder<DocumentSnapshot<Map<String, dynamic>>>(
                  stream: reqDoc.snapshots(),
                  builder: (context, reqSnap) {
                    final data = reqSnap.data?.data();
                    final status = (data?['status'] as String?) ?? 'pending';
                    String txt = 'Status: ausstehend';
                    if (status == 'approved') txt = 'Status: bestätigt';
                    if (status == 'rejected') txt = 'Status: abgelehnt';
                    return Text(txt, style: const TextStyle(fontSize: 16));
                  },
                ),
                const Spacer(),
                FilledButton.icon(
                  onPressed: () => Navigator.pop(context),
                  icon: const Icon(Icons.arrow_back),
                  label: const Text('Zurück'),
                ),
              ],
            );
          },
        ),
      ),
    );
  }
}

/// =======================
///  OWNER: JOIN REQUESTS
/// =======================
class JoinRequestsScreen extends StatelessWidget {
  final DocumentReference<Map<String, dynamic>> famRef;
  final String familyId;

  const JoinRequestsScreen({
    super.key,
    required this.famRef,
    required this.familyId,
  });

  @override
  Widget build(BuildContext context) {
    final uid = FirebaseAuth.instance.currentUser!.uid;
    final myMemberDoc = famRef.collection('members').doc(uid);

    return Scaffold(
      appBar: AppBar(title: const Text('Beitrittsanfragen')),
      body: StreamBuilder<DocumentSnapshot<Map<String, dynamic>>>(
        stream: myMemberDoc.snapshots(),
        builder: (context, snap) {
          final role = snap.data?.data()?['role'] as String?;
          final isOwner = role == 'owner';

          if (!isOwner) {
            return const Center(child: Text('Nur der Owner kann Anfragen verwalten.'));
          }

          final q = famRef
              .collection('joinRequests')
              .where('status', isEqualTo: 'pending')
              .orderBy('createdAt', descending: false);

          return StreamBuilder<QuerySnapshot<Map<String, dynamic>>>(
            stream: q.snapshots(),
            builder: (context, qs) {
              if (qs.connectionState == ConnectionState.waiting) {
                return const Center(child: CircularProgressIndicator());
              }
              final docs = qs.data?.docs ?? [];
              if (docs.isEmpty) {
                return const Center(child: Text('Keine offenen Anfragen.'));
              }

              return ListView.separated(
                padding: const EdgeInsets.all(12),
                itemCount: docs.length,
                separatorBuilder: (_, __) => const SizedBox(height: 8),
                itemBuilder: (context, i) {
                  final d = docs[i];
                  final data = d.data();
                  final reqUid = (data['uid'] as String?) ?? d.id;
                  final name = (data['name'] as String?) ?? 'Unbekannt';

                  return Card(
                    child: ListTile(
                      leading: const Icon(Icons.person_add),
                      title: Text(name),
                      subtitle: Text(reqUid),
                      trailing: Row(
                        mainAxisSize: MainAxisSize.min,
                        children: [
                          IconButton(
                            tooltip: 'Ablehnen',
                            onPressed: () async {
                              await famRef.collection('joinRequests').doc(reqUid).update({
                                'status': 'rejected',
                                'decidedAt': FieldValue.serverTimestamp(),
                              });
                            },
                            icon: const Icon(Icons.close),
                          ),
                          IconButton(
                            tooltip: 'Bestätigen',
                            onPressed: () async {
                              final batch = FirebaseFirestore.instance.batch();
                              batch.set(famRef.collection('members').doc(reqUid), {
                                'uid': reqUid,
                                'name': name,
                                'role': 'member',
                                'joinedAt': FieldValue.serverTimestamp(),
                              });
                              batch.update(famRef.collection('joinRequests').doc(reqUid), {
                                'status': 'approved',
                                'decidedAt': FieldValue.serverTimestamp(),
                              });
                              await batch.commit();
                            },
                            icon: const Icon(Icons.check),
                          ),
                        ],
                      ),
                    ),
                  );
                },
              );
            },
          );
        },
      ),
    );
  }
}

class QrScanScreen extends StatelessWidget {
  const QrScanScreen({super.key});

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: const Text('QR scannen')),
      body: MobileScanner(
        onDetect: (capture) {
          final barcodes = capture.barcodes;
          if (barcodes.isEmpty) return;
          final raw = barcodes.first.rawValue;
          if (raw == null || raw.trim().isEmpty) return;
          Navigator.pop(context, raw.trim());
        },
      ),
    );
  }
}

/// =======================
/// HOME + FIRESTORE APP
/// =======================

enum TxType { expense, income }
enum TxStatus { open, paid }

class HomeScreen extends StatefulWidget {
  const HomeScreen({
    super.key,
    required this.familyId,
    required this.memberName,
    required this.onLeaveFamily,
  });

  final String familyId;
  final String? memberName;
  final VoidCallback onLeaveFamily;

  @override
  State<HomeScreen> createState() => _HomeScreenState();
}

class _HomeScreenState extends State<HomeScreen> {
  int idx = 0;
  final ValueNotifier<DateTime> monthNotifier = ValueNotifier(DateTime(DateTime.now().year, DateTime.now().month, 1));


  @override
  void initState() {
    super.initState();
    // Wiederkehrende Zahlungen beim Start nachziehen
    WidgetsBinding.instance.addPostFrameCallback((_) {
      ensureRecurringGeneratedForFamily(famRef: famRef);
    });
  }

  final db = FirebaseFirestore.instance;
  final auth = FirebaseAuth.instance;

  DocumentReference<Map<String, dynamic>> get famRef => db.collection('families').doc(widget.familyId);

  Stream<QuerySnapshot<Map<String, dynamic>>> get txStream =>
      famRef.collection('tx').orderBy('createdAt', descending: true).snapshots();

  Stream<DocumentSnapshot<Map<String, dynamic>>> get familyStream => famRef.snapshots();
  @override
  void dispose() {
    monthNotifier.dispose();
    super.dispose();
  }


  @override
  Widget build(BuildContext context) {
    final uid = auth.currentUser?.uid ?? '';
    return Scaffold(
      appBar: AppBar(
        title: const Text('FamilyBudget'),
        actions: [
          Padding(
            padding: const EdgeInsets.symmetric(horizontal: 10),
            child: Center(child: Text(widget.memberName ?? 'Member')),
          ),
          IconButton(
            tooltip: 'Familie wechseln',
            icon: const Icon(Icons.logout),
            onPressed: () async {
              // optional: member doc löschen (kannst du später machen)
              // await famRef.collection('members').doc(uid).delete();
              widget.onLeaveFamily();
            },
          )
        ],
      ),
      body: IndexedStack(
        index: idx,
        children: [
          OverviewTab(familyStream: familyStream, txStream: txStream, monthNotifier: monthNotifier),
          PaymentsTab(famRef: famRef, monthNotifier: monthNotifier),
          WorkTab(famRef: famRef),
          TaxesTab(famRef: famRef),
          SettingsTab(famRef: famRef, familyId: widget.familyId, monthNotifier: monthNotifier),
        ],
      ),
      bottomNavigationBar: NavigationBar(
        selectedIndex: idx,
        onDestinationSelected: (v) => setState(() => idx = v),
        destinations: const [
          NavigationDestination(icon: Icon(Icons.dashboard), label: 'Übersicht'),
          NavigationDestination(icon: Icon(Icons.payments), label: 'Zahlungen'),
          NavigationDestination(icon: Icon(Icons.work_outline), label: 'Arbeit'),
          NavigationDestination(icon: Icon(Icons.account_balance), label: 'Steuern'),
          NavigationDestination(icon: Icon(Icons.settings), label: 'Einstellungen'),
        ],
      ),
    );
  }
}

class OverviewTab extends StatelessWidget {
  final Stream<DocumentSnapshot<Map<String, dynamic>>> familyStream;
  final Stream<QuerySnapshot<Map<String, dynamic>>> txStream;
  final ValueNotifier<DateTime> monthNotifier;

  const OverviewTab({
    super.key,
    required this.familyStream,
    required this.txStream,
    required this.monthNotifier,
  });

  static DateTime _monthStart(DateTime d) => DateTime(d.year, d.month, 1);
  static DateTime _monthEndExclusive(DateTime d) => DateTime(d.year, d.month + 1, 1);

  static const _deMonths = <String>[
    'Januar','Februar','März','April','Mai','Juni','Juli','August','September','Oktober','November','Dezember'
  ];

  static String _monthLabel(DateTime d) => '${_deMonths[d.month - 1]} ${d.year}';

  static bool _isIncome(Map<String, dynamic> m, double amount) {
    final t = (m['type'] ?? m['kind'] ?? m['direction'] ?? '').toString().toLowerCase();
    if (m['isIncome'] == true) return true;
    if (m['isExpense'] == true) return false;
    if (t.contains('income') || t.contains('ein') || t.contains('gutsch')) return true;
    if (t.contains('expense') || t.contains('aus') || t.contains('abbuch')) return false;
    // fallback: positive = income, negative = expense
    return amount >= 0;
  }

  @override
  Widget build(BuildContext context) {
    return ValueListenableBuilder<DateTime>(
      valueListenable: monthNotifier,
      builder: (context, selected, _) {
        final selectedMonth = _monthStart(selected);
        final mStart = selectedMonth;
        final mEnd = _monthEndExclusive(selectedMonth);

        // Month list: last 12 + next 12 months
        final now = DateTime.now();
        final base = DateTime(now.year, now.month, 1);
        final months = <DateTime>[];
        for (int i = -11; i <= 12; i++) {
          months.add(DateTime(base.year, base.month + i, 1));
        }
        if (!months.any((m) => m.year == selectedMonth.year && m.month == selectedMonth.month)) {
          months.add(selectedMonth);
          months.sort((a, b) => a.compareTo(b));
        }

        return StreamBuilder<QuerySnapshot<Map<String, dynamic>>>(
          stream: txStream,
          builder: (context, snap) {
            final docs = snap.data?.docs ?? const [];
            double income = 0;
            double expense = 0;

            for (final d in docs) {
              final m = d.data();
              final dt = tsToDate(m['dueDate'] ?? m['date'] ?? m['effectiveDate'] ?? m['createdAt'] ?? m['ts']);
              if (dt == null) continue;
              if (dt.isBefore(mStart) || !dt.isBefore(mEnd)) continue;

              final amt = (m['amount'] as num?)?.toDouble() ?? 0.0;
              if (_isIncome(m, amt)) {
                income += amt.abs();
              } else {
                expense += amt.abs();
              }
            }

            final saldo = income - expense;

            Widget miniCard(String title, double value, IconData icon) {
              return Expanded(
                child: Card(
                  child: Padding(
                    padding: const EdgeInsets.all(12),
                    child: Column(
                      crossAxisAlignment: CrossAxisAlignment.start,
                      children: [
                        Row(
                          children: [
                            Icon(icon, size: 18),
                            const SizedBox(width: 8),
                            Text(title, style: const TextStyle(fontWeight: FontWeight.w600)),
                          ],
                        ),
                        const SizedBox(height: 8),
                        Text('${value.toStringAsFixed(2)} €', style: const TextStyle(fontSize: 18, fontWeight: FontWeight.bold)),
                      ],
                    ),
                  ),
                ),
              );
            }

            return SingleChildScrollView(
              padding: const EdgeInsets.all(16),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Row(
                    children: [
                      const Icon(Icons.calendar_month),
                      const SizedBox(width: 8),
                      Expanded(
                        child: DropdownButtonFormField<DateTime>(
                          value: selectedMonth,
                          decoration: const InputDecoration(
                            labelText: 'Monat',
                            border: OutlineInputBorder(),
                            isDense: true,
                          ),
                          items: months
                              .map((m) => DropdownMenuItem<DateTime>(
                                    value: m,
                                    child: Text(_monthLabel(m)),
                                  ))
                              .toList(),
                          onChanged: (v) {
                            if (v == null) return;
                            monthNotifier.value = _monthStart(v);
                          },
                        ),
                      ),
                    ],
                  ),
                  const SizedBox(height: 12),
                  Text('Monats-Zusammenfassung', style: Theme.of(context).textTheme.titleMedium),
                  const SizedBox(height: 10),
                  Row(
                    children: [
                      miniCard('Einnahmen', income, Icons.arrow_downward),
                      const SizedBox(width: 10),
                      miniCard('Ausgaben', expense, Icons.arrow_upward),
                    ],
                  ),
                  const SizedBox(height: 10),
                  Row(
                    children: [
                      miniCard('Saldo', saldo, Icons.account_balance_wallet),
                    ],
                  ),
                  const SizedBox(height: 16),

                  // Existing dashboard content (family config, quick actions, etc.)
                  // We keep the rest of the original Overview content by embedding the old widget tree
                  // via the helper below.
                  _OverviewRest(familyStream: familyStream, txStream: txStream, month: selectedMonth),
                ],
              ),
            );
          },
        );
      },
    );
  }
}

/// Extracted remainder of the original Overview UI so OverviewTab stays readable.
/// This widget is intentionally small: it only wraps the previous implementation
/// of the overview list/cards.
class _OverviewRest extends StatelessWidget {
  final Stream<DocumentSnapshot<Map<String, dynamic>>> familyStream;
  final Stream<QuerySnapshot<Map<String, dynamic>>> txStream;
  final DateTime month;

  const _OverviewRest({
    required this.familyStream,
    required this.txStream,
    required this.month,
  });

  static DateTime _monthStart(DateTime d) => DateTime(d.year, d.month, 1);
  static DateTime _monthEndExclusive(DateTime d) => DateTime(d.year, d.month + 1, 1);

  @override
  Widget build(BuildContext context) {
    // Minimal placeholder: keep compile-safe and avoids duplicating the huge old overview.
    // You can expand this later to show per-month tx list, charts, budgets, etc.
    final mStart = _monthStart(month);
    final mEnd = _monthEndExclusive(month);

    return Card(
      child: Padding(
        padding: const EdgeInsets.all(12),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text('Zahlungen im Monat', style: Theme.of(context).textTheme.titleSmall),
            const SizedBox(height: 8),
            StreamBuilder<QuerySnapshot<Map<String, dynamic>>>(
              stream: txStream,
              builder: (context, snap) {
                final docs = snap.data?.docs ?? const [];
                final monthDocs = docs.where((d) {
                  final dt = tsToDate(d.data()['dueDate'] ?? d.data()['date'] ?? d.data()['effectiveDate'] ?? d.data()['createdAt'] ?? d.data()['ts']);
                  if (dt == null) return false;
                  return !dt.isBefore(mStart) && dt.isBefore(mEnd);
                }).toList();

                if (monthDocs.isEmpty) {
                  return const Text('Keine Buchungen für diesen Monat.');
                }

                monthDocs.sort((a, b) {
                  final da = tsToDate(a.data()['dueDate'] ?? a.data()['date'] ?? a.data()['effectiveDate'] ?? a.data()['createdAt'] ?? a.data()['ts']) ?? DateTime(1970);
                  final db = tsToDate(b.data()['dueDate'] ?? b.data()['date'] ?? b.data()['effectiveDate'] ?? b.data()['createdAt'] ?? b.data()['ts']) ?? DateTime(1970);
                  return db.compareTo(da);
                });

                return Column(
                  children: monthDocs.take(8).map((d) {
                    final m = d.data();
                    final title = (m['title'] ?? m['name'] ?? 'Zahlung').toString();
                    final amt = (m['amount'] as num?)?.toDouble() ?? 0.0;
                    final dt = tsToDate(m['dueDate'] ?? m['date'] ?? m['effectiveDate'] ?? m['createdAt'] ?? m['ts']);
                    return Dismissible(
                      key: ValueKey(d.id),
                      direction: DismissDirection.endToStart,
                      confirmDismiss: (_) async {
                        return await showDialog<bool>(
                              context: context,
                              builder: (ctx) => AlertDialog(
                                title: const Text('Zahlung löschen?'),
                                content: Text('„$title“ wirklich löschen?'),
                                actions: [
                                  TextButton(onPressed: () => Navigator.pop(ctx, false), child: const Text('Abbrechen')),
                                  FilledButton(onPressed: () => Navigator.pop(ctx, true), child: const Text('Löschen')),
                                ],
                              ),
                            ) ??
                            false;
                      },
                      background: Container(
                        alignment: Alignment.centerRight,
                        padding: const EdgeInsets.symmetric(horizontal: 16),
                        color: Colors.red,
                        child: const Icon(Icons.delete, color: Colors.white),
                      ),
                      onDismissed: (_) async {
                        await d.reference.delete();
                        if (context.mounted) {
                          ScaffoldMessenger.of(context).showSnackBar(const SnackBar(content: Text('Zahlung gelöscht.')));
                        }
                      },
                      child: ListTile(
                        dense: true,
                        title: Text(title, maxLines: 1, overflow: TextOverflow.ellipsis),
                        subtitle: Text(dt == null ? '' : '${dt.day.toString().padLeft(2, '0')}.${dt.month.toString().padLeft(2, '0')}.${dt.year}'),
                        trailing: Text('${amt.toStringAsFixed(2)} €'),
                      ),
                    );
                  }).toList(),
                );
              },
            ),
          ],
        ),
      ),
    );
  }
}

class PaymentsTab extends StatefulWidget {
  const PaymentsTab({super.key, required this.famRef, required this.monthNotifier});

  final DocumentReference<Map<String, dynamic>> famRef;
  final ValueNotifier<DateTime> monthNotifier;

  @override
  State<PaymentsTab> createState() => _PaymentsTabState();
}

class _PaymentsTabState extends State<PaymentsTab> {
  String? _selectedAccountId; // null => Alle Konten

  DateTime get selectedMonth => widget.monthNotifier.value;

  CollectionReference<Map<String, dynamic>> get txCol => widget.famRef.collection('tx');
  CollectionReference<Map<String, dynamic>> get accountsCol => widget.famRef.collection('accounts');

  @override
  void initState() {
    super.initState();
    // Ensure default account exists + lightweight migration
    ensureDefaultAccountForFamily(widget.famRef);
  }

  Future<void> _addDialog(BuildContext context) async {
    final titleC = TextEditingController();
    final amountC = TextEditingController();
    final noteC = TextEditingController();

    // Stundenlohn/Arbeitszeit (für Gehalt per Stunden)
    final hourlyRateC = TextEditingController();
    final hoursC = TextEditingController();
    final breakMinC = TextEditingController(text: '0');
    TimeOfDay? startTime;
    TimeOfDay? endTime;
    bool payrollHourly = false; // false = Brutto manuell, true = Stundenlohn * Stunden

    String type = 'expense';
    String category = 'Sonstiges';
    DateTime dueDate = DateTime.now();
    bool recurring = false;
    String interval = 'monthly';

    // Gehalt (Brutto→Netto)
    bool payrollCalc = false;
    String taxProfileId = 'default';
    PayrollResult? payrollPreview;
    int every = 1;
    String? receiptPath; // Rechnung
    String? proofPath;   // Zahlungsbeleg (Überweisung/Quittung)

    // Unterkonto: Standard = aktuell ausgewähltes Konto, sonst default
    String accountId = _selectedAccountId ?? 'default';

    // Wichtig: Dialog-State muss außerhalb des Builders liegen,
    // sonst wird er bei jedem Rebuild zurückgesetzt.
    bool saving = false;
    String? errorMsg;

    Future<String?> _pickAndSave(ImageSource src, String prefix) async {
      try {
        final picker = ImagePicker();
        final x = await picker.pickImage(source: src, imageQuality: 85);
        if (x == null) return null;

        final dir = await getApplicationDocumentsDirectory();
        final fileName = '${prefix}_${DateTime.now().millisecondsSinceEpoch}${p.extension(x.path)}';
        final dest = File(p.join(dir.path, fileName));
        await dest.writeAsBytes(await x.readAsBytes());
        return dest.path;
      } catch (e) {
        debugPrint('PickImage error: $e');
      return null;
    }
    }

    final saved = await showDialog<bool>(
      context: context,
      barrierDismissible: false,
      builder: (dialogCtx) => StatefulBuilder(
        builder: (dialogCtx, setS) {
          void close([bool? result]) {
            if (!dialogCtx.mounted) return;
            Navigator.of(dialogCtx).pop(result);
          }


          return AlertDialog(
            title: const Text('Neue Zahlung'),
            content: SingleChildScrollView(
              child: Column(
                mainAxisSize: MainAxisSize.min,
                children: [
                  // Unterkonto Auswahl
                  StreamBuilder<QuerySnapshot<Map<String, dynamic>>>(
                    stream: accountsCol.orderBy('name').snapshots(),
                    builder: (c, snap) {
                      final items = <MapEntry<String, String>>[
                        const MapEntry('default', 'Haushalt'),
                      ];
                      if (snap.hasData) {
                        for (final d in snap.data!.docs) {
                          final data = d.data();
                          if (data['archived'] == true) continue;
                          final name = (data['name'] ?? '').toString().trim();
                          if (name.isEmpty) continue;
                          // avoid duplicate default entry
                          if (d.id == 'default') {
                            items[0] = MapEntry('default', name);
                          } else {
                            items.add(MapEntry(d.id, name));
                          }
                        }
                      }
                      final ids = items.map((e) => e.key).toSet();
                      final effectiveAccountId = ids.contains(accountId) ? accountId : 'default';

                      return DropdownButtonFormField<String>(
                        value: effectiveAccountId,
                        decoration: const InputDecoration(labelText: 'Konto'),
                        items: items
                            .map((e) => DropdownMenuItem(value: e.key, child: Text(e.value)))
                            .toList(),
                        onChanged: (v) => setS(() => accountId = v ?? 'default'),
                      );
                    },
                  ),
                  const SizedBox(height: 8),

                  TextField(controller: titleC, decoration: const InputDecoration(labelText: 'Titel')),
                  const SizedBox(height: 8),
                  TextField(
                    controller: amountC,
                    readOnly: payrollCalc && payrollHourly,
                    decoration: InputDecoration(
                      labelText: payrollCalc && payrollHourly ? 'Brutto (berechnet)' : 'Betrag (€)',
                      helperText: (payrollCalc && payrollHourly)
                          ? 'Brutto wird aus Stundenlohn × Stunden berechnet.'
                          : null,
                    ),
                    keyboardType: const TextInputType.numberWithOptions(decimal: true),
                    onChanged: (_) => setS(() {}),
                  ),
                  const SizedBox(height: 8),
                  DropdownButtonFormField<String>(
                    value: type,
                    decoration: const InputDecoration(labelText: 'Typ'),
                    items: const [
                      DropdownMenuItem(value: 'expense', child: Text('Ausgabe')),
                      DropdownMenuItem(value: 'income', child: Text('Einnahme')),
                    ],
                    onChanged: (v) => setS(() => type = v ?? 'expense'),
                  ),

                  // Gehalt: Brutto → Netto (erstellt automatisch Netto-Einnahme + Abzüge als Ausgaben)
                  if (type == 'income') ...[
                    const SizedBox(height: 8),
                    SwitchListTile(
                      contentPadding: EdgeInsets.zero,
                      value: payrollCalc,
                      onChanged: (v) => setS(() {
                        payrollCalc = v;
                        if (payrollCalc) {
                          category = 'Gehalt';
                          recurring = false; // V1: recurring für payroll später als Regel-Generator
                        }
                      }),
                      title: const Text('Gehalt (Brutto→Netto)'),
                      subtitle: const Text('Erstellt Netto-Einnahme + Abzüge (KV/PV/RV/AV). Steuern tabellengenau folgt als nächster Schritt.'),
                    ),
                  ],

                  const SizedBox(height: 8),
                  StreamBuilder<QuerySnapshot<Map<String, dynamic>>>(
                    stream: widget.famRef.collection('categories').orderBy('nameLower').snapshots(),
                    builder: (c, snap) {
                      final items = <String>{'Sonstiges'};
                    if (snap.hasError) {
                      debugPrint('Kategorie-Stream Fehler: ${snap.error}');
                    }
                      if (snap.hasData) {
                        for (final d in snap.data!.docs) {
                        final data = d.data();
                        if (data['archived'] == true) continue;
                        final n = (data['name'] ?? '').toString().trim();
                        if (n.isNotEmpty) items.add(n);
                      }
                      }
                      final list = items.toList()..sort();
                      if (!list.contains(category)) category = list.first;
                      return DropdownButtonFormField<String>(
                        value: category,
                        decoration: const InputDecoration(labelText: 'Kategorie'),
                        items: list.map((e) => DropdownMenuItem(value: e, child: Text(e))).toList(),
                        onChanged: (v) => setS(() => category = v ?? 'Sonstiges'),
                      );
                    },
                  ),
                  const SizedBox(height: 8),

                  if (payrollCalc) ...[
                    // Eingabeart: Brutto direkt oder Stundenlohn × Stunden
                    Row(
                      children: [
                        Expanded(
                          child: ChoiceChip(
                            label: const Text('Brutto'),
                            selected: !payrollHourly,
                            onSelected: (_) => setS(() {
                              payrollHourly = false;
                            }),
                          ),
                        ),
                        const SizedBox(width: 8),
                        Expanded(
                          child: ChoiceChip(
                            label: const Text('Stunden'),
                            selected: payrollHourly,
                            onSelected: (_) => setS(() {
                              payrollHourly = true;
                              // setze Kategorie sinnvoll
                              category = 'Gehalt';
                              // Wenn leer, initiale Werte setzen
                              if (hourlyRateC.text.trim().isEmpty) hourlyRateC.text = '0';
                              if (hoursC.text.trim().isEmpty) hoursC.text = '0';
                              // Brutto neu berechnen
                              final rate = double.tryParse(hourlyRateC.text.replaceAll(',', '.')) ?? 0.0;
                              final hrs = double.tryParse(hoursC.text.replaceAll(',', '.')) ?? 0.0;
                              final gross = rate * hrs;
                              amountC.text = gross.toStringAsFixed(2);
                            }),
                          ),
                        ),
                      ],
                    ),
                    const SizedBox(height: 8),

                    if (payrollHourly) ...[
                      TextField(
                        controller: hourlyRateC,
                        decoration: const InputDecoration(labelText: 'Stundenlohn (€)'),
                        keyboardType: const TextInputType.numberWithOptions(decimal: true),
                        onChanged: (_) => setS(() {
                          final rate = double.tryParse(hourlyRateC.text.replaceAll(',', '.')) ?? 0.0;
                          final hrs = double.tryParse(hoursC.text.replaceAll(',', '.')) ?? 0.0;
                          amountC.text = (rate * hrs).toStringAsFixed(2);
                        }),
                      ),
                      const SizedBox(height: 8),
                      TextField(
                        controller: hoursC,
                        decoration: const InputDecoration(labelText: 'Stunden (z.B. 37,5)'),
                        keyboardType: const TextInputType.numberWithOptions(decimal: true),
                        onChanged: (_) => setS(() {
                          final rate = double.tryParse(hourlyRateC.text.replaceAll(',', '.')) ?? 0.0;
                          final hrs = double.tryParse(hoursC.text.replaceAll(',', '.')) ?? 0.0;
                          amountC.text = (rate * hrs).toStringAsFixed(2);
                        }),
                      ),
                      const SizedBox(height: 8),

                      ExpansionTile(
                        tilePadding: EdgeInsets.zero,
                        title: const Text('Stundenrechner'),
                        subtitle: const Text('Start/Ende + Pause → Stunden automatisch'),
                        children: [
                          Row(
                            children: [
                              Expanded(
                                child: OutlinedButton.icon(
                                  icon: const Icon(Icons.schedule),
                                  label: Text(startTime == null ? 'Start' : startTime!.format(dialogCtx)),
                                  onPressed: () async {
                                    final t = await showTimePicker(
                                      context: dialogCtx,
                                      initialTime: startTime ?? TimeOfDay.now(),
                                    );
                                    if (t != null) setS(() => startTime = t);
                                  },
                                ),
                              ),
                              const SizedBox(width: 8),
                              Expanded(
                                child: OutlinedButton.icon(
                                  icon: const Icon(Icons.schedule),
                                  label: Text(endTime == null ? 'Ende' : endTime!.format(dialogCtx)),
                                  onPressed: () async {
                                    final t = await showTimePicker(
                                      context: dialogCtx,
                                      initialTime: endTime ?? TimeOfDay.now(),
                                    );
                                    if (t != null) setS(() => endTime = t);
                                  },
                                ),
                              ),
                            ],
                          ),
                          const SizedBox(height: 8),
                          TextField(
                            controller: breakMinC,
                            decoration: const InputDecoration(labelText: 'Pause (Minuten)', hintText: 'z.B. 30'),
                            keyboardType: TextInputType.number,
                          ),
                          const SizedBox(height: 8),
                          Align(
                            alignment: Alignment.centerLeft,
                            child: ElevatedButton.icon(
                              icon: const Icon(Icons.calculate),
                              label: const Text('Stunden berechnen & übernehmen'),
                              onPressed: () {
                                if (startTime == null || endTime == null) return;
                                int s = startTime!.hour * 60 + startTime!.minute;
                                int e = endTime!.hour * 60 + endTime!.minute;
                                if (e < s) e += 24 * 60; // über Mitternacht
                                final pause = int.tryParse(breakMinC.text.trim()) ?? 0;
                                final mins = (e - s) - pause;
                                final hrs = mins <= 0 ? 0.0 : mins / 60.0;
                                hoursC.text = hrs.toStringAsFixed(2);
                                final rate = double.tryParse(hourlyRateC.text.replaceAll(',', '.')) ?? 0.0;
                                amountC.text = (rate * hrs).toStringAsFixed(2);
                                setS(() {});
                              },
                            ),
                          ),
                          const SizedBox(height: 8),
                        ],
                      ),
                      const SizedBox(height: 8),
                    ],

                    StreamBuilder<QuerySnapshot<Map<String, dynamic>>>(
                      stream: widget.famRef.collection('taxProfiles').snapshots(),
                      builder: (c, snap) {
                        final docs = snap.data?.docs ?? const [];
                        final hasDefault = docs.any((d) => d.id == taxProfileId);
                        final items = docs.isNotEmpty
                            ? docs
                            : [
                                // falls noch nichts existiert -> virtuelles default
                              ];
                        // Wenn noch kein Profil existiert, zeigen wir trotzdem 'default'
                        final dropdownItems = <DropdownMenuItem<String>>[
                          const DropdownMenuItem(value: 'default', child: Text('Standard (default)')),
                          ...docs.map((d) => DropdownMenuItem(value: d.id, child: Text((d.data()['name'] ?? d.id).toString()))),
                        ];

                        if (!hasDefault) {
                          // ok, default bleibt auswählbar
                        }

                        return DropdownButtonFormField<String>(
                          value: taxProfileId,
                          decoration: const InputDecoration(labelText: 'Steuerprofil'),
                          items: dropdownItems,
                          onChanged: (v) => setS(() {
                            taxProfileId = v ?? 'default';
                            payrollPreview = null;
                          }),
                        );
                      },
                    ),
                    const SizedBox(height: 8),
                    Builder(
                      builder: (c) {
                        final gross = double.tryParse(amountC.text.replaceAll(',', '.')) ?? 0.0;
                        return FutureBuilder<DocumentSnapshot<Map<String, dynamic>>>(
                          future: widget.famRef.collection('taxProfiles').doc(taxProfileId).get(),
                          builder: (c, snap) {
                            TaxProfile profile;
                            if (snap.hasData && snap.data!.data() != null) {
                              profile = TaxProfile.fromMap(snap.data!.id, snap.data!.data()!);
                            } else {
                              profile = const TaxProfile(
                                id: 'default',
                                name: 'Standard',
                                year: 2026,
                                state: 'Berlin',
                                taxClass: 'I',
                                churchTax: false,
                                churchTaxRate: 0.09,
                                soli: true,
                                kvEmployeeRate: 0.081,
                                pvEmployeeRate: 0.017,
                                rvEmployeeRate: 0.093,
                                avEmployeeRate: 0.013,
                                pvChildlessSurcharge: false,
                                pvChildlessSurchargeRate: 0.006,
                                fixedPkV: 0.0,
                              );
                            }

                            final pr = (gross > 0) ? computePayrollV1(gross: gross, profile: profile) : null;
                            payrollPreview = pr;

                            if (pr == null) {
                              return const Text('Gib bei Betrag dein Brutto ein, dann zeigen wir dir eine Vorschau.');
                            }

                            final rows = <Widget>[
                              Text('Netto (Vorschau): ${pr.net.toStringAsFixed(2)} €', style: const TextStyle(fontWeight: FontWeight.bold)),
                              const SizedBox(height: 4),
                              ...pr.deductions.entries.map((e) => Text('${e.key}: -${e.value.toStringAsFixed(2)} €')),
                              ...pr.taxes.entries.map((e) => Text('${e.key}: -${e.value.toStringAsFixed(2)} €')),
                            ];

                            return Container(
                              padding: const EdgeInsets.all(12),
                              decoration: BoxDecoration(
                                border: Border.all(color: Theme.of(context).dividerColor),
                                borderRadius: BorderRadius.circular(12),
                              ),
                              child: Column(crossAxisAlignment: CrossAxisAlignment.start, children: rows),
                            );
                          },
                        );
                      },
                    ),
                  ],

                  ListTile(
                    contentPadding: EdgeInsets.zero,
                    title: const Text('Fällig am'),
                    subtitle: Text(
                      '${dueDate.day.toString().padLeft(2, '0')}.${dueDate.month.toString().padLeft(2, '0')}.${dueDate.year}',
                    ),
                    trailing: const Icon(Icons.calendar_month),
                    onTap: () async {
                      final d = await showDatePicker(
                        context: dialogCtx,
                        firstDate: DateTime(2020),
                        lastDate: DateTime(2100),
                        initialDate: dueDate,
                      );
                      if (d != null) setS(() => dueDate = d);
                    },
                  ),
                  const Divider(),
                  SwitchListTile(
                    contentPadding: EdgeInsets.zero,
                    title: const Text('Wiederkehrend'),
                    value: recurring,
                    onChanged: (v) => setS(() => recurring = v),
                  ),
                  if (recurring) ...[
                    DropdownButtonFormField<String>(
                      value: interval,
                      decoration: const InputDecoration(labelText: 'Intervall'),
                      items: const [
                        DropdownMenuItem(value: 'monthly', child: Text('Monatlich')),
                        DropdownMenuItem(value: 'weekly', child: Text('Wöchentlich')),
                        DropdownMenuItem(value: 'yearly', child: Text('Jährlich')),
                      ],
                      onChanged: (v) => setS(() => interval = v ?? 'monthly'),
                    ),
                    const SizedBox(height: 8),
                    TextField(
                      decoration: const InputDecoration(labelText: 'Jeden … (z.B. 1)'),
                      keyboardType: TextInputType.number,
                      onChanged: (v) => setS(() => every = int.tryParse(v) ?? 1),
                    ),
                  ],
                  const Divider(),
                  TextField(
                    controller: noteC,
                    decoration: const InputDecoration(labelText: 'Notiz (optional)'),
                    minLines: 1,
                    maxLines: 4,
                  ),
                  const SizedBox(height: 12),
                  Row(
                    children: [
                      Expanded(
                        child: OutlinedButton.icon(
                          onPressed: () async {
                            final p = await _pickAndSave(ImageSource.camera, 'invoice');
                            if (p != null) setS(() => receiptPath = p);
                          },
                          icon: const Icon(Icons.photo_camera),
                          label: const Text('Rechnung Kamera'),
                        ),
                      ),
                      const SizedBox(width: 8),
                      Expanded(
                        child: OutlinedButton.icon(
                          onPressed: () async {
                            final p = await _pickAndSave(ImageSource.gallery, 'invoice');
                            if (p != null) setS(() => receiptPath = p);
                          },
                          icon: const Icon(Icons.photo_library),
                          label: const Text('Rechnung Galerie'),
                        ),
                      ),
                    ],
                  ),
                  const SizedBox(height: 8),
                  Row(
                    children: [
                      Expanded(
                        child: OutlinedButton.icon(
                          onPressed: () async {
                            final p = await _pickAndSave(ImageSource.camera, 'proof');
                            if (p != null) setS(() => proofPath = p);
                          },
                          icon: const Icon(Icons.receipt_long),
                          label: const Text('Beleg Kamera'),
                        ),
                      ),
                      const SizedBox(width: 8),
                      Expanded(
                        child: OutlinedButton.icon(
                          onPressed: () async {
                            final p = await _pickAndSave(ImageSource.gallery, 'proof');
                            if (p != null) setS(() => proofPath = p);
                          },
                          icon: const Icon(Icons.collections),
                          label: const Text('Beleg Galerie'),
                        ),
                      ),
                    ],
                  ),
                ],
              ),
            ),
            actions: [
              TextButton(
                onPressed: saving ? null : () => close(false),
                child: const Text('Abbrechen'),
              ),
              FilledButton(
                onPressed: saving
                    ? null
                    : () async {
                        final title = titleC.text.trim();
                        final amount = double.tryParse(amountC.text.replaceAll(',', '.')) ?? 0;
                        if (title.isEmpty || amount <= 0) {
                          setS(() => errorMsg = 'Bitte Titel und Betrag eingeben.');
                          return;
                        }
                        setS(() => errorMsg = null);

                        setS(() => saving = true);
                        try {
                          final now = DateTime.now();
                          final due = DateTime(dueDate.year, dueDate.month, dueDate.day);

                          // status/paid Konsistenz
                          final paid = type == 'income' ? true : false;
                          final status = type == 'income' ? 'paid' : 'open';

                          if (payrollCalc) {
                            // V1: payrollCalc erzeugt Netto-Einnahme + Abzüge als Ausgaben
                            final gross = amount;

                            final profSnap = await widget.famRef.collection('taxProfiles').doc(taxProfileId).get();
                            final profMap = profSnap.data();
                            final profile = (profMap != null)
                                ? TaxProfile.fromMap(profSnap.id, profMap)
                                : const TaxProfile(
                                    id: 'default',
                                    name: 'Standard',
                                    year: 2026,
                                    state: 'Berlin',
                                    taxClass: 'I',
                                    churchTax: false,
                                    churchTaxRate: 0.09,
                                    soli: true,
                                    kvEmployeeRate: 0.081,
                                    pvEmployeeRate: 0.017,
                                    rvEmployeeRate: 0.093,
                                    avEmployeeRate: 0.013,
                                    pvChildlessSurcharge: false,
                                    pvChildlessSurchargeRate: 0.006,
                                    fixedPkV: 0.0,
                                  );

                            final pr = computePayrollV1(gross: gross, profile: profile);

                            final groupId = txCol.doc().id;
                            final batch = FirebaseFirestore.instance.batch();

                            final netDoc = txCol.doc(groupId);

                            batch.set(netDoc, {
                              'accountId': accountId,
                              'title': title.isEmpty ? 'Gehalt' : title,
                              'amount': pr.net,
                              'type': 'income',
                              'category': 'Gehalt',
                              'dueDate': Timestamp.fromDate(due),
                              'note': noteC.text.trim(),
                              'paid': true,
                              'status': 'paid',
                              'createdAt': Timestamp.fromDate(now),
                              'updatedAt': Timestamp.fromDate(now),
                              'payroll': {
                                'gross': pr.gross,
                                'net': pr.net,
                                'taxProfileId': taxProfileId,
                                'deductions': pr.deductions,
                                'taxes': pr.taxes,
                                'version': 1,
                              },
                            }, SetOptions(merge: true));

                            // Abzüge als Ausgaben (für Auswertungen)
                            int n = 0;
                            Future<void> addDed(String key, double val) async {
                              if (val <= 0) return;
                              n++;
                              final did = '${groupId}_ded_$n';
                              batch.set(txCol.doc(did), {
                                'accountId': accountId,
                                'title': '$key (vom Gehalt)',
                                'amount': val,
                                'type': 'expense',
                                'category': 'Steuern & Abgaben',
                                'dueDate': Timestamp.fromDate(due),
                                'note': 'Auto-Abzug aus Gehalt: $title',
                                'paid': true,
                                'status': 'paid',
                                'createdAt': Timestamp.fromDate(now),
                                'updatedAt': Timestamp.fromDate(now),
                                'payrollGroupId': groupId,
                              }, SetOptions(merge: true));
                            }

                            for (final e in pr.deductions.entries) {
                              await addDed(e.key, e.value);
                            }
                            for (final e in pr.taxes.entries) {
                              await addDed(e.key, e.value);
                            }

                            await batch.commit();
                          } else {
                            final newDoc = txCol.doc();
                            await newDoc.set({
                              'accountId': accountId,
                              'title': title,
                              'amount': amount,
                              'type': type,
                              'category': category,
                              'dueDate': Timestamp.fromDate(due),
                              'note': noteC.text.trim(),
                              'paid': paid,
                              'status': status,
                              if (receiptPath != null) 'receiptPath': receiptPath,
                              if (proofPath != null) 'proofPath': proofPath,
                              'createdAt': Timestamp.fromDate(now),
                              'updatedAt': Timestamp.fromDate(now),
                            }, SetOptions(merge: true));

                            if (recurring) {
                              final ruleRef = widget.famRef.collection('recurring_rules').doc();
                              await ruleRef.set({
                                'accountId': accountId,
                                'title': title,
                                'amount': amount,
                                'type': type,
                                'category': category,
                                'note': noteC.text.trim(),
                                'unit': interval, // weekly/monthly/yearly
                                'interval': every, // int
                                // legacy compatibility:
                                'intervalLegacy': interval,
                                'every': every,
                                'startDate': Timestamp.fromDate(due),
                                'createdAt': Timestamp.fromDate(now),
                                'updatedAt': Timestamp.fromDate(now),
                                'archived': false,
                              }, SetOptions(merge: true));

                              await newDoc.set({'recurringRuleId': ruleRef.id}, SetOptions(merge: true));
                            }
                          }

close(true);
                        } catch (e) {
                          debugPrint('Save payment error: $e');
                          setS(() {
                            errorMsg = 'Speichern fehlgeschlagen: $e';
                            saving = false;
                          });
                        }
                      },
                child: const Text('Speichern'),
              ),
            ],
          );
        },
      ),
    );


    if (saved == true && context.mounted) {
      ScaffoldMessenger.of(context)
        ..hideCurrentSnackBar()
        ..showSnackBar(const SnackBar(content: Text('Erfolgreich gespeichert.')));
    }

    titleC.dispose();
    amountC.dispose();
    noteC.dispose();
  }

  Future<void> _togglePaid(DocumentSnapshot<Map<String, dynamic>> doc) async {
    final data = doc.data() ?? {};
    final type = data['type'] as String? ?? 'expense';
    if (type == 'income') return;

    final status = data['status'] as String? ?? 'open';
    final newStatus = status == 'paid' ? 'open' : 'paid';
    await doc.reference.update({'status': newStatus});
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      floatingActionButton: FloatingActionButton(
        onPressed: () => _addDialog(context),
        child: const Icon(Icons.add),
      ),
      body: Column(
        children: [
          // Konto-Auswahl (Alle + einzelne Konten)
          Padding(
            padding: const EdgeInsets.fromLTRB(12, 12, 12, 4),
            child: StreamBuilder<QuerySnapshot<Map<String, dynamic>>>(
              stream: accountsCol.orderBy('name').snapshots(),
              builder: (context, snap) {
                final entries = <MapEntry<String?, String>>[
                  const MapEntry<String?, String>(null, 'Alle Konten'),
                ];

                // Default + weitere
                String defaultName = 'Haushalt';
                final others = <MapEntry<String?, String>>[];

                if (snap.hasData) {
                  for (final d in snap.data!.docs) {
                    final data = d.data();
                    if (data['archived'] == true) continue;
                    final name = (data['name'] ?? '').toString().trim();
                    if (name.isEmpty) continue;
                    if (d.id == 'default') {
                      defaultName = name;
                    } else {
                      others.add(MapEntry(d.id, name));
                    }
                  }
                }

                entries.add(MapEntry('default', defaultName));
                others.sort((a, b) => a.value.toLowerCase().compareTo(b.value.toLowerCase()));
                entries.addAll(others);

                final current = entries.any((e) => e.key == _selectedAccountId) ? _selectedAccountId : null;

                return DropdownButtonFormField<String?>(
                  value: current,
                  decoration: const InputDecoration(
                    labelText: 'Konto',
                    border: OutlineInputBorder(),
                    isDense: true,
                  ),
                  items: entries
                      .map((e) => DropdownMenuItem<String?>(value: e.key, child: Text(e.value)))
                      .toList(),
                  onChanged: (v) => setState(() => _selectedAccountId = v),
                );
              },
            ),
          ),
          Expanded(
            child: StreamBuilder<QuerySnapshot<Map<String, dynamic>>>(
              stream: txCol.orderBy('createdAt', descending: true).snapshots(),
              builder: (context, snap) {
                if (!snap.hasData) return const Center(child: CircularProgressIndicator());
                final allDocs = snap.data!.docs;

                final mStart = monthStart(widget.monthNotifier.value);
                final mEnd = monthEndExclusive(widget.monthNotifier.value);

                final docs = allDocs.where((d) {
                  final data = d.data();

                  // Konto Filter
                  final acc = (data['accountId'] ?? 'default').toString();
                  if (_selectedAccountId != null && acc != _selectedAccountId) return false;

                  final dt = tsToDate(data['dueDate']) ?? tsToDate(data['date']) ?? tsToDate(data['createdAt']) ?? DateTime(1970);
                  return !dt.isBefore(mStart) && dt.isBefore(mEnd);
                }).toList();

                if (docs.isEmpty) {
                  return const Center(child: Text('Noch keine Zahlungen in diesem Monat/Konto.'));
                }

                return ListView.separated(
                  padding: const EdgeInsets.all(12),
                  itemBuilder: (context, i) {
                    final d = docs[i];
                    final data = d.data();
                    final title = data['title'] as String? ?? '';
                    final amount = (data['amount'] as num?)?.toDouble() ?? 0.0;
                    final type = data['type'] as String? ?? 'expense';
                    final status = data['status'] as String? ?? 'open';
                    final dueTs = data['dueDate'] as Timestamp?;
                    final due = dueTs == null ? null : dueTs.toDate();

                    final typeLabel = type == 'income' ? 'Einnahme' : 'Ausgabe';
                    final statusLabel = type == 'income' ? 'Verbucht' : (status == 'paid' ? 'Bezahlt' : 'Offen');
                    final dueText = due == null ? '' : ' • fällig ${due.toLocal().toString().split(' ').first}';

                    return Dismissible(
                      key: ValueKey(d.id),
                      direction: DismissDirection.endToStart,
                      background: Container(
                        alignment: Alignment.centerRight,
                        padding: const EdgeInsets.symmetric(horizontal: 16),
                        color: Colors.red,
                        child: const Icon(Icons.delete, color: Colors.white),
                      ),
                      confirmDismiss: (_) async {
                        return await showDialog<bool>(
                              context: context,
                              builder: (c) => AlertDialog(
                                title: const Text('Zahlung löschen?'),
                                content: Text('„$title“ wirklich löschen?'),
                                actions: [
                                  TextButton(onPressed: () => Navigator.pop(c, false), child: const Text('Abbrechen')),
                                  ElevatedButton(onPressed: () => Navigator.pop(c, true), child: const Text('Löschen')),
                                ],
                              ),
                            ) ??
                            false;
                      },
                      onDismissed: (_) async {
                        await d.reference.delete();
                      },
                      child: ListTile(
                        title: Text(title),
                        subtitle: Text('$typeLabel • $statusLabel$dueText'),
                        trailing: Text('${amount.toStringAsFixed(2)} €'),
                        onTap: () => openTxDetails(context, d),
                        onLongPress: () => _togglePaid(d),
                      ),
                    );
                  },
                  separatorBuilder: (_, __) => const Divider(),
                  itemCount: docs.length,
                );
              },
            ),
          ),
        ],
      ),
    );
  }
}


// ===================== Steuern & Abgaben (Tax Profile + Payroll helpers) =====================


// ===== Bundesländer (Dropdown) =====
const List<String> bundeslaender = [
  'Baden-Württemberg',
  'Bayern',
  'Berlin',
  'Brandenburg',
  'Bremen',
  'Hamburg',
  'Hessen',
  'Mecklenburg-Vorpommern',
  'Niedersachsen',
  'Nordrhein-Westfalen',
  'Rheinland-Pfalz',
  'Saarland',
  'Sachsen',
  'Sachsen-Anhalt',
  'Schleswig-Holstein',
  'Thüringen',
];



// ===== Default SV-Raten (AN) – du kannst sie im UI überschreiben =====
// Hinweis: Diese Defaults sind als Startwerte gedacht. Für 100% genaue Nettolohn-Berechnung
// (inkl. Lohnsteuer-Tabellen, Beitragsbemessungsgrenzen, Sonderfälle) erweitern wir das Schritt für Schritt.
const Map<int, Map<String, double>> kSvEmployeeDefaultsByYear = {
  2025: {'kv': 0.081, 'pv': 0.017, 'rv': 0.093, 'av': 0.013, 'pvChildless': 0.006},
  2026: {'kv': 0.081, 'pv': 0.017, 'rv': 0.093, 'av': 0.013, 'pvChildless': 0.006},
};

double _defaultChurchRateForState(String state) {
  // BW & Bayern: 8%, Rest: 9%
  final s = state.trim().toLowerCase();
  if (s.contains('baden') || s.contains('bayern')) return 0.08;
  return 0.09;
}

class TaxProfile {
  final String id;
  final String name;
  final int year;
  final String state; // Bundesland
  final String taxClass; // I-VI
  final bool churchTax;
  final double churchTaxRate; // 0.08 or 0.09 (config)
  final bool soli;
  final double kvEmployeeRate; // z.B. 0.073 + Zusatzbeitrag/2 (frei konfigurierbar)
  final double pvEmployeeRate; // Pflegeversicherung (AN)
  final double rvEmployeeRate; // Rentenversicherung (AN)
  final double avEmployeeRate; // Arbeitslosenversicherung (AN)
  final bool pvChildlessSurcharge; // kinderlos >23 -> PV Zuschlag (vereinfachter Schalter)
  final double pvChildlessSurchargeRate; // z.B. 0.006 (config)
  final double fixedPkV; // wenn PKV genutzt wird, optional fixer Betrag (0 => ignorieren)

  const TaxProfile({
    required this.id,
    required this.name,
    required this.year,
    required this.state,
    required this.taxClass,
    required this.churchTax,
    required this.churchTaxRate,
    required this.soli,
    required this.kvEmployeeRate,
    required this.pvEmployeeRate,
    required this.rvEmployeeRate,
    required this.avEmployeeRate,
    required this.pvChildlessSurcharge,
    required this.pvChildlessSurchargeRate,
    required this.fixedPkV,
  });

  static TaxProfile fromMap(String id, Map<String, dynamic> m) {
    double d(dynamic v, double def) => (v is num) ? v.toDouble() : double.tryParse('${v ?? ''}'.replaceAll(',', '.')) ?? def;
    bool b(dynamic v, bool def) => (v is bool) ? v : (v == null ? def : (v.toString().toLowerCase() == 'true'));
    int i(dynamic v, int def) => (v is num) ? v.toInt() : int.tryParse('$v') ?? def;

    return TaxProfile(
      id: id,
      name: (m['name'] ?? 'Standard').toString(),
      year: i(m['year'], DateTime.now().year),
      state: (m['state'] ?? 'Berlin').toString(),
      taxClass: (m['taxClass'] ?? 'I').toString(),
      churchTax: b(m['churchTax'], false),
      churchTaxRate: d(m['churchTaxRate'], 0.09),
      soli: b(m['soli'], true),
      kvEmployeeRate: d(m['kvEmployeeRate'], 0.081), // Default bewusst grob; lieber im Profil korrekt einstellen
      pvEmployeeRate: d(m['pvEmployeeRate'], 0.017),
      rvEmployeeRate: d(m['rvEmployeeRate'], 0.093),
      avEmployeeRate: d(m['avEmployeeRate'], 0.013),
      pvChildlessSurcharge: b(m['pvChildlessSurcharge'], false),
      pvChildlessSurchargeRate: d(m['pvChildlessSurchargeRate'], 0.006),
      fixedPkV: d(m['fixedPkV'], 0.0),
    );
  }

  Map<String, dynamic> toMap() => {
        'name': name,
        'year': year,
        'state': state,
        'taxClass': taxClass,
        'churchTax': churchTax,
        'churchTaxRate': churchTaxRate,
        'soli': soli,
        'kvEmployeeRate': kvEmployeeRate,
        'pvEmployeeRate': pvEmployeeRate,
        'rvEmployeeRate': rvEmployeeRate,
        'avEmployeeRate': avEmployeeRate,
        'pvChildlessSurcharge': pvChildlessSurcharge,
        'pvChildlessSurchargeRate': pvChildlessSurchargeRate,
        'fixedPkV': fixedPkV,
        'updatedAt': FieldValue.serverTimestamp(),
      };
}

class PayrollResult {
  final double gross;
  final double net;
  final Map<String, double> deductions; // positive numbers
  final Map<String, double> taxes; // reserved for later

  const PayrollResult({
    required this.gross,
    required this.net,
    required this.deductions,
    required this.taxes,
  });
}

/// V1 Payroll: macht SV (KV/PV/RV/AV) aus konfigurierten Raten.
/// Lohnsteuer/Soli/Kirchensteuer sind hier **noch nicht** tabellengenau –
/// du kannst das später austauschen, ohne dass UI/Datenmodell geändert werden müssen.
PayrollResult computePayrollV1({
  required double gross,
  required TaxProfile profile,
}) {
  double round2(double v) => (v * 100).roundToDouble() / 100.0;

  final kv = profile.fixedPkV > 0 ? profile.fixedPkV : gross * profile.kvEmployeeRate;
  final pvBase = gross * profile.pvEmployeeRate;
  final pvSurcharge = profile.pvChildlessSurcharge ? gross * profile.pvChildlessSurchargeRate : 0.0;
  final pv = pvBase + pvSurcharge;
  final rv = gross * profile.rvEmployeeRate;
  final av = gross * profile.avEmployeeRate;

  // Platzhalter-Tax: zunächst 0 (damit ihr nichts Falsches ausgebt)
  // -> Später: Lohnsteuer-Tabellen/ELStAM Engine implementieren.
  final lohnsteuer = 0.0;
  final soli = 0.0;
  final kirche = 0.0;

  final deductions = <String, double>{
    'KV': round2(kv),
    'PV': round2(pv),
    'RV': round2(rv),
    'AV': round2(av),
  };

  final taxes = <String, double>{
    'Lohnsteuer': round2(lohnsteuer),
    if (profile.soli) 'Soli': round2(soli),
    if (profile.churchTax) 'Kirchensteuer': round2(kirche),
  };

  final totalDed = deductions.values.fold<double>(0.0, (a, b) => a + b) +
      taxes.values.fold<double>(0.0, (a, b) => a + b);

  final net = round2(gross - totalDed);

  return PayrollResult(gross: round2(gross), net: net, deductions: deductions, taxes: taxes);
}


// ===================== Arbeit (Kalender & Stunden) =====================

int _todToMinutes(TimeOfDay t) => t.hour * 60 + t.minute;

String _minutesToHm(int minutes) {
  final h = minutes ~/ 60;
  final m = minutes % 60;
  return '${h.toString().padLeft(2, '0')}:${m.toString().padLeft(2, '0')}';
}

double _minutesToDecimalHours(int minutes) => minutes / 60.0;

Future<TimeOfDay?> _pickTimeWithStep(
  BuildContext context, {
  required TimeOfDay initialTime,
  required int minuteStep,
}) async {
  int selHour = initialTime.hour;
  int selMin = (initialTime.minute ~/ minuteStep) * minuteStep;

  final minutes = <int>[];
  for (int m = 0; m < 60; m += minuteStep) {
    minutes.add(m);
  }
  if (!minutes.contains(selMin)) selMin = minutes.first;

  return showModalBottomSheet<TimeOfDay>(
    context: context,
    isScrollControlled: true,
    builder: (ctx) {
      return SafeArea(
        child: Padding(
          padding: const EdgeInsets.fromLTRB(16, 16, 16, 16),
          child: Column(
            mainAxisSize: MainAxisSize.min,
            children: [
              const Text('Uhrzeit wählen', style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold)),
              const SizedBox(height: 12),
              Row(
                children: [
                  Expanded(
                    child: DropdownButtonFormField<int>(
                      value: selHour,
                      decoration: const InputDecoration(labelText: 'Stunde', border: OutlineInputBorder()),
                      items: List.generate(24, (i) => i)
                          .map((h) => DropdownMenuItem(value: h, child: Text(h.toString().padLeft(2, '0'))))
                          .toList(),
                      onChanged: (v) => selHour = v ?? selHour,
                    ),
                  ),
                  const SizedBox(width: 12),
                  Expanded(
                    child: DropdownButtonFormField<int>(
                      value: selMin,
                      decoration: const InputDecoration(labelText: 'Minute', border: OutlineInputBorder()),
                      items: minutes
                          .map((m) => DropdownMenuItem(value: m, child: Text(m.toString().padLeft(2, '0'))))
                          .toList(),
                      onChanged: (v) => selMin = v ?? selMin,
                    ),
                  ),
                ],
              ),
              const SizedBox(height: 16),
              Row(
                children: [
                  Expanded(
                    child: OutlinedButton(
                      onPressed: () => Navigator.of(ctx).pop(null),
                      child: const Text('Abbrechen'),
                    ),
                  ),
                  const SizedBox(width: 12),
                  Expanded(
                    child: ElevatedButton(
                      onPressed: () => Navigator.of(ctx).pop(TimeOfDay(hour: selHour, minute: selMin)),
                      child: const Text('Übernehmen'),
                    ),
                  ),
                ],
              ),
            ],
          ),
        ),
      );
    },
  );
}

class WorkTab extends StatefulWidget {
  const WorkTab({super.key, required this.famRef});

  final DocumentReference<Map<String, dynamic>> famRef;

  @override
  State<WorkTab> createState() => _WorkTabState();
}

class _WorkTabState extends State<WorkTab> {
  DateTime _selectedDay = DateTime.now();
  int _minuteStep = 15;

  TimeOfDay _start = const TimeOfDay(hour: 8, minute: 0);
  TimeOfDay _end = const TimeOfDay(hour: 16, minute: 0);
  final _breakC = TextEditingController(text: '30');
  final _hourlyC = TextEditingController(text: '0');

  // Manuelle Messung (Start/Stop)
  bool _timerRunning = false;
  DateTime? _timerStartedAt;
  Timer? _tick;

  bool _saving = false;

  String get _dateId => yyyymmdd(_selectedDay);

  CollectionReference<Map<String, dynamic>> _daysRef(String uid) =>
      widget.famRef.collection('work').doc(uid).collection('days');

  @override
  void initState() {
    super.initState();
    // Refresh UI while timer is running (so elapsed time updates)
    _tick = Timer.periodic(const Duration(seconds: 20), (_) {
      if (mounted) setState(() {});
    });
  }

  TimeOfDay _nowTodFloorStep() {
    final now = DateTime.now();
    final step = (_minuteStep == 5 || _minuteStep == 15) ? _minuteStep : 15;
    final flooredMin = (now.minute ~/ step) * step;
    return TimeOfDay(hour: now.hour, minute: flooredMin);
  }

  Future<void> _startStopwatch(String uid) async {
    final now = DateTime.now();
    final tod = _nowTodFloorStep();
    final startedAt = DateTime(now.year, now.month, now.day, tod.hour, tod.minute);

    setState(() {
      _timerRunning = true;
      _timerStartedAt = startedAt;
      _start = tod;
      // Don't auto-change end here; user may stop later.
    });

    await _daysRef(uid).doc(_dateId).set({
      'date': Timestamp.fromDate(DateTime(_selectedDay.year, _selectedDay.month, _selectedDay.day)),
      'startMin': _todToMinutes(tod),
      'timerRunning': true,
      'timerStartedAt': Timestamp.fromDate(startedAt),
      'updatedAt': FieldValue.serverTimestamp(),
    }, SetOptions(merge: true));
  }

  Future<void> _stopStopwatch(String uid) async {
    final now = DateTime.now();
    final tod = _nowTodFloorStep();
    setState(() {
      _timerRunning = false;
      _end = tod;
    });

    // Save times + computed values
    await _daysRef(uid).doc(_dateId).set({
      'endMin': _todToMinutes(tod),
      'timerRunning': false,
      'timerStoppedAt': FieldValue.serverTimestamp(),
      'updatedAt': FieldValue.serverTimestamp(),
    }, SetOptions(merge: true));

    await _saveDay(uid);
  }

  int _computeWorkedMinutes() {
    final s = _todToMinutes(_start);
    final e = _todToMinutes(_end);
    int diff = e - s;
    if (diff < 0) diff += 24 * 60; // über Mitternacht
    final pause = int.tryParse(_breakC.text.trim()) ?? 0;
    diff -= pause;
    if (diff < 0) diff = 0;
    return diff;
  }

  Future<void> _pickDay(String uid) async {
    final d = await showDatePicker(
      context: context,
      initialDate: _selectedDay,
      firstDate: DateTime(2000),
      lastDate: DateTime(2100),
    );
    if (d == null) return;

    setState(() => _selectedDay = d);

    // Wenn für den Tag schon Daten existieren: übernehmen.
    // Sonst direkt "Uhr-Dialog" öffnen (Start/Ende) und Werte übernehmen.
    try {
      final snap = await _daysRef(uid).doc(_dateId).get();
      if (snap.exists) {
        final data = snap.data();
        if (data != null) _applyFromDoc(data);
        return;
      }
    } catch (_) {
      // Ignorieren, wir fragen dann einfach nach Zeiten.
    }

    final start = await _pickTimeWithStep(context, initialTime: _start, minuteStep: _minuteStep);
    if (start == null) return;
    final end = await _pickTimeWithStep(context, initialTime: _end, minuteStep: _minuteStep);
    if (end == null) {
      setState(() => _start = start);
      return;
    }

    setState(() {
      _start = start;
      _end = end;
    });
  }

  Future<void> _pickStart() async {
    final t = await _pickTimeWithStep(context, initialTime: _start, minuteStep: _minuteStep);
    if (t != null) setState(() => _start = t);
  }

  Future<void> _pickEnd() async {
    final t = await _pickTimeWithStep(context, initialTime: _end, minuteStep: _minuteStep);
    if (t != null) setState(() => _end = t);
  }

  Future<void> _saveDay(String uid) async {
    setState(() => _saving = true);
    try {
      final workedMin = _computeWorkedMinutes();
      final hours = _minutesToDecimalHours(workedMin);
      final rate = double.tryParse(_hourlyC.text.replaceAll(',', '.').trim()) ?? 0.0;
      final gross = hours * rate;

      await _daysRef(uid).doc(_dateId).set({
        'date': Timestamp.fromDate(DateTime(_selectedDay.year, _selectedDay.month, _selectedDay.day)),
        'startMin': _todToMinutes(_start),
        'endMin': _todToMinutes(_end),
        'breakMin': int.tryParse(_breakC.text.trim()) ?? 0,
        'minuteStep': _minuteStep,
        'hourlyRate': rate,
        'workedMin': workedMin,
        'workedHours': hours,
        'gross': gross,
        'timerRunning': _timerRunning,
        if (_timerStartedAt != null) 'timerStartedAt': Timestamp.fromDate(_timerStartedAt!),
        'updatedAt': FieldValue.serverTimestamp(),
      }, SetOptions(merge: true));

      if (!mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(const SnackBar(content: Text('Arbeitstag gespeichert.')));
    } finally {
      if (mounted) setState(() => _saving = false);
    }
  }

  void _applyFromDoc(Map<String, dynamic> m) {
    final s = (m['startMin'] ?? 480) as int;
    final e = (m['endMin'] ?? 960) as int;
    final b = (m['breakMin'] ?? 30) as int;
    final step = (m['minuteStep'] ?? 15) as int;
    final rate = (m['hourlyRate'] ?? 0.0);

    final running = (m['timerRunning'] == true);
    final startedAt = tsToDate(m['timerStartedAt']);

    setState(() {
      _timerRunning = running;
      _timerStartedAt = startedAt;
      _minuteStep = (step == 5 || step == 15) ? step : 15;
      _start = TimeOfDay(hour: (s ~/ 60) % 24, minute: s % 60);
      _end = TimeOfDay(hour: (e ~/ 60) % 24, minute: e % 60);
      _breakC.text = '$b';
      _hourlyC.text = '${(rate is num) ? rate.toDouble() : double.tryParse(rate.toString()) ?? 0.0}';
    });
  }

  @override
  void dispose() {
    _tick?.cancel();
    _breakC.dispose();
    _hourlyC.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final uid = FirebaseAuth.instance.currentUser?.uid ?? '';
    if (uid.isEmpty) {
      return const Center(child: Text('Nicht eingeloggt.'));
    }

    final workedMin = _computeWorkedMinutes();
    final workedHours = _minutesToDecimalHours(workedMin);
    final rate = double.tryParse(_hourlyC.text.replaceAll(',', '.').trim()) ?? 0.0;
    final gross = workedHours * rate;

    return Scaffold(
      body: ListView(
        padding: const EdgeInsets.all(16),
        children: [
          Row(
            children: [
              const Text('Arbeit', style: TextStyle(fontSize: 20, fontWeight: FontWeight.bold)),
              const Spacer(),
              OutlinedButton.icon(
                onPressed: () => _pickDay(uid),
                icon: const Icon(Icons.calendar_month),
                label: Text('${_selectedDay.day.toString().padLeft(2, '0')}.${_selectedDay.month.toString().padLeft(2, '0')}.${_selectedDay.year}'),
              ),
            ],
          ),
          const SizedBox(height: 12),

          StreamBuilder<DocumentSnapshot<Map<String, dynamic>>>(
            stream: _daysRef(uid).doc(_dateId).snapshots(),
            builder: (context, snap) {
              final m = snap.data?.data();
              return Card(
                child: Padding(
                  padding: const EdgeInsets.all(12),
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      Row(
                        children: [
                          const Text('Schrittweite:', style: TextStyle(fontWeight: FontWeight.bold)),
                          const SizedBox(width: 10),
                          ChoiceChip(
                            label: const Text('15 Min'),
                            selected: _minuteStep == 15,
                            onSelected: (v) => setState(() => _minuteStep = 15),
                          ),
                          const SizedBox(width: 8),
                          ChoiceChip(
                            label: const Text('5 Min'),
                            selected: _minuteStep == 5,
                            onSelected: (v) => setState(() => _minuteStep = 5),
                          ),
                          const Spacer(),
                          if (m != null)
                            TextButton.icon(
                              onPressed: () => _applyFromDoc(m),
                              icon: const Icon(Icons.download),
                              label: const Text('Laden'),
                            ),
                        ],
                      ),
                      const SizedBox(height: 8),

                      // Manuelle Messung: Start/Stop
                      Builder(builder: (context) {
                        final running = (m?['timerRunning'] == true) || _timerRunning;
                        final startedAt = tsToDate(m?['timerStartedAt']) ?? _timerStartedAt;
                        final now = DateTime.now();
                        final elapsedMin = (running && startedAt != null)
                            ? max(0, now.difference(startedAt).inMinutes)
                            : 0;
                        final elapsedTxt = _minutesToHm(elapsedMin);

                        return Card(
                          child: Padding(
                            padding: const EdgeInsets.all(12),
                            child: Row(
                              children: [
                                Expanded(
                                  child: Column(
                                    crossAxisAlignment: CrossAxisAlignment.start,
                                    children: [
                                      const Text('Messung', style: TextStyle(fontWeight: FontWeight.bold)),
                                      const SizedBox(height: 4),
                                      Text(running && startedAt != null
                                          ? 'Läuft seit ${startedAt.hour.toString().padLeft(2, '0')}:${startedAt.minute.toString().padLeft(2, '0')} • $elapsedTxt'
                                          : 'Nicht aktiv'),
                                    ],
                                  ),
                                ),
                                const SizedBox(width: 12),
                                FilledButton.icon(
                                  onPressed: running ? null : () => _startStopwatch(uid),
                                  icon: const Icon(Icons.play_arrow),
                                  label: const Text('Start'),
                                ),
                                const SizedBox(width: 8),
                                FilledButton.tonalIcon(
                                  onPressed: running ? () => _stopStopwatch(uid) : null,
                                  icon: const Icon(Icons.stop),
                                  label: const Text('Stop'),
                                ),
                              ],
                            ),
                          ),
                        );
                      }),

                      const SizedBox(height: 8),
                      Row(
                        children: [
                          Expanded(
                            child: InkWell(
                              onTap: _pickStart,
                              child: InputDecorator(
                                decoration: const InputDecoration(labelText: 'Start', border: OutlineInputBorder()),
                                child: Text(_minutesToHm(_todToMinutes(_start))),
                              ),
                            ),
                          ),
                          const SizedBox(width: 12),
                          Expanded(
                            child: InkWell(
                              onTap: _pickEnd,
                              child: InputDecorator(
                                decoration: const InputDecoration(labelText: 'Ende', border: OutlineInputBorder()),
                                child: Text(_minutesToHm(_todToMinutes(_end))),
                              ),
                            ),
                          ),
                        ],
                      ),
                      const SizedBox(height: 12),
                      Row(
                        children: [
                          Expanded(
                            child: TextField(
                              controller: _breakC,
                              keyboardType: TextInputType.number,
                              decoration: const InputDecoration(labelText: 'Pause (Min)', border: OutlineInputBorder()),
                            ),
                          ),
                          const SizedBox(width: 12),
                          Expanded(
                            child: TextField(
                              controller: _hourlyC,
                              keyboardType: const TextInputType.numberWithOptions(decimal: true),
                              decoration: const InputDecoration(labelText: 'Stundenlohn (€)', border: OutlineInputBorder()),
                            ),
                          ),
                        ],
                      ),
                      const SizedBox(height: 12),
                      Container(
                        padding: const EdgeInsets.all(12),
                        decoration: BoxDecoration(
                          borderRadius: BorderRadius.circular(12),
                          border: Border.all(color: Theme.of(context).dividerColor),
                        ),
                        child: Row(
                          children: [
                            Expanded(child: Text('Arbeitszeit: ${_minutesToHm(workedMin)} (${workedHours.toStringAsFixed(2)} h)')),
                            Expanded(child: Text('Brutto: ${gross.toStringAsFixed(2)} €', textAlign: TextAlign.end)),
                          ],
                        ),
                      ),
                      const SizedBox(height: 12),
                      SizedBox(
                        width: double.infinity,
                        child: ElevatedButton.icon(
                          onPressed: _saving ? null : () => _saveDay(uid),
                          icon: _saving ? const SizedBox(width: 16, height: 16, child: CircularProgressIndicator(strokeWidth: 2)) : const Icon(Icons.save),
                          label: const Text('Speichern'),
                        ),
                      ),
                    ],
                  ),
                ),
              );
            },
          ),

          const SizedBox(height: 16),
          const Text('Tage im Monat (Kurz)', style: TextStyle(fontWeight: FontWeight.bold)),
          const SizedBox(height: 8),
          StreamBuilder<QuerySnapshot<Map<String, dynamic>>>(
            stream: _daysRef(uid)
                .where('date', isGreaterThanOrEqualTo: Timestamp.fromDate(DateTime(_selectedDay.year, _selectedDay.month, 1)))
                .where('date', isLessThan: Timestamp.fromDate(DateTime(_selectedDay.year, _selectedDay.month + 1, 1)))
                .orderBy('date')
                .snapshots(),
            builder: (context, snap) {
              final docs = snap.data?.docs ?? const [];
              if (docs.isEmpty) return const Text('Noch keine Einträge in diesem Monat.');
              int sumMin = 0;
              double sumGross = 0;
              for (final d in docs) {
                final m = d.data();
                sumMin += (m['workedMin'] ?? 0) as int;
                sumGross += _asDouble(m['gross']);
              }
              return Column(
                children: [
                  ...docs.map((d) {
                    final m = d.data();
                    final dt = (m['date'] as Timestamp?)?.toDate() ?? DateTime.now();
                    final label = '${dt.day.toString().padLeft(2, '0')}.${dt.month.toString().padLeft(2, '0')}';
                    final wm = (m['workedMin'] ?? 0) as int;
                    final g = _asDouble(m['gross']);
                    return ListTile(
                      dense: true,
                      leading: const Icon(Icons.work_outline),
                      title: Text(label),
                      subtitle: Text('Zeit: ${_minutesToHm(wm)}'),
                      trailing: Text('${g.toStringAsFixed(2)} €'),
                      onTap: () => setState(() => _selectedDay = DateTime(dt.year, dt.month, dt.day)),
                    );
                  }),
                  const Divider(),
                  ListTile(
                    dense: true,
                    leading: const Icon(Icons.summarize),
                    title: const Text('Monatssumme'),
                    subtitle: Text('Zeit: ${_minutesToHm(sumMin)} (${_minutesToDecimalHours(sumMin).toStringAsFixed(2)} h)'),
                    trailing: Text('${sumGross.toStringAsFixed(2)} €'),
                  ),
                ],
              );
            },
          ),
        ],
      ),
    );
  }
}


class TaxesTab extends StatefulWidget {
  const TaxesTab({super.key, required this.famRef});

  final DocumentReference<Map<String, dynamic>> famRef;

  @override
  State<TaxesTab> createState() => _TaxesTabState();
}

class _TaxesTabState extends State<TaxesTab> {
  final _formKey = GlobalKey<FormState>();

  // Minimaler Standard-Profil-Editor (1 Profil). Du kannst später mehrere Profile hinzufügen.
  final _nameC = TextEditingController(text: 'Standard');
  final _yearC = TextEditingController(text: '${DateTime.now().year}');
  String? _selectedState = 'Berlin';
  String _taxClass = 'I';
  bool _churchTax = false;
  final _churchRateC = TextEditingController(text: '0.09');
  bool _soli = true;

  final _kvC = TextEditingController(text: '0.081');
  final _pvC = TextEditingController(text: '0.017');
  final _rvC = TextEditingController(text: '0.093');
  final _avC = TextEditingController(text: '0.013');

  bool _pvChildless = false;
  final _pvChildlessRateC = TextEditingController(text: '0.006');

  final _pkvFixedC = TextEditingController(text: '0');

  bool _saving = false;

  DocumentReference<Map<String, dynamic>> get _profileRef => widget.famRef.collection('taxProfiles').doc('default');

  @override
  void dispose() {
    _nameC.dispose();
    _yearC.dispose();
    _churchRateC.dispose();
    _kvC.dispose();
    _pvC.dispose();
    _rvC.dispose();
    _avC.dispose();
    _pvChildlessRateC.dispose();
    _pkvFixedC.dispose();
    super.dispose();
  }

  double _d(TextEditingController c, double def) => double.tryParse(c.text.replaceAll(',', '.')) ?? def;
  int _i(TextEditingController c, int def) => int.tryParse(c.text.trim()) ?? def;

  void _applyDefaultsFromTable() {
    final year = _i(_yearC, DateTime.now().year);
    final st = (_selectedState ?? 'Berlin');
    final d = kSvEmployeeDefaultsByYear[year] ?? kSvEmployeeDefaultsByYear.values.first;

    setState(() {
      _kvC.text = '${d['kv'] ?? 0.081}';
      _pvC.text = '${d['pv'] ?? 0.017}';
      _rvC.text = '${d['rv'] ?? 0.093}';
      _avC.text = '${d['av'] ?? 0.013}';
      _pvChildlessRateC.text = '${d['pvChildless'] ?? 0.006}';
      _churchRateC.text = '${_defaultChurchRateForState(st)}';
    });
  }


  Future<void> _load() async {
    final snap = await _profileRef.get();
    final m = snap.data();
    if (!mounted || m == null) return;

    setState(() {
      _nameC.text = (m['name'] ?? 'Standard').toString();
      _yearC.text = '${m['year'] ?? DateTime.now().year}';
      _selectedState = (m['state'] ?? 'Berlin').toString();
      _taxClass = (m['taxClass'] ?? 'I').toString();
      _churchTax = (m['churchTax'] ?? false) == true;
      _churchRateC.text = '${m['churchTaxRate'] ?? 0.09}';
      _soli = (m['soli'] ?? true) == true;

      _kvC.text = '${m['kvEmployeeRate'] ?? 0.081}';
      _pvC.text = '${m['pvEmployeeRate'] ?? 0.017}';
      _rvC.text = '${m['rvEmployeeRate'] ?? 0.093}';
      _avC.text = '${m['avEmployeeRate'] ?? 0.013}';

      _pvChildless = (m['pvChildlessSurcharge'] ?? false) == true;
      _pvChildlessRateC.text = '${m['pvChildlessSurchargeRate'] ?? 0.006}';
      _pkvFixedC.text = '${m['fixedPkV'] ?? 0.0}';
    });
  }

  Future<void> _save() async {
    if (!_formKey.currentState!.validate()) return;
    setState(() => _saving = true);
    try {
      final p = TaxProfile(
        id: 'default',
        name: _nameC.text.trim().isEmpty ? 'Standard' : _nameC.text.trim(),
        year: _i(_yearC, DateTime.now().year),
        state: (_selectedState ?? 'Berlin').trim().isEmpty ? 'Berlin' : (_selectedState ?? 'Berlin').trim(),
        taxClass: _taxClass,
        churchTax: _churchTax,
        churchTaxRate: _d(_churchRateC, 0.09),
        soli: _soli,
        kvEmployeeRate: _d(_kvC, 0.081),
        pvEmployeeRate: _d(_pvC, 0.017),
        rvEmployeeRate: _d(_rvC, 0.093),
        avEmployeeRate: _d(_avC, 0.013),
        pvChildlessSurcharge: _pvChildless,
        pvChildlessSurchargeRate: _d(_pvChildlessRateC, 0.006),
        fixedPkV: _d(_pkvFixedC, 0.0),
      );

      await _profileRef.set(p.toMap(), SetOptions(merge: true));

      if (!mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(const SnackBar(content: Text('Steuerprofil gespeichert.')));
    } finally {
      if (mounted) setState(() => _saving = false);
    }
  }

  @override
  void initState() {
    super.initState();
    _load();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      body: ListView(
        padding: const EdgeInsets.all(16),
        children: [
          const Text('Steuern & Abgaben', style: TextStyle(fontSize: 20, fontWeight: FontWeight.bold)),
          const SizedBox(height: 8),
          const Text(
            'Hier trägst du dein Steuer-/SV-Profil ein. Dieses Profil kann in „Zahlungen“ für Gehalt (Brutto→Netto) genutzt werden.\n'
            'Hinweis: In V1 berechnen wir Sozialabgaben aus den Raten. Lohnsteuer/Soli/Kirchensteuer sind als Platzhalter vorgesehen und kommen als nächster Schritt tabellengenau.',
          ),
          const SizedBox(height: 16),

          Card(
            child: Padding(
              padding: const EdgeInsets.all(12),
              child: Row(
                children: [
                  const Expanded(child: Text('Standardwerte (DE) für SV/Kirchensteuer laden')),
                  const SizedBox(width: 12),
                  ElevatedButton.icon(
                    onPressed: _saving ? null : _applyDefaultsFromTable,
                    icon: const Icon(Icons.auto_fix_high),
                    label: const Text('Laden'),
                  ),
                ],
              ),
            ),
          ),
          const SizedBox(height: 8),

          Form(
            key: _formKey,
            child: Column(
              children: [
                TextFormField(
                  controller: _nameC,
                  decoration: const InputDecoration(labelText: 'Profilname'),
                ),
                const SizedBox(height: 8),
                Row(
                  children: [
                    Expanded(
                      child: TextFormField(
                        controller: _yearC,
                        keyboardType: TextInputType.number,
                        decoration: const InputDecoration(labelText: 'Jahr'),
                        validator: (v) => (int.tryParse((v ?? '').trim()) == null) ? 'Bitte Jahr eingeben' : null,
                      ),
                    ),
                    const SizedBox(width: 8),
                    Expanded(
                      child: DropdownButtonFormField<String>(
                        isExpanded: true,
                        value: _selectedState != null && bundeslaender.contains(_selectedState) ? _selectedState : 'Berlin',
                        decoration: const InputDecoration(labelText: 'Bundesland'),
                        items: bundeslaender
                            .map((st) => DropdownMenuItem<String>(
                                  value: st,
                                  child: Text(
                                    st,
                                    maxLines: 1,
                                    overflow: TextOverflow.ellipsis,
                                  ),
                                ))
                            .toList(),
                        onChanged: (v) => setState(() => _selectedState = v),
                        validator: (v) => (v == null || v.trim().isEmpty) ? 'Pflichtfeld' : null,
                      ),
                    ),
                  ],
                ),
                const SizedBox(height: 8),
                DropdownButtonFormField<String>(
                  value: _taxClass,
                  items: const [
                    DropdownMenuItem(value: 'I', child: Text('Steuerklasse I')),
                    DropdownMenuItem(value: 'II', child: Text('Steuerklasse II')),
                    DropdownMenuItem(value: 'III', child: Text('Steuerklasse III')),
                    DropdownMenuItem(value: 'IV', child: Text('Steuerklasse IV')),
                    DropdownMenuItem(value: 'V', child: Text('Steuerklasse V')),
                    DropdownMenuItem(value: 'VI', child: Text('Steuerklasse VI')),
                  ],
                  onChanged: (v) => setState(() => _taxClass = v ?? 'I'),
                  decoration: const InputDecoration(labelText: 'Steuerklasse'),
                ),
                const SizedBox(height: 8),
                SwitchListTile(
                  value: _churchTax,
                  onChanged: (v) => setState(() => _churchTax = v),
                  title: const Text('Kirchensteuerpflichtig'),
                ),
                if (_churchTax)
                  TextFormField(
                    controller: _churchRateC,
                    keyboardType: const TextInputType.numberWithOptions(decimal: true),
                    decoration: const InputDecoration(labelText: 'Kirchensteuersatz (0.08 oder 0.09)'),
                    validator: (v) => (double.tryParse((v ?? '').replaceAll(',', '.')) == null) ? 'Zahl eingeben' : null,
                  ),
                SwitchListTile(
                  value: _soli,
                  onChanged: (v) => setState(() => _soli = v),
                  title: const Text('Solidaritätszuschlag berücksichtigen (später tabellengenau)'),
                ),
                const Divider(height: 24),
                const Align(
                  alignment: Alignment.centerLeft,
                  child: Text('SV-Raten (Arbeitnehmer)', style: TextStyle(fontWeight: FontWeight.bold)),
                ),
                const SizedBox(height: 8),
                TextFormField(
                  controller: _kvC,
                  keyboardType: const TextInputType.numberWithOptions(decimal: true),
                  decoration: const InputDecoration(labelText: 'KV-Rate (AN, z.B. 0.081)'),
                  validator: (v) => (double.tryParse((v ?? '').replaceAll(',', '.')) == null) ? 'Zahl eingeben' : null,
                ),
                TextFormField(
                  controller: _pvC,
                  keyboardType: const TextInputType.numberWithOptions(decimal: true),
                  decoration: const InputDecoration(labelText: 'PV-Rate (AN, z.B. 0.017)'),
                  validator: (v) => (double.tryParse((v ?? '').replaceAll(',', '.')) == null) ? 'Zahl eingeben' : null,
                ),
                SwitchListTile(
                  value: _pvChildless,
                  onChanged: (v) => setState(() => _pvChildless = v),
                  title: const Text('PV-Zuschlag kinderlos > 23'),
                ),
                if (_pvChildless)
                  TextFormField(
                    controller: _pvChildlessRateC,
                    keyboardType: const TextInputType.numberWithOptions(decimal: true),
                    decoration: const InputDecoration(labelText: 'PV-Zuschlag-Rate (z.B. 0.006)'),
                    validator: (v) => (double.tryParse((v ?? '').replaceAll(',', '.')) == null) ? 'Zahl eingeben' : null,
                  ),
                TextFormField(
                  controller: _rvC,
                  keyboardType: const TextInputType.numberWithOptions(decimal: true),
                  decoration: const InputDecoration(labelText: 'RV-Rate (AN, z.B. 0.093)'),
                  validator: (v) => (double.tryParse((v ?? '').replaceAll(',', '.')) == null) ? 'Zahl eingeben' : null,
                ),
                TextFormField(
                  controller: _avC,
                  keyboardType: const TextInputType.numberWithOptions(decimal: true),
                  decoration: const InputDecoration(labelText: 'AV-Rate (AN, z.B. 0.013)'),
                  validator: (v) => (double.tryParse((v ?? '').replaceAll(',', '.')) == null) ? 'Zahl eingeben' : null,
                ),
                const SizedBox(height: 8),
                TextFormField(
                  controller: _pkvFixedC,
                  keyboardType: const TextInputType.numberWithOptions(decimal: true),
                  decoration: const InputDecoration(labelText: 'PKV fixer AN-Betrag (0 = aus)'),
                  validator: (v) => (double.tryParse((v ?? '').replaceAll(',', '.')) == null) ? 'Zahl eingeben' : null,
                ),
              ],
            ),
          ),

          const SizedBox(height: 16),
          FilledButton.icon(
            onPressed: _saving ? null : _save,
            icon: const Icon(Icons.save),
            label: Text(_saving ? 'Speichere…' : 'Speichern'),
          ),
        ],
      ),
    );
  }
}

class SettingsTab extends StatefulWidget {
  const SettingsTab({super.key, required this.famRef, required this.familyId, required this.monthNotifier});

  final DocumentReference<Map<String, dynamic>> famRef;
  final String familyId;

  final ValueNotifier<DateTime> monthNotifier;

  @override
  State<SettingsTab> createState() => _SettingsTabState();
}

class _SettingsTabState extends State<SettingsTab> {
  DateTime get selectedMonth => widget.monthNotifier.value;

  final budgetC = TextEditingController();
  final startBalC = TextEditingController();
  final catNameC = TextEditingController();
  final catLimitC = TextEditingController();
  final newCategoryC = TextEditingController();

  // App Lock (Biometrie + PIN)
  bool _lockLoaded = false;

  // Secure Invite Token (QR) – rotiert ~ jede Minute (single-use)
  Timer? _inviteTimer;
  bool _inviteBusy = false;
  String _inviteToken = '';
  DateTime _inviteExpiresAt = DateTime.fromMillisecondsSinceEpoch(0);

  String _genInviteToken() {
    const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789abcdefghijkmnopqrstuvwxyz';
    final r = Random.secure();
    return List.generate(28, (_) => chars[r.nextInt(chars.length)]).join();
  }

  Future<void> _rotateInviteToken() async {
    if (_inviteBusy) return;
    setState(() => _inviteBusy = true);
    try {
      // Nur wenn FamilyId verfügbar ist
      final famId = widget.familyId;

      final newToken = _genInviteToken();
      final expires = DateTime.now().add(const Duration(seconds: 75));

      // alten Token löschen (best-effort)
      final old = _inviteToken;
      if (old.isNotEmpty) {
        await FirebaseFirestore.instance.collection('inviteTokens').doc(old).delete().catchError((_) {});
      }

      await FirebaseFirestore.instance.collection('inviteTokens').doc(newToken).set({
        'familyId': famId,
        'expiresAt': Timestamp.fromDate(expires),
        'createdAt': FieldValue.serverTimestamp(),
      });

      if (!mounted) return;
      setState(() {
        _inviteToken = newToken;
        _inviteExpiresAt = expires;
      });
    } catch (_) {
      // silent – UI zeigt einfach keinen Token
    } finally {
      if (mounted) setState(() => _inviteBusy = false);
    }
  }

  void _startInviteRotation() {
    _inviteTimer?.cancel();
    // sofort
    _rotateInviteToken();
    _inviteTimer = Timer.periodic(const Duration(seconds: 55), (_) => _rotateInviteToken());
  }


  bool _lockEnabled = false;
  bool _bioPreferred = true;
  bool _pinSet = false;

  @override
  void initState() {
    super.initState();
    _loadLockSettings();
    _startInviteRotation();
  }

  Future<void> _loadLockSettings() async {
    final svc = AppLockService.instance;
    final enabled = await svc.isLockEnabled();
    final bio = await svc.isBiometricsPreferred();
    final hasPin = await svc.hasPin();
    if (!context.mounted) return;
    setState(() {
      _lockLoaded = true;
      _lockEnabled = enabled;
      _bioPreferred = bio;
      _pinSet = hasPin;
    });
  }

  Future<void> _setOrChangePin({required bool change}) async {
    final svc = AppLockService.instance;

    // Wenn PIN schon existiert und wir ändern, erst alten PIN prüfen
    if (change && await svc.hasPin()) {
      final ok = await showDialog<bool>(
        context: context,
        barrierDismissible: false,
        builder: (_) => _PinDialog(
          title: 'Alten PIN bestätigen',
          onVerify: (pin) => svc.verifyPin(pin),
        ),
      );
      if (ok != true) return;
    }

    final newPin = await showDialog<String?>(
      context: context,
      barrierDismissible: false,
      builder: (ctx) => _CreatePinDialog(title: change ? 'Neuen PIN setzen' : 'PIN setzen'),
    );

    if (newPin == null) return;

    await svc.setPin(newPin);
    if (!context.mounted) return;
    setState(() => _pinSet = true);
    ScaffoldMessenger.of(context).showSnackBar(const SnackBar(content: Text('PIN gespeichert.')));
  }

  Future<void> _removePin() async {
    final svc = AppLockService.instance;
    final ok = await showDialog<bool>(
      context: context,
      builder: (ctx) => AlertDialog(
        title: const Text('PIN entfernen?'),
        content: const Text('Dadurch funktioniert das Entsperren per App-PIN nicht mehr.'),
        actions: [
          TextButton(onPressed: () => Navigator.of(ctx).pop(false), child: const Text('Abbrechen')),
          ElevatedButton(onPressed: () => Navigator.of(ctx).pop(true), child: const Text('Entfernen')),
        ],
      ),
    );
    if (ok != true) return;
    await svc.clearPin();
    if (!context.mounted) return;
    setState(() => _pinSet = false);
    ScaffoldMessenger.of(context).showSnackBar(const SnackBar(content: Text('PIN entfernt.')));
  }




  @override
  void dispose() {
    budgetC.dispose();
    startBalC.dispose();
    catNameC.dispose();
    catLimitC.dispose();
    newCategoryC.dispose();
    _inviteTimer?.cancel();
    super.dispose();
  }

  Future<void> _saveBudget() async {
    final v = double.tryParse(budgetC.text.replaceAll(',', '.'));
    if (v == null || v < 0) return;
    await widget.famRef.set({'monthlyBudget': v}, SetOptions(merge: true));
    budgetC.clear();
    if (!context.mounted) return;
    ScaffoldMessenger.of(context).showSnackBar(const SnackBar(content: Text('Budget gespeichert')));
  }
  Future<void> _saveStartBalance() async {
    final v = double.tryParse(startBalC.text.replaceAll(',', '.'));
    if (v == null) return;
    await widget.famRef.set({'startBalance': v}, SetOptions(merge: true));
    if (!context.mounted) return;
    ScaffoldMessenger.of(context).showSnackBar(const SnackBar(content: Text('Startsaldo gespeichert')));
  }

  Future<void> _addOrUpdateCategoryBudget(Map<String, dynamic> existing) async {
    final name = catNameC.text.trim();
    final lim = double.tryParse(catLimitC.text.replaceAll(',', '.'));
    if (name.isEmpty || lim == null || lim < 0) return;

    final next = Map<String, dynamic>.from(existing);
    next[name] = lim;

    await widget.famRef.set({'categoryBudgets': next}, SetOptions(merge: true));
    catNameC.clear();
    catLimitC.clear();
    if (!context.mounted) return;
    ScaffoldMessenger.of(context).showSnackBar(const SnackBar(content: Text('Kategorie-Budget gespeichert')));
  }

  Future<void> _removeCategoryBudget(Map<String, dynamic> existing, String name) async {
    final next = Map<String, dynamic>.from(existing);
    next.remove(name);
    await widget.famRef.set({'categoryBudgets': next}, SetOptions(merge: true));
  }
  Future<void> _addCategory() async {
    final name = newCategoryC.text.trim();
    if (name.isEmpty) return;

    // Prevent duplicates (case-insensitive)
    final q = await widget.famRef.collection('categories').where('nameLower', isEqualTo: name.toLowerCase()).limit(1).get();
    if (q.docs.isNotEmpty) {
      if (!context.mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(const SnackBar(content: Text('Kategorie existiert bereits.')));
      return;
    }

    await widget.famRef.collection('categories').doc().set({
      'name': name,
      'nameLower': name.toLowerCase(),
      'createdAt': Timestamp.fromDate(DateTime.now()),
      'archived': false,
    });

    newCategoryC.clear();
    if (!context.mounted) return;
    ScaffoldMessenger.of(context).showSnackBar(const SnackBar(content: Text('Kategorie hinzugefügt.')));
  }

  Future<void> _deleteCategory(DocumentSnapshot<Map<String, dynamic>> doc) async {
    // Soft-delete (archive) so old tx keep their category text
    await doc.reference.set({'archived': true, 'updatedAt': Timestamp.fromDate(DateTime.now())}, SetOptions(merge: true));
    if (!context.mounted) return;
    ScaffoldMessenger.of(context).showSnackBar(const SnackBar(content: Text('Kategorie archiviert.')));
  }



  Future<void> _addRecurringRuleDialog(BuildContext context) async {
  final titleC = TextEditingController();
  final amountC = TextEditingController(text: '0.00');
  String type = 'expense'; // expense|income
  String category = 'Sonstiges';
  DateTime startDate = DateTime.now();
  String unit = 'monthly'; // monthly|weekly|yearly
  int interval = 1;
  DateTime? endDate;
  int? count;

  // Unterkonto
  String accountId = 'default';

  bool saving = false;
  String? errorMsg;

  final ok = await showDialog<bool>(
    context: context,
    barrierDismissible: false,
    builder: (ctx) => StatefulBuilder(
      builder: (ctx, setS) {
        void close([bool? result]) {
            if (!ctx.mounted) return;
            Navigator.of(ctx).pop(result);
          }

        return AlertDialog(
          title: const Text('Wiederkehrende Zahlung'),
          content: SingleChildScrollView(
            child: Column(
              mainAxisSize: MainAxisSize.min,
              children: [
                if (errorMsg != null) ...[
                  Text(errorMsg!, style: const TextStyle(color: Colors.red)),
                  const SizedBox(height: 8),
                ],

                // Konto
                StreamBuilder<QuerySnapshot<Map<String, dynamic>>>(
                  stream: widget.famRef.collection('accounts').orderBy('name').snapshots(),
                  builder: (c, snap) {
                    final items = <MapEntry<String, String>>[
                      const MapEntry('default', 'Haushalt'),
                    ];
                    if (snap.hasData) {
                      for (final d in snap.data!.docs) {
                        final data = d.data();
                        if (data['archived'] == true) continue;
                        final name = (data['name'] ?? '').toString().trim();
                        if (name.isEmpty) continue;
                        if (d.id == 'default') {
                          items[0] = MapEntry('default', name);
                        } else {
                          items.add(MapEntry(d.id, name));
                        }
                      }
                    }
                    final ids = items.map((e) => e.key).toSet();
                    final effectiveAccountId = ids.contains(accountId) ? accountId : 'default';

                    return DropdownButtonFormField<String>(
                      value: effectiveAccountId,
                      decoration: const InputDecoration(labelText: 'Konto'),
                      items: items.map((e) => DropdownMenuItem(value: e.key, child: Text(e.value))).toList(),
                      onChanged: saving ? null : (v) => setS(() => accountId = v ?? 'default'),
                    );
                  },
                ),
                const SizedBox(height: 8),

                TextField(controller: titleC, decoration: const InputDecoration(labelText: 'Bezeichnung')),
                const SizedBox(height: 8),
                TextField(
                  controller: amountC,
                  keyboardType: const TextInputType.numberWithOptions(decimal: true),
                  decoration: const InputDecoration(labelText: 'Betrag'),
                ),
                const SizedBox(height: 8),

                DropdownButtonFormField<String>(
                  value: type,
                  items: const [
                    DropdownMenuItem(value: 'expense', child: Text('Ausgabe')),
                    DropdownMenuItem(value: 'income', child: Text('Einnahme')),
                  ],
                  onChanged: saving ? null : (v) => setS(() => type = v ?? 'expense'),
                  decoration: const InputDecoration(labelText: 'Typ'),
                ),
                const SizedBox(height: 8),

                // Kategorie aus Firestore
                StreamBuilder<QuerySnapshot<Map<String, dynamic>>>(
                  stream: widget.famRef
                      .collection('categories')
                      .orderBy('nameLower')
                      .snapshots(),
                  builder: (c, snap) {
                    final items = <String>{'Sonstiges'};
                    if (snap.hasError) {
                      debugPrint('Kategorie-Stream Fehler: ${snap.error}');
                    }
                    if (snap.hasData) {
                      for (final d in snap.data!.docs) {
                        final data = d.data();
                        if (data['archived'] == true) continue;
                        final n = (data['name'] ?? '').toString().trim();
                        if (n.isNotEmpty) items.add(n);
                      }
                    }
                    final list = items.toList()..sort();
                    if (!list.contains(category)) category = list.first;

                    return DropdownButtonFormField<String>(
                      value: category,
                      items: list.map((e) => DropdownMenuItem(value: e, child: Text(e))).toList(),
                      onChanged: saving ? null : (v) => setS(() => category = v ?? 'Sonstiges'),
                      decoration: const InputDecoration(labelText: 'Kategorie'),
                    );
                  },
                ),
                const SizedBox(height: 8),

                ListTile(
                  contentPadding: EdgeInsets.zero,
                  title: const Text('Startdatum'),
                  subtitle: Text(startDate.toLocal().toString().split(' ').first),
                  trailing: const Icon(Icons.date_range),
                  onTap: saving
                      ? null
                      : () async {
                          final picked = await showDatePicker(
                            context: ctx,
                            firstDate: DateTime(DateTime.now().year - 1),
                            lastDate: DateTime(DateTime.now().year + 10),
                            initialDate: startDate,
                          );
                          if (picked != null) setS(() => startDate = picked);
                        },
                ),

                const SizedBox(height: 8),
                DropdownButtonFormField<String>(
                  value: unit,
                  items: const [
                    DropdownMenuItem(value: 'monthly', child: Text('Monatlich')),
                    DropdownMenuItem(value: 'weekly', child: Text('Wöchentlich (z.B. 4-wöchentlich)')),
                    DropdownMenuItem(value: 'yearly', child: Text('Jährlich')),
                  ],
                  onChanged: saving ? null : (v) => setS(() => unit = v ?? 'monthly'),
                  decoration: const InputDecoration(labelText: 'Intervall-Typ'),
                ),
                const SizedBox(height: 8),
                TextField(
                  keyboardType: TextInputType.number,
                  decoration: InputDecoration(
                    labelText: unit == 'weekly' ? 'Intervall (4 = 4-wöchentlich)' : 'Intervall (1 = normal)',
                  ),
                  onChanged: saving ? null : (v) => setS(() => interval = int.tryParse(v) ?? 1),
                ),

                const SizedBox(height: 8),
                ListTile(
                  contentPadding: EdgeInsets.zero,
                  title: const Text('Enddatum (optional)'),
                  subtitle: Text(endDate == null ? 'Keins' : endDate!.toLocal().toString().split(' ').first),
                  trailing: const Icon(Icons.event_busy),
                  onTap: saving
                      ? null
                      : () async {
                          final picked = await showDatePicker(
                            context: ctx,
                            firstDate: DateTime.now(),
                            lastDate: DateTime(DateTime.now().year + 20),
                            initialDate: endDate ?? DateTime.now().add(const Duration(days: 30)),
                          );
                          if (picked != null) setS(() => endDate = picked);
                        },
                ),
                const SizedBox(height: 8),

                TextField(
                  keyboardType: TextInputType.number,
                  decoration: const InputDecoration(labelText: 'Stop nach X Mal (optional)'),
                  onChanged: saving ? null : (v) => setS(() => count = v.trim().isEmpty ? null : int.tryParse(v)),
                ),
              ],
            ),
            ),
          actions: [
            TextButton(onPressed: saving ? null : () => close(false), child: const Text('Abbrechen')),
            FilledButton(
              onPressed: saving
                  ? null
                  : () async {
                      final title = titleC.text.trim();
                      final amount = double.tryParse(amountC.text.replaceAll(',', '.')) ?? 0.0;
                      if (title.isEmpty || amount <= 0) {
                        setS(() => errorMsg = 'Bitte Titel und Betrag eingeben.');
                        return;
                      }
                      setS(() {
                        errorMsg = null;
                        saving = true;
                      });

                      try {
                        await widget.famRef.collection('recurring_rules').add({
                          'accountId': accountId,
                          'title': title,
                          'amount': amount,
                          'type': type,
                          'category': category,
                          'startDate': Timestamp.fromDate(DateTime(startDate.year, startDate.month, startDate.day)),
                          'unit': unit,
                          'interval': interval,
                          if (endDate != null) 'endDate': Timestamp.fromDate(DateTime(endDate!.year, endDate!.month, endDate!.day)),
                          if (count != null) 'remainingCount': count,
                          'active': true,
                          'createdAt': FieldValue.serverTimestamp(),
                          'updatedAt': FieldValue.serverTimestamp(),
                        });

                        close(true);
                      } catch (e) {
                        debugPrint('Add recurring rule error: $e');
                        setS(() {
                          errorMsg = 'Speichern fehlgeschlagen: $e';
                          saving = false;
                        });
                      }
                    },
              child: const Text('Speichern'),
            ),
          ],
        );
      },
    ),
  );

  titleC.dispose();
  amountC.dispose();

  if (ok == true && context.mounted) {
    ScaffoldMessenger.of(context)
      ..hideCurrentSnackBar()
      ..showSnackBar(const SnackBar(content: Text('Wiederkehrende Zahlung gespeichert.')));
  }
}


  Future<void> _editRecurringRuleDialog(BuildContext context, DocumentSnapshot<Map<String, dynamic>> ruleDoc) async {
  final r = ruleDoc.data() ?? {};
  final titleC = TextEditingController(text: (r['title'] as String?) ?? '');
  final amountC = TextEditingController(text: ((r['amount'] as num?)?.toDouble() ?? 0.0).toStringAsFixed(2));
  String type = (r['type'] as String?) ?? 'expense';
  String category = (r['category'] as String?) ?? 'Sonstiges';
  DateTime startDate = (r['startDate'] as Timestamp?)?.toDate() ?? DateTime.now();
  String unit = (r['unit'] as String?) ?? ((r['interval'] is String) ? (r['interval'] as String) : null) ?? 'monthly';
  int interval = (r['interval'] is num)
      ? (r['interval'] as num).toInt()
      : ((r['every'] is num) ? (r['every'] as num).toInt() : (int.tryParse((r['every'] ?? '').toString()) ?? 1));
  DateTime? endDate = (r['endDate'] as Timestamp?)?.toDate();
  int? count = (r['remainingCount'] as num?)?.toInt();
  bool active = (r['active'] as bool?) ?? true;

  String accountId = (r['accountId'] ?? 'default').toString().trim();
  if (accountId.isEmpty) accountId = 'default';

  bool saving = false;
  String? errorMsg;

  final ok = await showDialog<bool>(
    context: context,
    barrierDismissible: false,
    builder: (ctx) => StatefulBuilder(
      builder: (ctx, setS) {
        void close([bool? result]) {
            if (!ctx.mounted) return;
            Navigator.of(ctx).pop(result);
          }

        return AlertDialog(
          title: const Text('Wiederkehrende Regel bearbeiten'),
          content: SingleChildScrollView(
            child: Column(
              mainAxisSize: MainAxisSize.min,
              children: [
                if (errorMsg != null) ...[
                  Text(errorMsg!, style: const TextStyle(color: Colors.red)),
                  const SizedBox(height: 8),
                ],

                // Konto
                StreamBuilder<QuerySnapshot<Map<String, dynamic>>>(
                  stream: widget.famRef.collection('accounts').orderBy('name').snapshots(),
                  builder: (c, snap) {
                    final items = <MapEntry<String, String>>[
                      const MapEntry('default', 'Haushalt'),
                    ];
                    if (snap.hasData) {
                      for (final d in snap.data!.docs) {
                        final data = d.data();
                        if (data['archived'] == true) continue;
                        final name = (data['name'] ?? '').toString().trim();
                        if (name.isEmpty) continue;
                        if (d.id == 'default') {
                          items[0] = MapEntry('default', name);
                        } else {
                          items.add(MapEntry(d.id, name));
                        }
                      }
                    }
                    final ids = items.map((e) => e.key).toSet();
                    final effectiveAccountId = ids.contains(accountId) ? accountId : 'default';

                    return DropdownButtonFormField<String>(
                      value: effectiveAccountId,
                      decoration: const InputDecoration(labelText: 'Konto'),
                      items: items.map((e) => DropdownMenuItem(value: e.key, child: Text(e.value))).toList(),
                      onChanged: saving ? null : (v) => setS(() => accountId = v ?? 'default'),
                    );
                  },
                ),
                const SizedBox(height: 8),

                TextField(controller: titleC, decoration: const InputDecoration(labelText: 'Bezeichnung')),
                const SizedBox(height: 8),
                TextField(
                  controller: amountC,
                  keyboardType: const TextInputType.numberWithOptions(decimal: true),
                  decoration: const InputDecoration(labelText: 'Betrag'),
                ),
                const SizedBox(height: 8),
                DropdownButtonFormField<String>(
                  value: type,
                  items: const [
                    DropdownMenuItem(value: 'expense', child: Text('Ausgabe')),
                    DropdownMenuItem(value: 'income', child: Text('Einnahme')),
                  ],
                  onChanged: saving ? null : (v) => setS(() => type = v ?? 'expense'),
                  decoration: const InputDecoration(labelText: 'Typ'),
                ),
                const SizedBox(height: 8),

                StreamBuilder<QuerySnapshot<Map<String, dynamic>>>(
                  stream: widget.famRef.collection('categories').orderBy('nameLower').snapshots(),
                  builder: (c, snap) {
                    final items = <String>{'Sonstiges'};
                    if (snap.hasError) {
                      debugPrint('Kategorie-Stream Fehler: ${snap.error}');
                    }
                    if (snap.hasData) {
                      for (final d in snap.data!.docs) {
                        final data = d.data();
                        if (data['archived'] == true) continue;
                        final n = (data['name'] ?? '').toString().trim();
                        if (n.isNotEmpty) items.add(n);
                      }
                    }
                    final list = items.toList()..sort();
                    if (!list.contains(category)) category = list.first;

                    return DropdownButtonFormField<String>(
                      value: category,
                      items: list.map((e) => DropdownMenuItem(value: e, child: Text(e))).toList(),
                      onChanged: saving ? null : (v) => setS(() => category = v ?? 'Sonstiges'),
                      decoration: const InputDecoration(labelText: 'Kategorie'),
                    );
                  },
                ),
                const SizedBox(height: 8),

                ListTile(
                  contentPadding: EdgeInsets.zero,
                  title: const Text('Startdatum'),
                  subtitle: Text(startDate.toLocal().toString().split(' ').first),
                  trailing: const Icon(Icons.date_range),
                  onTap: saving
                      ? null
                      : () async {
                          final picked = await showDatePicker(
                            context: ctx,
                            firstDate: DateTime(DateTime.now().year - 1),
                            lastDate: DateTime(DateTime.now().year + 20),
                            initialDate: startDate,
                          );
                          if (picked != null) setS(() => startDate = picked);
                        },
                ),

                DropdownButtonFormField<String>(
                  value: unit,
                  items: const [
                    DropdownMenuItem(value: 'monthly', child: Text('Monatlich')),
                    DropdownMenuItem(value: 'weekly', child: Text('Wöchentlich')),
                    DropdownMenuItem(value: 'yearly', child: Text('Jährlich')),
                  ],
                  onChanged: saving ? null : (v) => setS(() => unit = v ?? 'monthly'),
                  decoration: const InputDecoration(labelText: 'Intervall-Typ'),
                ),
                const SizedBox(height: 8),

                TextField(
                  keyboardType: TextInputType.number,
                  decoration: const InputDecoration(labelText: 'Intervall (z.B. 1)'),
                  onChanged: saving ? null : (v) => setS(() => interval = int.tryParse(v) ?? 1),
                ),
                const SizedBox(height: 8),

                ListTile(
                  contentPadding: EdgeInsets.zero,
                  title: const Text('Enddatum (optional)'),
                  subtitle: Text(endDate == null ? 'Keins' : endDate!.toLocal().toString().split(' ').first),
                  trailing: const Icon(Icons.event_busy),
                  onTap: saving
                      ? null
                      : () async {
                          final picked = await showDatePicker(
                            context: ctx,
                            firstDate: DateTime.now(),
                            lastDate: DateTime(DateTime.now().year + 20),
                            initialDate: endDate ?? DateTime.now().add(const Duration(days: 30)),
                          );
                          if (picked != null) setS(() => endDate = picked);
                        },
                ),

                TextField(
                  keyboardType: TextInputType.number,
                  decoration: const InputDecoration(labelText: 'Stop nach X Mal (optional)'),
                  onChanged: saving ? null : (v) => setS(() => count = v.trim().isEmpty ? null : int.tryParse(v)),
                ),
                const SizedBox(height: 8),

                SwitchListTile(
                  contentPadding: EdgeInsets.zero,
                  title: const Text('Aktiv'),
                  value: active,
                  onChanged: saving ? null : (v) => setS(() => active = v),
                ),
              ],
            ),
            ),
          actions: [
            TextButton(onPressed: saving ? null : () => close(false), child: const Text('Abbrechen')),
            FilledButton(
              onPressed: saving
                  ? null
                  : () async {
                      final title = titleC.text.trim();
                      final amount = double.tryParse(amountC.text.replaceAll(',', '.')) ?? 0.0;

                      if (title.isEmpty || amount <= 0) {
                        setS(() => errorMsg = 'Bitte Titel und Betrag eingeben.');
                        return;
                      }

                      setS(() {
                        errorMsg = null;
                        saving = true;
                      });

                      try {
                        await ruleDoc.reference.set({
                          'accountId': accountId,
                          'title': title,
                          'amount': amount,
                          'type': type,
                          'category': category,
                          'startDate': Timestamp.fromDate(DateTime(startDate.year, startDate.month, startDate.day)),
                          'unit': unit,
                          'interval': interval,
                          // legacy for older readers
                          'every': interval,
                          if (endDate != null) 'endDate': Timestamp.fromDate(DateTime(endDate!.year, endDate!.month, endDate!.day)),
                          if (count != null) 'remainingCount': count,
                          'active': active,
                          'updatedAt': FieldValue.serverTimestamp(),
                        }, SetOptions(merge: true));

                        close(true);
                      } catch (e) {
                        debugPrint('Edit recurring rule error: $e');
                        setS(() {
                          errorMsg = 'Speichern fehlgeschlagen: $e';
                          saving = false;
                        });
                      }
                    },
              child: const Text('Speichern'),
            ),
          ],
        );
      },
    ),
  );

  titleC.dispose();
  amountC.dispose();

  if (ok == true && context.mounted) {
    ScaffoldMessenger.of(context)
      ..hideCurrentSnackBar()
      ..showSnackBar(const SnackBar(content: Text('Regel gespeichert.')));
  }
}

  void _openYearReport() {
    Navigator.of(context).push(
      MaterialPageRoute(
        builder: (_) => YearReportScreen(famRef: widget.famRef),
      ),
    );
  }


  Future<void> _syncNow() async {
    try {
      // Force Firestore to reconnect & push pending local writes
      await FirebaseFirestore.instance.enableNetwork();
      await FirebaseFirestore.instance.waitForPendingWrites();
      // Zusätzlich einmal kurz neu laden (triggert auch bei Offline->Online schneller)
      await widget.famRef.get(const GetOptions(source: Source.serverAndCache));
      // Wiederkehrende Zahlungen nachziehen
      await ensureRecurringGeneratedForFamily(famRef: widget.famRef);
      if (!context.mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(const SnackBar(content: Text('Synchronisation gestartet')));
    } catch (e) {
      if (!context.mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(SnackBar(content: Text('Sync fehlgeschlagen: $e')));
    }
  }


  @override
  Widget build(BuildContext context) {
    return StreamBuilder<DocumentSnapshot<Map<String, dynamic>>>(
      stream: widget.famRef.snapshots(),
      builder: (context, snap) {
        final data = snap.data?.data() ?? {};
        final code = data['code'] as String? ?? '--------';
        final budget = (data['monthlyBudget'] is num) ? (data['monthlyBudget'] as num).toDouble() : 0.0;
        final startBalance = (data['startBalance'] is num) ? (data['startBalance'] as num).toDouble() : 0.0;
        final categoryBudgets = (data['categoryBudgets'] is Map) ? Map<String, dynamic>.from(data['categoryBudgets']) : <String, dynamic>{};
        // Controller-Defaults setzen (ohne dauernd Cursor zu springen)
        if (startBalC.text.isEmpty) startBalC.text = startBalance.toStringAsFixed(2);


        // QR enthält nur den Code (simpel)
        final qrPayload = code;

        return Padding(
          padding: const EdgeInsets.all(16),
          child: ListView(
            children: [
              Card(
                child: Padding(
                  padding: const EdgeInsets.all(16),
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      const Text('Monatliches Budget', style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold)),
                      const SizedBox(height: 6),
                      Text('Aktuell: ${budget.toStringAsFixed(2)} €'),
                      const SizedBox(height: 10),
                      TextField(
                        controller: budgetC,
                        keyboardType: TextInputType.number,
                        decoration: const InputDecoration(
                          labelText: 'Neues Budget (z.B. 3000)',
                          border: OutlineInputBorder(),
                        ),
                      ),
                      const SizedBox(height: 10),
                      Align(
                        alignment: Alignment.centerRight,
                        child: ElevatedButton(
                          onPressed: _saveBudget,
                          child: const Text('Speichern'),
                        ),
                      ),
                    ],
                  ),
                ),
              ),
              const SizedBox(height: 16),
              Card(
                child: Padding(
                  padding: const EdgeInsets.all(16),
                  child: ExpansionTile(
                    tilePadding: EdgeInsets.zero,
                    title: const Text(
                      'Alter Familien-Code (optional)',
                      style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold),
                    ),
                    subtitle: const Text(
                      'Falls du ihn noch brauchst: nur als Text (kein QR).',
                      style: TextStyle(color: Colors.black54),
                    ),
                    children: [
                      const SizedBox(height: 8),
                      SelectableText(
                        code,
                        style: const TextStyle(fontSize: 22, fontWeight: FontWeight.w700, letterSpacing: 2),
                      ),
                      const SizedBox(height: 8),
                      Align(
                        alignment: Alignment.centerLeft,
                        child: Text(
                          'Tipp: Besser den sicheren Invite-Token verwenden.',
                          style: const TextStyle(color: Colors.black54),
                        ),
                      ),
                      const SizedBox(height: 6),
                    ],
                  ),
                ),
              ),
              const SizedBox(height: 16),
const SizedBox(height: 16),

              // -------- Sicherer Beitritt (Invite-Token, Single-Use) --------
              Card(
                child: Padding(
                  padding: const EdgeInsets.all(16),
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      const Text('Sicherer Beitritt (Invite-Token)',
                          style: TextStyle(fontSize: 18, fontWeight: FontWeight.w700)),
                      const SizedBox(height: 8),
                      const Text(
                        'Der Token ist schwer zu erraten, läuft schnell ab und wird beim Beitritt gelöscht.',
                        style: TextStyle(color: Colors.black54),
                      ),
                      const SizedBox(height: 12),
                      Row(
                        children: [
                          Expanded(
                            child: SelectableText(
                              _inviteToken.isEmpty ? '—' : _inviteToken,
                              style: const TextStyle(fontSize: 14),
                            ),
                          ),
                          const SizedBox(width: 8),
                          Text(
                            _inviteToken.isEmpty
                                ? ''
                                : '⏱ ${_inviteExpiresAt.difference(DateTime.now()).inSeconds.clamp(0, 9999)}s',
                            style: const TextStyle(color: Colors.black54),
                          ),
                        ],
                      ),
                      const SizedBox(height: 12),
                      if (_inviteToken.isNotEmpty)
                        Center(
                          child: QrImageView(
                            data: _inviteToken,
                            size: 220,
                          ),
                        ),
                      const SizedBox(height: 12),
                      Row(
                        children: [
                          Expanded(
                            child: OutlinedButton.icon(
                              onPressed: _inviteBusy ? null : _rotateInviteToken,
                              icon: const Icon(Icons.refresh),
                              label: Text(_inviteBusy ? 'Bitte warten…' : 'Neuen Token erzeugen'),
                            ),
                          ),
                        ],
                      ),
                    ],
                  ),
                ),
              ),
              
              // -------- Mitglieder --------
              Card(
                child: Padding(
                  padding: const EdgeInsets.all(16),
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      const Text('Mitglieder',
                          style: TextStyle(fontSize: 18, fontWeight: FontWeight.w700)),
                      const SizedBox(height: 10),
                      
StreamBuilder<DocumentSnapshot>(
                        stream: FirebaseFirestore.instance
                            .collection('families')
                            .doc(widget.familyId)
                            .collection('members')
                            .doc(FirebaseAuth.instance.currentUser?.uid ?? '_')
                            .snapshots(),
                        builder: (context, meSnap) {
                          final myUid = FirebaseAuth.instance.currentUser?.uid ?? '';
                          final myData = (meSnap.data?.data() as Map<String, dynamic>?) ?? {};
                          final myRole = (myData['role'] ?? 'member').toString();
                          final isAdmin = myRole == 'admin';

                          return Column(
                            crossAxisAlignment: CrossAxisAlignment.start,
                            children: [
                              // Admin claim (optional)
                              if (myUid.isNotEmpty && !isAdmin)
                                Align(
                                  alignment: Alignment.centerRight,
                                  child: TextButton.icon(
                                    icon: const Icon(Icons.verified_user),
                                    label: const Text('Ich bin der Admin'),
                                    onPressed: () async {
                                      try {
                                        final q = await FirebaseFirestore.instance
                                            .collection('families')
                                            .doc(widget.familyId)
                                            .collection('members')
                                            .where('role', isEqualTo: 'admin')
                                            .limit(1)
                                            .get();

                                        if (q.docs.isNotEmpty) {
                                          if (context.mounted) {
                                            ScaffoldMessenger.of(context).showSnackBar(
                                              const SnackBar(content: Text('Es gibt bereits einen Admin.')),
                                            );
                                          }
                                          return;
                                        }

                                        await FirebaseFirestore.instance
                                            .collection('families')
                                            .doc(widget.familyId)
                                            .collection('members')
                                            .doc(myUid)
                                            .set({'role': 'admin'}, SetOptions(merge: true));

                                        if (context.mounted) {
                                          ScaffoldMessenger.of(context).showSnackBar(
                                            const SnackBar(content: Text('Du bist jetzt Admin.')),
                                          );
                                        }
                                      } catch (e) {
                                        if (context.mounted) {
                                          ScaffoldMessenger.of(context).showSnackBar(
                                            SnackBar(content: Text('Admin setzen fehlgeschlagen: $e')),
                                          );
                                        }
                                      }
                                    },
                                  ),
                                ),

                              StreamBuilder<QuerySnapshot>(
                                stream: FirebaseFirestore.instance
                                    .collection('families')
                                    .doc(widget.familyId)
                                    .collection('members')
                                    .orderBy('joinedAt', descending: false)
                                    .snapshots(),
                                builder: (context, snap) {
                                  if (snap.hasError) {
                                    return Text('Fehler: ${snap.error}');
                                  }
                                  if (!snap.hasData) {
                                    return const Padding(
                                      padding: EdgeInsets.symmetric(vertical: 8),
                                      child: Center(child: CircularProgressIndicator()),
                                    );
                                  }
                                  final docs = snap.data!.docs;
                                  if (docs.isEmpty) {
                                    return const Text('Noch keine Mitglieder.');
                                  }

                                  Future<void> editMember(String memberId, Map<String, dynamic> d) async {
                                    final nameCtrl = TextEditingController(
                                      text: (d['name'] ?? d['displayName'] ?? '').toString(),
                                    );
                                    String? avatarB64 = (d['avatarB64'] as String?);
                                    await showModalBottomSheet(
                                      context: context,
                                      isScrollControlled: true,
                                      showDragHandle: true,
                                      builder: (ctx) {
                                        return Padding(
                                          padding: EdgeInsets.only(
                                            left: 16,
                                            right: 16,
                                            top: 12,
                                            bottom: MediaQuery.of(ctx).viewInsets.bottom + 16,
                                          ),
                                          child: Column(
                                            mainAxisSize: MainAxisSize.min,
                                            crossAxisAlignment: CrossAxisAlignment.start,
                                            children: [
                                              Text(
                                                'Mitglied bearbeiten',
                                                style: Theme.of(ctx).textTheme.titleLarge,
                                              ),
                                              const SizedBox(height: 12),
                                              Row(
                                                children: [
                                                  CircleAvatar(
                                                    radius: 26,
                                                    child: (avatarB64?.isNotEmpty ?? false)
                                                        ? ClipOval(
                                                            child: Image.memory(
                                                              base64Decode(avatarB64!),
                                                              width: 52,
                                                              height: 52,
                                                              fit: BoxFit.cover,
                                                            ),
                                                          )
                                                        : Text(_initials(nameCtrl.text.isEmpty ? memberId : nameCtrl.text)),
                                                  ),
                                                  const SizedBox(width: 12),
                                                  Expanded(
                                                    child: Column(
                                                      crossAxisAlignment: CrossAxisAlignment.start,
                                                      children: [
                                                        FilledButton.icon(
                                                          icon: const Icon(Icons.photo),
                                                          label: const Text('Avatar aus Galerie'),
                                                          onPressed: () async {
                                                            try {
                                                              final picker = ImagePicker();
                                                              final x = await picker.pickImage(
                                                                source: ImageSource.gallery,
                                                                imageQuality: 70,
                                                                maxWidth: 512,
                                                                maxHeight: 512,
                                                              );
                                                              if (x == null) return;
                                                              final bytes = await x.readAsBytes();
                                                              avatarB64 = base64Encode(bytes);
                                                              if (ctx.mounted) Navigator.pop(ctx);
                                                              // reopen to refresh UI quickly
                                                              await editMember(memberId, {...d, 'avatarB64': avatarB64});
                                                            } catch (e) {
                                                              if (ctx.mounted) {
                                                                ScaffoldMessenger.of(ctx).showSnackBar(
                                                                  SnackBar(content: Text('Avatar wählen fehlgeschlagen: $e')),
                                                                );
                                                              }
                                                            }
                                                          },
                                                        ),
                                                        TextButton.icon(
                                                          icon: const Icon(Icons.delete_outline),
                                                          label: const Text('Avatar entfernen'),
                                                          onPressed: () async {
                                                            avatarB64 = null;
                                                            if (ctx.mounted) Navigator.pop(ctx);
                                                            await editMember(memberId, {...d, 'avatarB64': ''});
                                                          },
                                                        ),
                                                      ],
                                                    ),
                                                  ),
                                                ],
                                              ),
                                              const SizedBox(height: 12),
                                              TextField(
                                                controller: nameCtrl,
                                                decoration: const InputDecoration(
                                                  labelText: 'Name',
                                                  border: OutlineInputBorder(),
                                                ),
                                              ),
                                              const SizedBox(height: 12),
                                              Align(
                                                alignment: Alignment.centerRight,
                                                child: FilledButton(
                                                  onPressed: () async {
                                                    try {
                                                      await FirebaseFirestore.instance
                                                          .collection('families')
                                                          .doc(widget.familyId)
                                                          .collection('members')
                                                          .doc(memberId)
                                                          .set({
                                                        'name': nameCtrl.text.trim(),
                                                        if (avatarB64 != null) 'avatarB64': avatarB64,
                                                      }, SetOptions(merge: true));
                                                      if (ctx.mounted) Navigator.pop(ctx);
                                                    } catch (e) {
                                                      if (ctx.mounted) {
                                                        ScaffoldMessenger.of(ctx).showSnackBar(
                                                          SnackBar(content: Text('Speichern fehlgeschlagen: $e')),
                                                        );
                                                      }
                                                    }
                                                  },
                                                  child: const Text('Speichern'),
                                                ),
                                              ),
                                            ],
                                          ),
                                        );
                                      },
                                    );
                                  }

                                  Future<void> deleteMember(String memberId) async {
                                    if (!isAdmin) return;
                                    final ok = await showDialog<bool>(
                                      context: context,
                                      builder: (ctx) => AlertDialog(
                                        title: const Text('Mitglied entfernen?'),
                                        content: const Text('Das Mitglied verliert den Zugriff auf die Familie.'),
                                        actions: [
                                          TextButton(onPressed: () => Navigator.pop(ctx, false), child: const Text('Abbrechen')),
                                          FilledButton(onPressed: () => Navigator.pop(ctx, true), child: const Text('Entfernen')),
                                        ],
                                      ),
                                    );
                                    if (ok != true) return;

                                    await FirebaseFirestore.instance
                                        .collection('families')
                                        .doc(widget.familyId)
                                        .collection('members')
                                        .doc(memberId)
                                        .delete();
                                  }

                                  return ListView.separated(
                                    shrinkWrap: true,
                                    physics: const NeverScrollableScrollPhysics(),
                                    itemCount: docs.length,
                                    separatorBuilder: (_, __) => const Divider(height: 12),
                                    itemBuilder: (_, i) {
                                      final doc = docs[i];
                                      final d = doc.data() as Map<String, dynamic>;
                                      final memberId = doc.id;

                                      final name = (d['name'] ?? d['displayName'] ?? d['uid'] ?? memberId).toString();
                                      final role = (d['role'] ?? 'member').toString();
                                      final joined = _fmtJoinedAt(d['joinedAt']);

                                      final avatarB64 = (d['avatarB64'] as String?) ?? '';
                                      final canEdit = (memberId == myUid) || isAdmin;
                                      final canDelete = isAdmin && memberId != myUid;

                                      return ListTile(
                                        leading: CircleAvatar(
                                          child: (avatarB64.isNotEmpty)
                                              ? ClipOval(
                                                  child: Image.memory(
                                                    base64Decode(avatarB64),
                                                    width: 40,
                                                    height: 40,
                                                    fit: BoxFit.cover,
                                                  ),
                                                )
                                              : Text(_initials(name)),
                                        ),
                                        title: Row(
                                          children: [
                                            Expanded(
                                              child: Text(
                                                name,
                                                style: const TextStyle(fontWeight: FontWeight.w700),
                                              ),
                                            ),
                                            Container(
                                              padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 4),
                                              decoration: BoxDecoration(
                                                borderRadius: BorderRadius.circular(12),
                                                color: role == 'admin' ? Colors.amber.shade200 : Colors.grey.shade200,
                                              ),
                                              child: Text(role == 'admin' ? 'Admin' : 'Member'),
                                            ),
                                          ],
                                        ),
                                        subtitle: joined.isEmpty ? null : Text('Beigetreten: $joined'),
                                        trailing: Row(
                                          mainAxisSize: MainAxisSize.min,
                                          children: [
                                            if (canEdit)
                                              IconButton(
                                                tooltip: 'Bearbeiten',
                                                icon: const Icon(Icons.edit),
                                                onPressed: () => editMember(memberId, d),
                                              ),
                                            if (canDelete)
                                              IconButton(
                                                tooltip: 'Entfernen',
                                                icon: const Icon(Icons.person_remove_alt_1),
                                                onPressed: () => deleteMember(memberId),
                                              ),
                                          ],
                                        ),
                                      );
                                    },
                                  );
                                },
                              ),
                            ],
                          );
                        },
                      ),

                    ],
                  ),
                ),
              ),
              const SizedBox(height: 16),
const SizedBox(height: 16),

              Card(
                child: Padding(
                  padding: const EdgeInsets.all(16),
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      const Text('Manuell synchronisieren', style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold)),
                      const SizedBox(height: 8),
                      const Text(
                        'Wenn du offline warst, kannst du hier die Synchronisation mit deiner Familie manuell anstoßen.',
                        style: TextStyle(color: Colors.black54),
                      ),
                      const SizedBox(height: 12),
                      Align(
                        alignment: Alignment.centerRight,
                        child: ElevatedButton.icon(
                          onPressed: _syncNow,
                          icon: const Icon(Icons.sync),
                          label: const Text('Jetzt synchronisieren'),
                        ),
                      ),
                    ],
                  ),
                ),
              ),

              

              

              // ===== Sicherheit (Biometrie + PIN) =====
              Card(
                child: Padding(
                  padding: const EdgeInsets.all(16),
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      const Text('Sicherheit', style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold)),
                      const SizedBox(height: 8),
                      if (!_lockLoaded) const LinearProgressIndicator(),
                      SwitchListTile(
                        contentPadding: EdgeInsets.zero,
                        title: const Text('App-Schutz aktivieren'),
                        subtitle: const Text('Biometrie bevorzugt, sonst App-PIN.'),
                        value: _lockEnabled,
                        onChanged: (v) async {
                          await AppLockService.instance.setLockEnabled(v);
                          if (!context.mounted) return;
                          setState(() => _lockEnabled = v);
                        },
                      ),
                      SwitchListTile(
                        contentPadding: EdgeInsets.zero,
                        title: const Text('Biometrie bevorzugen'),
                        subtitle: const Text('Wenn verfügbar, Face/Fingerprint zuerst verwenden.'),
                        value: _bioPreferred,
                        onChanged: _lockEnabled
                            ? (v) async {
                                await AppLockService.instance.setBiometricsPreferred(v);
                                if (!context.mounted) return;
                                setState(() => _bioPreferred = v);
                              }
                            : null,
                      ),
                      const Divider(),
                      ListTile(
                        contentPadding: EdgeInsets.zero,
                        leading: const Icon(Icons.pin),
                        title: Text(_pinSet ? 'PIN ändern' : 'PIN setzen'),
                        subtitle: Text(_pinSet ? 'Ändert deinen App-PIN (4–8 Ziffern).' : 'Erforderlich, wenn keine Biometrie verfügbar ist.'),
                        onTap: _lockEnabled
                            ? () => _setOrChangePin(change: _pinSet)
                            : null,
                      ),
                      if (_pinSet)
                        ListTile(
                          contentPadding: EdgeInsets.zero,
                          leading: const Icon(Icons.delete_forever),
                          title: const Text('PIN entfernen'),
                          onTap: _lockEnabled ? _removePin : null,
                        ),
                      const SizedBox(height: 6),
                      FutureBuilder<bool>(
                        future: AppLockService.instance.canUseBiometrics(),
                        builder: (context, s) {
                          final can = s.data ?? false;
                          return Text(
                            can ? 'Biometrie ist auf diesem Gerät verfügbar.' : 'Biometrie nicht verfügbar (oder nicht eingerichtet).',
                            style: TextStyle(color: Theme.of(context).hintColor),
                          );
                        },
                      ),
                    ],
                  ),
                ),
              ),

const SizedBox(height: 16),
// ===== Themen =====
Card(
  child: Padding(
    padding: const EdgeInsets.all(16),
    child: Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        const Text('Themen', style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold)),
        const SizedBox(height: 8),
        ValueListenableBuilder<AppTheme>(
          valueListenable: themeNotifier,
          builder: (context, theme, _) {
            return ListTile(
              contentPadding: EdgeInsets.zero,
              title: const Text('Theme auswählen'),
              subtitle: const Text('5 helle + 5 dunkle Themes (außergewöhnlich)'),
              trailing: DropdownButton<AppTheme>(
                value: theme,
                onChanged: (v) async {
                  if (v == null) return;
                  await ThemeStore.setTheme(v);
                  themeNotifier.value = v;
                },
                items: const [
                  // Light (5)
                  DropdownMenuItem(value: AppTheme.light, child: Text('Hell (Classic)')),
                  DropdownMenuItem(value: AppTheme.sand, child: Text('Sand (Warm)')),
                  DropdownMenuItem(value: AppTheme.ocean, child: Text('Ocean (Meer)')),
                  DropdownMenuItem(value: AppTheme.teal, child: Text('Teal (Soft)')),
                  DropdownMenuItem(value: AppTheme.sakura, child: Text('Sakura (Pink)')),

                  // Dark (5)
                  DropdownMenuItem(value: AppTheme.dark, child: Text('Dunkel (Classic)')),
                  DropdownMenuItem(value: AppTheme.neonBlue, child: Text('Neon Blau')),
                  DropdownMenuItem(value: AppTheme.obsidian, child: Text('Obsidian Gold')),
                  DropdownMenuItem(value: AppTheme.amethyst, child: Text('Amethyst Night')),
                  DropdownMenuItem(value: AppTheme.crimson, child: Text('Crimson Noir')),
                ],
              ),
            );
          },
        ),
      ],
    ),
  ),
),
// ===== Export (Monat / Jahr) =====
              Card(
                child: Padding(
                  padding: const EdgeInsets.all(16),
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      const Text('Export', style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold)),
                      const SizedBox(height: 8),
                      const Text('Exportiert alle Zahlungen inkl. Belegen (die auf diesem Gerät vorhanden sind) als ZIP-Datei.'),
                      const SizedBox(height: 12),
                      Wrap(
                        spacing: 12,
                        runSpacing: 12,
                        children: [
                          ElevatedButton.icon(
                            icon: const Icon(Icons.calendar_month),
                            label: const Text('Monat exportieren'),
                            onPressed: () async {
                              final now = monthStart(selectedMonth);
                              final picked = await showDatePicker(
                                context: context,
                                initialDate: DateTime(now.year, now.month, 1),
                                firstDate: DateTime(now.year - 5, 1, 1),
                                lastDate: DateTime(now.year + 5, 12, 31),
                                helpText: 'Monat auswählen (Tag egal)',
                              );
                              if (picked == null) return;
                              final from = DateTime(picked.year, picked.month, 1);
                              final to = DateTime(picked.year, picked.month + 1, 1);
                              final base = 'familybudget_${picked.year}_${picked.month.toString().padLeft(2,'0')}';
                              await exportPaymentsZip(
                                context: context,
                                famRef: widget.famRef,
                                fromInclusive: from,
                                toExclusive: to,
                                filenameBase: base,
                              );
                            },
                          ),
                          ElevatedButton.icon(
                            icon: const Icon(Icons.date_range),
                            label: const Text('Jahr exportieren'),
                            onPressed: () async {
                              final now = monthStart(selectedMonth);
                              final year = await showDialog<int>(
                                context: context,
                                builder: (c) {
                                  int y = now.year;
                                  return StatefulBuilder(
                                    builder: (c, setDState) {
                                      return AlertDialog(
                                        title: const Text('Jahr auswählen'),
                                        content: DropdownButton<int>(
                                          value: y,
                                          items: List.generate(10, (i) => now.year - i)
                                              .map((v) => DropdownMenuItem(value: v, child: Text(v.toString())))
                                              .toList(),
                                          onChanged: (v) {
                                            if (v == null) return;
                                            setDState(() => y = v);
                                          },
                                        ),
                                        actions: [
                                          TextButton(onPressed: () => Navigator.pop(c), child: const Text('Abbrechen')),
                                          ElevatedButton(onPressed: () => Navigator.pop(c, y), child: const Text('Export')),
                                        ],
                                      );
                                    },
                                  );
                                },
                              );
                              if (year == null) return;
                              final from = DateTime(year, 1, 1);
                              final to = DateTime(year + 1, 1, 1);
                              final base = 'familybudget_${year}_jahr';
                              await exportPaymentsZip(
                                context: context,
                                famRef: widget.famRef,
                                fromInclusive: from,
                                toExclusive: to,
                                filenameBase: base,
                              );
                            },
                          ),
                        
                          ElevatedButton.icon(
                            icon: const Icon(Icons.lock),
                            label: const Text('Backup exportieren'),
                            onPressed: () async {
                              await exportEncryptedBackup(context: context, famRef: widget.famRef);
                            },
                          ),
                          ElevatedButton.icon(
                            icon: const Icon(Icons.restore),
                            label: const Text('Backup importieren'),
                            onPressed: () async {
                              final fid = await importEncryptedBackup(context: context, targetFamRef: widget.famRef);
                              if (fid != null && context.mounted) {
                                Navigator.of(context).pushAndRemoveUntil(
                                  MaterialPageRoute(builder: (_) => const BootGate()),
                                  (_) => false,
                                );
                              }
                            },
                          ),
],
                      ),
                    ],
                  ),
                ),
              ),
// ===== Budgets pro Kategorie (mit Hard-Limit Toggle) =====
              

// ===== Konten / Kategorien verwalten (mit Umbenennen & Archiv) =====
Card(
  child: Column(
    children: [
      ListTile(
        leading: const Icon(Icons.account_balance_wallet),
        title: const Text('Konten verwalten'),
        subtitle: const Text('Unterkonten anlegen, umbenennen, archivieren'),
        onTap: () async {
          final accountsCol = widget.famRef.collection('accounts');

          // Default sicherstellen
          await accountsCol.doc('default').set({
            'name': 'Haushalt',
            'nameLower': 'haushalt',
            'archived': false,
            'updatedAt': Timestamp.fromDate(DateTime.now()),
            'createdAt': Timestamp.fromDate(DateTime.now()),
          }, SetOptions(merge: true));

          await showManageCollectionDialog(
            context: context,
            title: 'Konten',
            col: accountsCol,
            txCol: widget.famRef.collection('tx'),
            txField: 'accountId',
            itemLabelSingular: 'Konto',
          );
        },
      ),
      const Divider(height: 1),
      ListTile(
        leading: const Icon(Icons.category),
        title: const Text('Kategorien verwalten'),
        subtitle: const Text('Kategorien anlegen, umbenennen, archivieren'),
        onTap: () async {
          final catCol = widget.famRef.collection('categories');

          await showManageCollectionDialog(
            context: context,
            title: 'Kategorien',
            col: catCol,
            txCol: widget.famRef.collection('tx'),
            txField: 'categoryId',
            itemLabelSingular: 'Kategorie',
          );
        },
      ),
    ],
  ),
),
const SizedBox(height: 12),

              // ===== Kategorien verwalten =====
              Card(
                child: Padding(
                  padding: const EdgeInsets.all(16),
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      const Text('Kategorien', style: TextStyle(fontSize: 16, fontWeight: FontWeight.w700)),
                      const SizedBox(height: 8),
                      Row(
                        children: [
                          Expanded(
                            child: TextField(
                              controller: newCategoryC,
                              decoration: const InputDecoration(
                                labelText: 'Neue Kategorie (z.B. Essen)',
                                border: OutlineInputBorder(),
                              ),
                            ),
                          ),
                          const SizedBox(width: 8),
                          ElevatedButton(
                            onPressed: _addCategory,
                            child: const Text('Hinzufügen'),
                          ),
                        ],
                      ),
                      const SizedBox(height: 12),
                      StreamBuilder<QuerySnapshot<Map<String, dynamic>>>(
                        stream: widget.famRef.collection('categories').orderBy('nameLower').snapshots(),
                        builder: (c, snap) {
                          if (snap.hasError) {
                            return Padding(
                              padding: const EdgeInsets.all(12),
                              child: Text('Fehler beim Laden: ${snap.error}', style: const TextStyle(color: Colors.red)),
                            );
                          }
                          if (!snap.hasData) return const Center(child: Padding(padding: EdgeInsets.all(12), child: CircularProgressIndicator()));
                          final docs = snap.data!.docs;
                          if (docs.isEmpty) return const Text('Noch keine Kategorien angelegt.');
                          return Column(
                            children: docs.map((d) {
                              final data = d.data();
                              if (data['archived'] == true) return const SizedBox.shrink();
                              final name = (data['name'] ?? '').toString();
                              return ListTile(
                                dense: true,
                                contentPadding: EdgeInsets.zero,
                                title: Text(name),
                                trailing: IconButton(
                                  icon: const Icon(Icons.archive),
                                  tooltip: 'Archivieren',
                                  onPressed: () => _deleteCategory(d),
                                ),
                              );
                            }).toList(),
                          );
                        },
                      ),
                    ],
                  ),
                ),
              ),

Card(
                child: Padding(
                  padding: const EdgeInsets.all(16),
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      const Text('Budgets pro Kategorie', style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold)),
                      const SizedBox(height: 8),
                      SwitchListTile(
                        contentPadding: EdgeInsets.zero,
                        title: const Text('Budget-Limit aktiv'),
                        subtitle: const Text('Wenn du beim Speichern drüber bist, bekommst du eine Warnung.'),
                        value: (data['enforceBudgets'] is bool) ? data['enforceBudgets'] as bool : true,
                        onChanged: (v) async {
                          await widget.famRef.set({'enforceBudgets': v}, SetOptions(merge: true));
                        },
                      ),
                      const SizedBox(height: 8),
                      if (categoryBudgets.isEmpty)
                        const Text('Noch keine Kategorie-Budgets angelegt.'),
                      if (categoryBudgets.isNotEmpty)
                        StreamBuilder<QuerySnapshot<Map<String, dynamic>>>(
                          stream: widget.famRef.collection('tx').snapshots(),
                          builder: (context, txSnap) {
                            final now = monthStart(selectedMonth);
                            final mStart = DateTime(now.year, now.month, 1);
                            final monthEnd = DateTime(now.year, now.month + 1, 1);

                            final spentByCat = <String, double>{};
                            if (txSnap.hasData) {
                              for (final d in txSnap.data!.docs) {
                                final p = d.data();
                                if ((p['type'] as String? ?? 'expense') != 'expense') continue;
                                final cat = p['category'] as String? ?? 'Sonstiges';
                                final amt = (p['amount'] as num?)?.toDouble() ?? 0.0;
                                final dueDt = tsToDate(p['dueDate']);
                                final createdDt = tsToDate(p['createdAt']);
                                final effective = dueDt ?? createdDt ?? now;
                                if (!effective.isBefore(mStart) && effective.isBefore(monthEnd)) {
                                  spentByCat[cat] = (spentByCat[cat] ?? 0.0) + amt;
                                }
                              }
                            }

                            final entries = categoryBudgets.entries.toList()
                              ..sort((a, b) => a.key.compareTo(b.key));

                            return Column(
                              children: entries.map((e) {
                                final cat = e.key;
                                final limit = (e.value is num) ? (e.value as num).toDouble() : 0.0;
                                final spent = spentByCat[cat] ?? 0.0;
                                final ratio = (limit <= 0) ? 0.0 : (spent / limit).clamp(0.0, 1.0);
                                final warn80 = limit > 0 && spent >= 0.8 * limit && spent < limit;
                                final warn100 = limit > 0 && spent >= limit;

                                return Padding(
                                  padding: const EdgeInsets.symmetric(vertical: 8),
                                  child: Column(
                                    crossAxisAlignment: CrossAxisAlignment.start,
                                    children: [
                                      Row(
                                        children: [
                                          Expanded(child: Text(cat, style: const TextStyle(fontWeight: FontWeight.w600))),
                                          Text('${spent.toStringAsFixed(2)} / ${limit.toStringAsFixed(2)} €'),
                                          IconButton(
                                            tooltip: 'Budget entfernen',
                                            icon: const Icon(Icons.delete_outline),
                                            onPressed: () => _removeCategoryBudget(categoryBudgets, cat),
                                          ),
                                        ],
                                      ),
                                      LinearProgressIndicator(value: ratio),
                                      if (warn80 || warn100) ...[
                                        const SizedBox(height: 6),
                                        Text(
                                          warn100 ? '⚠️ Budget überschritten' : '⚠️ Budget fast erreicht (80%)',
                                          style: const TextStyle(color: Colors.redAccent),
                                        ),
                                      ],
                                    ],
                                  ),
                                );
                              }).toList(),
                            );
                          },
                        ),
                      const Divider(height: 24),
                      const Text('Neues/Update Kategorie-Budget', style: TextStyle(fontWeight: FontWeight.w600)),
                      const SizedBox(height: 8),
                      TextField(
                        controller: catNameC,
                        decoration: const InputDecoration(labelText: 'Kategorie (z.B. Essen)', border: OutlineInputBorder()),
                      ),
                      const SizedBox(height: 8),
                      TextField(
                        controller: catLimitC,
                        keyboardType: TextInputType.number,
                        decoration: const InputDecoration(labelText: 'Limit (z.B. 400)', border: OutlineInputBorder()),
                      ),
                      const SizedBox(height: 10),
                      Align(
                        alignment: Alignment.centerRight,
                        child: ElevatedButton(
                          onPressed: () => _addOrUpdateCategoryBudget(categoryBudgets),
                          child: const Text('Speichern'),
                        ),
                      ),
                    ],
                  ),
                ),
              ),
              // ===== Wiederkehrende Zahlungen: Liste + Bearbeiten + Pausieren =====
              Card(
                child: Padding(
                  padding: const EdgeInsets.all(16),
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      Row(
                        children: [
                          const Expanded(
                            child: Text('Wiederkehrende Zahlungen',
                                style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold)),
                          ),
                          ElevatedButton.icon(
                            onPressed: () => _addRecurringRuleDialog(context),
                            icon: const Icon(Icons.add),
                            label: const Text('Neu'),
                          ),
                        ],
                      ),
                      const SizedBox(height: 8),
                      StreamBuilder<QuerySnapshot<Map<String, dynamic>>>(
                        stream: widget.famRef
                            .collection('recurring_rules')
                            .orderBy('createdAt', descending: true)
                            .snapshots(),
                        builder: (context, rSnap) {
                          if (!rSnap.hasData) return const Padding(
                            padding: EdgeInsets.all(8),
                            child: Center(child: CircularProgressIndicator()),
                          );
                          final rules = rSnap.data!.docs;
                          if (rules.isEmpty) return const Text('Noch keine wiederkehrenden Regeln.');

                          return Column(
                            children: rules.map((rDoc) {
                              final r = rDoc.data();
                              final title = r['title'] as String? ?? 'Wiederkehrend';
                              final amount = (r['amount'] as num?)?.toDouble() ?? 0.0;
                              final cat = r['category'] as String? ?? 'Sonstiges';
                              final unit = (r['unit'] as String?) ?? ((r['interval'] is String) ? (r['interval'] as String) : null) ?? 'monthly';
                              final interval = (r['interval'] is num) ? (r['interval'] as num).toInt() : ((r['every'] is num) ? (r['every'] as num).toInt() : (int.tryParse((r['every'] ?? '').toString()) ?? 1));
                              final active = (r['active'] as bool?) ?? true;

	                              Future<bool> confirmDelete() async {
	                                final del = await showDialog<bool>(
	                                  context: context,
	                                  builder: (c) => AlertDialog(
	                                    title: const Text('Regel löschen?'),
	                                    content: Text(
	                                      '„$title“ wirklich löschen?\n\n'
	                                      'Hinweis: Bereits erzeugte Zahlungen bleiben bestehen.',
	                                    ),
	                                    actions: [
	                                      TextButton(onPressed: () => Navigator.pop(c, false), child: const Text('Abbrechen')),
	                                      ElevatedButton(onPressed: () => Navigator.pop(c, true), child: const Text('Löschen')),
	                                    ],
	                                  ),
	                                );
	                                return del == true;
	                              }

	                              return Dismissible(
	                                key: ValueKey('rule_${rDoc.id}'),
	                                direction: DismissDirection.endToStart,
	                                background: Container(
	                                  alignment: Alignment.centerRight,
	                                  padding: const EdgeInsets.symmetric(horizontal: 16),
	                                  color: Colors.red.withOpacity(0.85),
	                                  child: const Icon(Icons.delete, color: Colors.white),
	                                ),
	                                confirmDismiss: (_) => confirmDelete(),
	                                onDismissed: (_) async {
	                                  await rDoc.reference.delete();
	                                },
	                                child: ListTile(
	                                  contentPadding: EdgeInsets.zero,
	                                  title: Text(title),
	                                  subtitle: Text('$cat • ${_unitIntervalLabel(unit, interval)}'),
	                                  trailing: Row(
	                                    mainAxisSize: MainAxisSize.min,
	                                    children: [
	                                      Switch(
	                                        value: active,
	                                        onChanged: (v) async {
	                                          await rDoc.reference.update({'active': v});
	                                          if (v) {
	                                            await ensureRecurringGeneratedForFamily(famRef: widget.famRef);
	                                          }
	                                        },
	                                      ),
	                                      IconButton(
	                                        tooltip: 'Löschen',
	                                        icon: const Icon(Icons.delete_outline),
	                                        onPressed: () async {
	                                          if (await confirmDelete()) {
	                                            await rDoc.reference.delete();
	                                          }
	                                        },
	                                      ),
	                                    ],
	                                  ),
	                                  leading: CircleAvatar(
	                                    child: Text(amount.toStringAsFixed(0)),
	                                  ),
	                                  onTap: () => _editRecurringRuleDialog(context, rDoc),
	                                ),
	                              );
                            }).toList(),
                          );
                        },
                      ),
                    ],
                  ),
                ),
              ),
              // ===== Jahresübersicht =====
              Card(
                child: ListTile(
                  title: const Text('Jahresübersicht'),
                  subtitle: const Text('Einnahmen/Ausgaben & Kategorien als Diagramm'),
                  leading: const Icon(Icons.bar_chart),
                  onTap: _openYearReport,
                ),
              ),
            ],
          ),
        );
      },
    );
  }
}


class YearReportScreen extends StatefulWidget {
  const YearReportScreen({super.key, required this.famRef});

  final DocumentReference<Map<String, dynamic>> famRef;

  @override
  State<YearReportScreen> createState() => _YearReportScreenState();
}

class _YearReportScreenState extends State<YearReportScreen> {
  Widget _miniCard(String title, double value, IconData icon) {
    return Card(
      child: Padding(
        padding: const EdgeInsets.all(12),
        child: Row(
          children: [
            Icon(icon),
            const SizedBox(width: 10),
            Expanded(
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text(title, style: const TextStyle(fontWeight: FontWeight.w600)),
                  const SizedBox(height: 4),
                  Text('${value.toStringAsFixed(2)} €', style: const TextStyle(fontSize: 18)),
                ],
              ),
            ),
          ],
        ),
      ),
    );
  }


  late int year;

  @override
  void initState() {
    super.initState();
    year = DateTime.now().year;
  }

  DateTime _startOfYear(int y) => DateTime(y, 1, 1);
  DateTime _startOfNextYear(int y) => DateTime(y + 1, 1, 1);

  Future<_YearReportData> _load(int y) async {
    final from = _startOfYear(y);
    final to = _startOfNextYear(y);

    // Wir lesen alles aus und filtern in Memory (wegen optionaler dueDate==null)
    final snap = await widget.famRef.collection('tx').get();

    double income = 0.0;
    double expense = 0.0;
    final byCat = <String, double>{};
    final incomeByMonth = List<double>.filled(12, 0.0);
    final expenseByMonth = List<double>.filled(12, 0.0);

    for (final d in snap.docs) {
      final p = d.data();
      final t = p['type'] as String? ?? 'expense';
      final amt = (p['amount'] as num?)?.toDouble() ?? 0.0;
      final cat = p['category'] as String? ?? 'Sonstiges';

      final dueDt = tsToDate(p['dueDate']);
      final createdDt = tsToDate(p['createdAt']);
      final effective = dueDt ?? createdDt;

      if (effective == null) continue;
      if (effective.isBefore(from) || !effective.isBefore(to)) continue;

      final monthIndex = effective.month - 1;
      if (t == 'income') {
        income += amt;
        incomeByMonth[monthIndex] += amt;
      } else {
        expense += amt;
        expenseByMonth[monthIndex] += amt;
        byCat[cat] = (byCat[cat] ?? 0.0) + amt;
      }
    }

    // Top Kategorien + Rest
    final catEntries = byCat.entries.toList()
      ..sort((a, b) => b.value.compareTo(a.value));

    final top = <MapEntry<String, double>>[];
    double other = 0.0;
    for (int i = 0; i < catEntries.length; i++) {
      if (i < 6) {
        top.add(catEntries[i]);
      } else {
        other += catEntries[i].value;
      }
    }
    if (other > 0.0001) top.add(MapEntry('Andere', other));

    return _YearReportData(
      income: income,
      expense: expense,
      byCategoryTop: top,
      incomeByMonth: incomeByMonth,
      expenseByMonth: expenseByMonth,
    );
  }

  @override
  Widget build(BuildContext context) {
    final years = List<int>.generate(5, (i) => DateTime.now().year - i);

    return Scaffold(
      appBar: AppBar(title: const Text('Jahresübersicht')),
      body: FutureBuilder<_YearReportData>(
        future: _load(year),
        builder: (context, snap) {
          final data = snap.data;

          return ListView(
            padding: const EdgeInsets.all(16),
            children: [
              Card(
                child: Padding(
                  padding: const EdgeInsets.all(16),
                  child: Row(
                    children: [
                      const Text('Jahr:', style: TextStyle(fontWeight: FontWeight.w600)),
                      const SizedBox(width: 12),
                      DropdownButton<int>(
                        value: year,
                        items: years.map((y) => DropdownMenuItem(value: y, child: Text(y.toString()))).toList(),
                        onChanged: (v) => setState(() => year = v ?? year),
                      ),
                      const Spacer(),
                      if (snap.connectionState == ConnectionState.waiting) const SizedBox(
                        width: 18, height: 18, child: CircularProgressIndicator(strokeWidth: 2),
                      ),
                    ],
                  ),
                ),
              ),
              if (data != null) ...[
                const SizedBox(height: 12),
                Row(
                  children: [
                    Expanded(child: _miniCard('Einnahmen', data.income, Icons.arrow_downward)),
                    const SizedBox(width: 10),
                    Expanded(child: _miniCard('Ausgaben', data.expense, Icons.arrow_upward)),
                  ],
                ),
                const SizedBox(height: 10),
                _miniCard('Saldo', data.income - data.expense, Icons.swap_vert),
                const SizedBox(height: 12),
                Card(
                  child: Padding(
                    padding: const EdgeInsets.all(16),
                    child: Column(
                      crossAxisAlignment: CrossAxisAlignment.start,
                      children: [
                        const Text('Ausgaben nach Kategorie', style: TextStyle(fontSize: 16, fontWeight: FontWeight.bold)),
                        const SizedBox(height: 12),
                        SizedBox(
                          height: 220,
                          child: data.byCategoryTop.isEmpty
                              ? const Center(child: Text('Keine Ausgaben in diesem Jahr.'))
                              : PieChart(
                                  PieChartData(
                                    sectionsSpace: 2,
                                    centerSpaceRadius: 42,
                                    sections: [
                                      for (int i = 0; i < data.byCategoryTop.length; i++)
                                        PieChartSectionData(
                                          value: data.byCategoryTop[i].value,
                                          title: '',
                                          radius: 70,
                                        ),
                                    ],
                                  ),
                                ),
                        ),
                        const SizedBox(height: 12),
                        ...data.byCategoryTop.map((e) => Padding(
                              padding: const EdgeInsets.symmetric(vertical: 2),
                              child: Row(
                                children: [
                                  Expanded(child: Text(e.key)),
                                  Text('${e.value.toStringAsFixed(2)} €'),
                                ],
                              ),
                            )),
                      ],
                    ),
                  ),
                ),
                const SizedBox(height: 12),
                Card(
                  child: Padding(
                    padding: const EdgeInsets.all(16),
                    child: Column(
                      crossAxisAlignment: CrossAxisAlignment.start,
                      children: [
                        const Text('Monate', style: TextStyle(fontSize: 16, fontWeight: FontWeight.bold)),
                        const SizedBox(height: 12),
                        SizedBox(
                          height: 260,
                          child: BarChart(
                            BarChartData(
                              gridData: const FlGridData(show: true),
                              titlesData: FlTitlesData(
                                leftTitles: const AxisTitles(sideTitles: SideTitles(showTitles: true, reservedSize: 40)),
                                rightTitles: const AxisTitles(sideTitles: SideTitles(showTitles: false)),
                                topTitles: const AxisTitles(sideTitles: SideTitles(showTitles: false)),
                                bottomTitles: AxisTitles(
                                  sideTitles: SideTitles(
                                    showTitles: true,
                                    getTitlesWidget: (value, meta) {
                                      const labels = ['J', 'F', 'M', 'A', 'M', 'J', 'J', 'A', 'S', 'O', 'N', 'D'];
                                      final idx = value.toInt();
                                      if (idx < 0 || idx > 11) return const SizedBox.shrink();
                                      return Padding(
                                        padding: const EdgeInsets.only(top: 6),
                                        child: Text(labels[idx]),
                                      );
                                    },
                                  ),
                                ),
                              ),
                              barGroups: List.generate(12, (i) {
                                final inc = data.incomeByMonth[i];
                                final exp = data.expenseByMonth[i];
                                return BarChartGroupData(
                                  x: i,
                                  barRods: [
                                    BarChartRodData(toY: inc),
                                    BarChartRodData(toY: exp),
                                  ],
                                );
                              }),
                            ),
                          ),
                        ),
                        const SizedBox(height: 8),
                        const Text('Links: Einnahmen • Rechts: Ausgaben', style: TextStyle(color: Colors.black54)),
                      ],
                    ),
                  ),
                ),
              ],
            ],
          );
        },
      ),
    );
  }
}

class _YearReportData {
  final double income;
  final double expense;
  final List<MapEntry<String, double>> byCategoryTop;
  final List<double> incomeByMonth; // len 12
  final List<double> expenseByMonth; // len 12

  _YearReportData({
    required this.income,
    required this.expense,
    required this.byCategoryTop,
    required this.incomeByMonth,
    required this.expenseByMonth,
  });
}
