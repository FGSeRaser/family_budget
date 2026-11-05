// Prüft, dass alle Themes gebaut werden können.
// von FGSeRaser (https://github.com/FGSeRaser)
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';

import 'package:familybudget/main.dart';

void main() {
  test('Alle Themes lassen sich bauen und haben einen Namen', () {
    for (final t in AppTheme.values) {
      final theme = buildAppTheme(t);
      expect(theme.useMaterial3, isTrue, reason: t.name);
      expect(appThemeLabel(t), isNotEmpty, reason: t.name);
    }
  });

  test('Kräftige Themes haben Farbverlauf und farbige Kopfleiste', () {
    expect(strongThemes.length, 10);
    strongThemes.forEach((t, spec) {
      final theme = buildAppTheme(t);
      expect(theme.extension<AppBackground>(), isNotNull, reason: t.name);
      expect(theme.appBarTheme.backgroundColor, spec.primary, reason: t.name);
      expect(theme.colorScheme.brightness, spec.brightness, reason: t.name);
    });
  });

  test('Klassische Themes bleiben ohne Farbverlauf', () {
    expect(buildAppTheme(AppTheme.azureOrange).extension<AppBackground>(), isNull);
    expect(buildAppTheme(AppTheme.cyberpunk).colorScheme.brightness, Brightness.dark);
  });
}
