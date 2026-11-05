// Prüft, dass Eingabe-Fenster in jedem Theme aufgehen.
// von FGSeRaser (https://github.com/FGSeRaser)
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';

import 'package:familybudget/main.dart';

void main() {
  for (final t in AppTheme.values) {
    testWidgets('E-Mail-Fenster geht auf: ${t.name}', (tester) async {
      await tester.pumpWidget(MaterialApp(
        theme: buildAppTheme(t),
        home: Scaffold(
          body: Builder(
            builder: (context) => Center(
              child: FilledButton(
                onPressed: () => askEmailPassword(context, title: 'Konto sichern', okText: 'Sichern'),
                child: const Text('öffnen'),
              ),
            ),
          ),
        ),
      ));
      await tester.tap(find.text('öffnen'));
      await tester.pumpAndSettle();
      expect(find.byType(AlertDialog), findsOneWidget);
      expect(find.text('Konto sichern'), findsOneWidget);
      expect(tester.takeException(), isNull);
    });
  }

  testWidgets('Theme-Auswahl geht auf', (tester) async {
    await tester.pumpWidget(MaterialApp(
      theme: buildAppTheme(AppTheme.bloodMoon),
      home: Scaffold(
        body: Builder(
          builder: (context) => FilledButton(onPressed: () => showThemePicker(context), child: const Text('themes')),
        ),
      ),
    ));
    await tester.tap(find.text('themes'));
    await tester.pumpAndSettle();
    expect(find.text('Theme auswählen'), findsOneWidget);
    expect(tester.takeException(), isNull);
  });
}
