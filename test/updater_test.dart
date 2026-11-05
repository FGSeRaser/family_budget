// Tests für den Versionsvergleich beim Auto-Update.
// von FGSeRaser (https://github.com/FGSeRaser)
import 'package:flutter_test/flutter_test.dart';

import 'package:familybudget/updater.dart';

void main() {
  test('Neuere Versionen werden erkannt', () {
    expect(compareVersions('v2.3.0', '2.2.0'), greaterThan(0));
    expect(compareVersions('2.10.0', '2.9.9'), greaterThan(0));
    expect(compareVersions('3.0', '2.9.9'), greaterThan(0));
  });

  test('Gleiche oder ältere Versionen lösen kein Update aus', () {
    expect(compareVersions('v2.2.0', '2.2.0'), 0);
    expect(compareVersions('2.2.0+5', '2.2.0'), 0);
    expect(compareVersions('2.1.9', '2.2.0'), lessThan(0));
  });
}
