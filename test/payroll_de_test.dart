// Tests für die Lohnsteuer-Berechnung – von FGSeRaser (github.com/FGSeRaser)
import 'package:flutter_test/flutter_test.dart';
import 'package:familybudget/payroll_de.dart';

void main() {
  test('Tarif 2025: 40.000 € zvE ergibt 7.320 €', () {
    expect(kLohnWerte2025.tarif(40000), 7320);
  });

  test('Tarif 2026: Grundfreibetrag ist steuerfrei', () {
    expect(kLohnWerte2026.tarif(12348), 0);
    expect(kLohnWerte2026.tarif(12349) >= 0, true);
  });

  test('Steuerklasse III zahlt weniger als I, V mehr als I', () {
    LohnsteuerErgebnis r(String kl) => berechneLohnsteuerMonat(
          bruttoMonat: 3000,
          jahr: 2026,
          steuerklasse: kl,
          vorsorgeMonat: 600,
          kirchensteuer: false,
          kirchensteuerSatz: 0.09,
          soli: true,
        );
    expect(r('III').lohnsteuer < r('I').lohnsteuer, true);
    expect(r('V').lohnsteuer > r('I').lohnsteuer, true);
    expect(r('I').soli, 0); // unter der Freigrenze
  });
}
