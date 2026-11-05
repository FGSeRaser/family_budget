// Brutto -> Netto (Deutschland) – Lohnsteuer, Soli, Kirchensteuer
// von FGSeRaser (https://github.com/FGSeRaser)
//
// Nachbau des amtlichen Programmablaufplans (PAP) in vereinfachter Form:
// - Einkommensteuertarif nach § 32a EStG (2025 und 2026)
// - Steuerklassen I bis VI (III = Splitting, V/VI = Sonderformel wie im PAP)
// - Arbeitnehmer-Pauschbetrag, Sonderausgaben-Pauschbetrag, Entlastungsbetrag (II)
// - Vorsorgepauschale aus den echten AN-Anteilen der Sozialversicherung
// - Soli mit Freigrenze und Milderungszone, Kirchensteuer 8 % / 9 %
// Nicht enthalten: Kinderfreibeträge, Freibeträge aus ELStAM, Sonderzahlungen.
// Ergebnis ist deshalb auf wenige Euro genau, aber keine offizielle Abrechnung.

class LohnJahrWerte {
  final int jahr;
  final double bbgKvMonat; // Beitragsbemessungsgrenze KV/PV pro Monat
  final double bbgRvMonat; // Beitragsbemessungsgrenze RV/AV pro Monat
  final double soliFreigrenze; // Jahres-Lohnsteuer, Einzelveranlagung
  final double w1Stkl5;
  final double w2Stkl5;
  final double w3Stkl5;
  final double Function(double zve) tarif;

  const LohnJahrWerte({
    required this.jahr,
    required this.bbgKvMonat,
    required this.bbgRvMonat,
    required this.soliFreigrenze,
    required this.w1Stkl5,
    required this.w2Stkl5,
    required this.w3Stkl5,
    required this.tarif,
  });
}

double _tarif2025(double x) {
  final zve = x.floorToDouble();
  if (zve <= 12096) return 0;
  if (zve <= 17443) {
    final y = (zve - 12096) / 10000;
    return ((932.30 * y + 1400) * y).floorToDouble();
  }
  if (zve <= 68480) {
    final z = (zve - 17443) / 10000;
    return ((176.64 * z + 2397) * z + 1015.13).floorToDouble();
  }
  if (zve <= 277825) return (0.42 * zve - 10911.92).floorToDouble();
  return (0.45 * zve - 19246.67).floorToDouble();
}

double _tarif2026(double x) {
  final zve = x.floorToDouble();
  if (zve <= 12348) return 0;
  if (zve <= 17799) {
    final y = (zve - 12348) / 10000;
    return ((914.51 * y + 1400) * y).floorToDouble();
  }
  if (zve <= 69878) {
    final z = (zve - 17799) / 10000;
    return ((173.10 * z + 2397) * z + 1034.87).floorToDouble();
  }
  if (zve <= 277825) return (0.42 * zve - 11135.63).floorToDouble();
  return (0.45 * zve - 19470.38).floorToDouble();
}

const LohnJahrWerte kLohnWerte2025 = LohnJahrWerte(
  jahr: 2025,
  bbgKvMonat: 5512.50,
  bbgRvMonat: 8050.00,
  soliFreigrenze: 19950,
  w1Stkl5: 13785,
  w2Stkl5: 34240,
  w3Stkl5: 222260,
  tarif: _tarif2025,
);

const LohnJahrWerte kLohnWerte2026 = LohnJahrWerte(
  jahr: 2026,
  bbgKvMonat: 5812.50,
  bbgRvMonat: 8450.00,
  soliFreigrenze: 20350,
  w1Stkl5: 14071,
  w2Stkl5: 34939,
  w3Stkl5: 222260,
  tarif: _tarif2026,
);

/// Werte für ein Jahr. Für Jahre nach 2026 werden die 2026er Werte genutzt.
LohnJahrWerte lohnWerteFuerJahr(int jahr) => jahr <= 2025 ? kLohnWerte2025 : kLohnWerte2026;

/// Steuerklasse V/VI nach PAP (MST5_6 / UP5_6)
double _steuerKlasse56(double zve, LohnJahrWerte w) {
  double up56(double zx) {
    final st1 = w.tarif(zx * 1.25);
    final st2 = w.tarif(zx * 0.75);
    final diff = (st1 - st2) * 2;
    final mist = (zx * 0.14).floorToDouble();
    return diff < mist ? mist : diff;
  }

  final zzx = zve.floorToDouble();
  if (zzx > w.w2Stkl5) {
    var st = up56(w.w2Stkl5);
    if (zzx > w.w3Stkl5) {
      st = (st + (w.w3Stkl5 - w.w2Stkl5) * 0.42).floorToDouble();
      st = (st + (zzx - w.w3Stkl5) * 0.45).floorToDouble();
    } else {
      st = (st + (zzx - w.w2Stkl5) * 0.42).floorToDouble();
    }
    return st;
  }

  var st = up56(zzx);
  if (zzx > w.w1Stkl5) {
    final vergl = st;
    final hoch = (up56(w.w1Stkl5) + (zzx - w.w1Stkl5) * 0.42).floorToDouble();
    st = hoch < vergl ? hoch : vergl;
  }
  return st;
}

class LohnsteuerErgebnis {
  final double lohnsteuer; // pro Monat
  final double soli; // pro Monat
  final double kirche; // pro Monat
  final double zveJahr; // zu versteuerndes Jahreseinkommen (Info)

  const LohnsteuerErgebnis({
    required this.lohnsteuer,
    required this.soli,
    required this.kirche,
    required this.zveJahr,
  });
}

double _cent(double v) => (v * 100).floorToDouble() / 100.0;

/// Lohnsteuer für einen Monatslohn.
/// [vorsorgeMonat] = AN-Anteile KV + PV + RV + AV in diesem Monat (Vorsorgepauschale).
LohnsteuerErgebnis berechneLohnsteuerMonat({
  required double bruttoMonat,
  required int jahr,
  required String steuerklasse,
  required double vorsorgeMonat,
  required bool kirchensteuer,
  required double kirchensteuerSatz,
  required bool soli,
}) {
  final w = lohnWerteFuerJahr(jahr);
  final kl = steuerklasse.trim().toUpperCase();

  final jahresBrutto = bruttoMonat * 12;
  double abzug = 0;
  if (kl != 'VI') {
    abzug += jahresBrutto < 1230 ? jahresBrutto : 1230; // Arbeitnehmer-Pauschbetrag
    abzug += 36; // Sonderausgaben-Pauschbetrag
  }
  if (kl == 'II') abzug += 4260; // Entlastungsbetrag Alleinerziehende
  abzug += vorsorgeMonat * 12;

  var zve = jahresBrutto - abzug;
  if (zve < 0) zve = 0;
  zve = zve.floorToDouble();

  double lstJahr;
  if (kl == 'III') {
    lstJahr = 2 * w.tarif((zve / 2).floorToDouble());
  } else if (kl == 'V' || kl == 'VI') {
    lstJahr = _steuerKlasse56(zve, w);
  } else {
    lstJahr = w.tarif(zve);
  }

  double soliJahr = 0;
  if (soli) {
    final freigrenze = kl == 'III' ? w.soliFreigrenze * 2 : w.soliFreigrenze;
    if (lstJahr > freigrenze) {
      final voll = lstJahr * 0.055;
      final mild = (lstJahr - freigrenze) * 0.119;
      soliJahr = voll < mild ? voll : mild;
    }
  }

  final kircheJahr = kirchensteuer ? lstJahr * kirchensteuerSatz : 0.0;

  return LohnsteuerErgebnis(
    lohnsteuer: _cent(lstJahr / 12),
    soli: _cent(soliJahr / 12),
    kirche: _cent(kircheJahr / 12),
    zveJahr: zve,
  );
}
