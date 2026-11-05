# FamilyBudget – R8-Regeln (FGSeRaser)
# ML Kit Texterkennung: nur Latein ist eingebaut, die anderen Schriften fehlen absichtlich
-dontwarn com.google.mlkit.vision.text.chinese.**
-dontwarn com.google.mlkit.vision.text.devanagari.**
-dontwarn com.google.mlkit.vision.text.japanese.**
-dontwarn com.google.mlkit.vision.text.korean.**
# Benachrichtigungen (Gson-Typen der geplanten Erinnerungen)
-keep class com.dexterous.** { *; }
