import java.util.Properties
import java.io.FileInputStream

plugins {
    id("com.android.application")
    id("kotlin-android")
    // The Flutter Gradle Plugin must be applied after the Android and Kotlin Gradle plugins.
    id("dev.flutter.flutter-gradle-plugin")
	id("com.google.gms.google-services")
}

// Release-Signatur (von FGSeRaser, github.com/FGSeRaser)
// Reihenfolge: 1) GitHub Actions (Umgebungsvariablen)  2) android/key.properties (lokal)  3) Debug-Schlüssel
// Wichtig: Updates lassen sich nur drüberinstallieren, wenn IMMER derselbe Schlüssel benutzt wird.
val keyProps = Properties().apply {
    val f = rootProject.file("key.properties")
    if (f.exists()) load(FileInputStream(f))
}
val releaseStoreFile: String? = System.getenv("FB_KEYSTORE_PATH") ?: keyProps.getProperty("storeFile")
val releaseStorePassword: String? = System.getenv("FB_KEYSTORE_PASSWORD") ?: keyProps.getProperty("storePassword")
val releaseKeyAlias: String? = System.getenv("FB_KEY_ALIAS") ?: keyProps.getProperty("keyAlias")
val releaseKeyPassword: String? = System.getenv("FB_KEY_PASSWORD") ?: keyProps.getProperty("keyPassword")
val hasReleaseKey = !releaseStoreFile.isNullOrBlank() && file(releaseStoreFile!!).exists()

android {
    namespace = "com.example.familybudget"
    compileSdk = flutter.compileSdkVersion
    ndkVersion = flutter.ndkVersion

compileOptions {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
    isCoreLibraryDesugaringEnabled = true
}


    kotlinOptions {
        jvmTarget = JavaVersion.VERSION_17.toString()
    }

    defaultConfig {
        // TODO: Specify your own unique Application ID (https://developer.android.com/studio/build/application-id.html).
        applicationId = "com.example.familybudget"
        // You can update the following values to match your application needs.
        // For more information, see: https://flutter.dev/to/review-gradle-config.
        minSdk = flutter.minSdkVersion
        targetSdk = flutter.targetSdkVersion
        versionCode = flutter.versionCode
        versionName = flutter.versionName
    }

    signingConfigs {
        if (hasReleaseKey) {
            create("release") {
                storeFile = file(releaseStoreFile!!)
                storePassword = releaseStorePassword
                keyAlias = releaseKeyAlias
                keyPassword = releaseKeyPassword
            }
        }
    }

    buildTypes {
        release {
            // Mit eigenem Release-Schlüssel signieren, sonst (nur zum Testen) mit Debug-Schlüssel
            signingConfig = if (hasReleaseKey) signingConfigs.getByName("release") else signingConfigs.getByName("debug")
            proguardFiles(getDefaultProguardFile("proguard-android-optimize.txt"), "proguard-rules.pro")
        }
    }
}
dependencies {
    coreLibraryDesugaring("com.android.tools:desugar_jdk_libs:2.1.4")
}

flutter {
    source = "../.."
}
