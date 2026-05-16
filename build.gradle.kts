plugins {
    id("com.diffplug.spotless") version "7.0.2" apply false
}

group = "io.github.anupam"
version = "1.0-SNAPSHOT"

subprojects {
    plugins.apply("java")
    plugins.apply("com.diffplug.spotless")

    repositories { 
        mavenCentral() 
    }

    // Configure Java toolchain for all subprojects — Java 25 LTS
    extensions.configure<org.gradle.api.plugins.JavaPluginExtension> {
        toolchain.languageVersion.set(org.gradle.jvm.toolchain.JavaLanguageVersion.of(25))
    }

    tasks.withType<org.gradle.api.tasks.compile.JavaCompile>().configureEach {
        options.release.set(25)
    }

    dependencies {
        add("testImplementation", platform("org.junit:junit-bom:5.11.4"))
        add("testImplementation", "org.junit.jupiter:junit-jupiter")
        add("testRuntimeOnly", "org.junit.platform:junit-platform-launcher")
    }

    tasks.withType<org.gradle.api.tasks.testing.Test>().configureEach {
        useJUnitPlatform()
    }

    // Configure Spotless for code formatting
    configure<com.diffplug.gradle.spotless.SpotlessExtension> {
        java {
            googleJavaFormat("1.35.0").aosp().reflowLongStrings()

            importOrder("java", "javax", "", "\\#")
            removeUnusedImports()
            trimTrailingWhitespace()
            endWithNewline()
            target("src/**/*.java")
            targetExclude("build/**")
            formatAnnotations()
        }
    }
}