plugins {
    id("com.diffplug.spotless") version "6.23.3" apply false
}

group = "io.github.anupam"
version = "1.0-SNAPSHOT"

subprojects {
    plugins.apply("java")
    plugins.apply("com.diffplug.spotless")

    repositories { 
        mavenCentral() 
    }

    // Configure Java toolchain for all subprojects
    extensions.configure<org.gradle.api.plugins.JavaPluginExtension> {
        toolchain.languageVersion.set(org.gradle.jvm.toolchain.JavaLanguageVersion.of(21))
    }

    tasks.withType<org.gradle.api.tasks.compile.JavaCompile>().configureEach {
        options.release.set(21)
    }

    dependencies {
        add("testImplementation", platform("org.junit:junit-bom:5.10.0"))
        add("testImplementation", "org.junit.jupiter:junit-jupiter")
        add("testRuntimeOnly", "org.junit.platform:junit-platform-launcher")
    }

    tasks.withType<org.gradle.api.tasks.testing.Test>().configureEach {
        useJUnitPlatform()
    }

    // Configure Spotless for code formatting
    configure<com.diffplug.gradle.spotless.SpotlessExtension> {
        java {
            // Use Google Java Format
            googleJavaFormat("1.19.1").aosp().reflowLongStrings()
            
            // Import order: java/javax, blank line, all other imports, blank line, static imports
            importOrder("java", "javax", "", "\\#")
            
            // Remove unused imports
            removeUnusedImports()
            
            // Trim trailing whitespace
            trimTrailingWhitespace()
            
            // Ensure newline at end of file
            endWithNewline()
            
            // Format all Java files
            target("src/**/*.java")
            targetExclude("build/**")
            
            // Toggle for javadoc formatting
            formatAnnotations()
        }
    }
}