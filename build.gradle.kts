plugins {
}

group = "io.github.anupam"
version = "1.0-SNAPSHOT"

subprojects {
    plugins.apply("java")

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
}