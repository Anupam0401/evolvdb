plugins {
    application
}

dependencies {
    implementation(project(":evolvdb-config"))
}

application {
    mainClass.set("io.github.anupam.evolvdb.cli.Main")
}
