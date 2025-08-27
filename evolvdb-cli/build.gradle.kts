plugins {
    application
}

dependencies {
    implementation(project(":evolvdb-config"))
    implementation(project(":evolvdb-core"))
    implementation(project(":evolvdb-storage-record"))
    implementation(project(":evolvdb-storage-page"))
    implementation(project(":evolvdb-storage-disk"))
}

application {
    mainClass.set("io.github.anupam.evolvdb.cli.Main")
}
