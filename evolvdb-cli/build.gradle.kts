plugins {
    application
}

dependencies {
    implementation(project(":evolvdb-config"))
    implementation(project(":evolvdb-core"))
    implementation(project(":evolvdb-types"))
    implementation(project(":evolvdb-storage-record"))
    implementation(project(":evolvdb-storage-page"))
    implementation(project(":evolvdb-storage-disk"))

    // Database.execute() internally uses the SQL pipeline; needed at runtime
    runtimeOnly(project(":evolvdb-sql"))
    runtimeOnly(project(":evolvdb-planner"))
    runtimeOnly(project(":evolvdb-exec"))
}

application {
    mainClass.set("io.github.anupam.evolvdb.cli.Main")
}
