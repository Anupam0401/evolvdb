plugins {
    application
}

dependencies {
    implementation(project(":evolvdb-config"))
    implementation(project(":evolvdb-core"))
    implementation(project(":evolvdb-types"))

    runtimeOnly(project(":evolvdb-sql"))
    runtimeOnly(project(":evolvdb-planner"))
    runtimeOnly(project(":evolvdb-exec"))
}

application {
    mainClass.set("io.github.anupam.evolvdb.server.TcpServer")
}
