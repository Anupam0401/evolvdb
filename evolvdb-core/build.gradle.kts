plugins {
    `java-library`
}

dependencies {
    // Expose types used in Database public API via api (transitive to consumers like CLI)
    api(project(":evolvdb-config"))
    api(project(":evolvdb-storage-disk"))
    api(project(":evolvdb-storage-buffer"))
    api(project(":evolvdb-catalog"))
    api(project(":evolvdb-types"))

    // SQL pipeline (internal wiring for execute(sql))
    implementation(project(":evolvdb-sql"))
    implementation(project(":evolvdb-planner"))
    implementation(project(":evolvdb-exec"))

    // Internal implementation details
    implementation(project(":evolvdb-storage-page"))
    implementation(project(":evolvdb-common"))
}
