plugins {
    `java-library`
}

dependencies {
    implementation(project(":evolvdb-types"))
    implementation(project(":evolvdb-catalog"))
    implementation(project(":evolvdb-sql"))
    implementation(project(":evolvdb-planner"))
    implementation(project(":evolvdb-storage-page"))  // For RecordId

    testImplementation(project(":evolvdb-core"))
}
