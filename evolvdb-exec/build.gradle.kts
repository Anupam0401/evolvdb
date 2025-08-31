plugins {
    `java-library`
}

dependencies {
    implementation(project(":evolvdb-types"))
    implementation(project(":evolvdb-catalog"))
    implementation(project(":evolvdb-sql"))
    implementation(project(":evolvdb-planner"))

    testImplementation(project(":evolvdb-core"))
}
