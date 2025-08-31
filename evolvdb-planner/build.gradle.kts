plugins {
    `java-library`
}

dependencies {
    implementation(project(":evolvdb-types"))
    implementation(project(":evolvdb-catalog"))
    implementation(project(":evolvdb-sql"))

    testImplementation(project(":evolvdb-core"))
}
