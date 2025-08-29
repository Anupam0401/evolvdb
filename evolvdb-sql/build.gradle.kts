plugins {
    `java-library`
}

dependencies {
    implementation(project(":evolvdb-types"))
    implementation(project(":evolvdb-catalog"))
    testImplementation(project(":evolvdb-core"))
}
