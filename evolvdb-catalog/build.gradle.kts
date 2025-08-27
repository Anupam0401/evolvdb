plugins {
    `java-library`
}

dependencies {
    api(project(":evolvdb-types"))
    implementation(project(":evolvdb-storage-disk"))
    implementation(project(":evolvdb-common"))
}
