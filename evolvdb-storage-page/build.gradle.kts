plugins {
    `java-library`
}

dependencies {
    api(project(":evolvdb-storage-disk"))
    api(project(":evolvdb-types"))
    implementation(project(":evolvdb-common"))
}
