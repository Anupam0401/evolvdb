plugins {
    `java-library`
}

dependencies {
    implementation(project(":evolvdb-config"))
    implementation(project(":evolvdb-storage-disk"))
    implementation(project(":evolvdb-storage-buffer"))
    implementation(project(":evolvdb-storage-page"))
    implementation(project(":evolvdb-types"))
    implementation(project(":evolvdb-common"))
}
