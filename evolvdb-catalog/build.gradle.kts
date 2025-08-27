plugins {
    `java-library`
}

dependencies {
    api(project(":evolvdb-types"))
    implementation(project(":evolvdb-config"))
    implementation(project(":evolvdb-storage-disk"))
    implementation(project(":evolvdb-storage-buffer"))
    implementation(project(":evolvdb-storage-page"))
    implementation(project(":evolvdb-storage-record"))
    implementation(project(":evolvdb-common"))
}
