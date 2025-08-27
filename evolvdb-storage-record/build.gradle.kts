plugins {
    `java-library`
}

dependencies {
    api(project(":evolvdb-storage-page"))
    implementation(project(":evolvdb-storage-buffer"))
    implementation(project(":evolvdb-storage-disk"))
    implementation(project(":evolvdb-config"))
    implementation(project(":evolvdb-common"))
}
