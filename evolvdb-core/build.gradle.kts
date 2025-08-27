plugins {
    `java-library`
}

dependencies {
    // Expose types used in Database public API via api (transitive to consumers like CLI)
    api(project(":evolvdb-config"))
    api(project(":evolvdb-storage-disk"))
    api(project(":evolvdb-storage-buffer"))
    api(project(":evolvdb-catalog"))

    // Internal implementation details
    implementation(project(":evolvdb-storage-page"))
    implementation(project(":evolvdb-types"))
    implementation(project(":evolvdb-common"))
}
