plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.9.0"
}

rootProject.name = "evolvdb"
include(
    "evolvdb-common",
    "evolvdb-config",
    "evolvdb-types",
    "evolvdb-storage-disk",
    "evolvdb-storage-page",
    "evolvdb-storage-buffer",
    "evolvdb-storage-record",
    "evolvdb-catalog",
    "evolvdb-core",
    "evolvdb-cli",
    "evolvdb-server",
    "evolvdb-sql",
    "evolvdb-planner",
    "evolvdb-exec"
)