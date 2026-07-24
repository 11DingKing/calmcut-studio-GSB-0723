import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    kotlin("jvm") version "2.1.0"
    kotlin("plugin.serialization") version "2.1.0"
    application
    id("com.gradleup.shadow") version "8.3.5"
}

group = "com.calmcut"
version = "1.0.0"

repositories {
    mavenCentral()
}

val ktorVersion = "3.0.3"
val exposedVersion = "0.57.0"
val kafkaVersion = "3.9.0"

dependencies {
    // Ktor server
    implementation("io.ktor:ktor-server-core:$ktorVersion")
    implementation("io.ktor:ktor-server-netty:$ktorVersion")
    implementation("io.ktor:ktor-server-content-negotiation:$ktorVersion")
    implementation("io.ktor:ktor-serialization-kotlinx-json:$ktorVersion")
    implementation("io.ktor:ktor-server-status-pages:$ktorVersion")
    implementation("io.ktor:ktor-server-call-logging:$ktorVersion")

    // Serialization + coroutines
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.7.3")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.9.0")

    // Exposed + Postgres + HikariCP
    implementation("org.jetbrains.exposed:exposed-core:$exposedVersion")
    implementation("org.jetbrains.exposed:exposed-jdbc:$exposedVersion")
    implementation("org.jetbrains.exposed:exposed-java-time:$exposedVersion")
    implementation("org.postgresql:postgresql:42.7.4")
    implementation("com.zaxxer:HikariCP:6.2.1")

    // Kafka (Redpanda compatible)
    implementation("org.apache.kafka:kafka-clients:$kafkaVersion")

    // Config + logging
    implementation("com.typesafe:config:1.4.3")
    implementation("ch.qos.logback:logback-classic:1.5.12")

    // Tests
    testImplementation(kotlin("test"))
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.9.0")
    testImplementation("io.ktor:ktor-server-test-host:$ktorVersion")

    // End-to-end tests against real PostgreSQL + Redpanda (Kafka API) via Testcontainers.
    testImplementation("org.testcontainers:testcontainers:1.20.4")
    testImplementation("org.testcontainers:junit-jupiter:1.20.4")
    testImplementation("org.testcontainers:postgresql:1.20.4")
    testImplementation("org.testcontainers:redpanda:1.20.4")
}

kotlin {
    jvmToolchain(17)
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_17)
    }
}

application {
    mainClass.set("com.calmcut.studio.ApplicationKt")
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
    // Help Testcontainers find the Docker daemon under Docker Desktop / OrbStack /
    // Colima, where the socket is not at the default /var/run/docker.sock.
    val dockerHost = System.getenv("DOCKER_HOST")
        ?: listOf(
            "${System.getProperty("user.home")}/.orbstack/run/docker.sock",
            "${System.getProperty("user.home")}/.docker/run/docker.sock",
            "${System.getProperty("user.home")}/.colima/default/docker.sock",
            "/var/run/docker.sock",
        ).firstOrNull { file(it).exists() }?.let { "unix://$it" }
    if (dockerHost != null) {
        environment("DOCKER_HOST", dockerHost)
        systemProperty("testcontainers.docker.host", dockerHost)
    }
    // Pin the Docker API version: some daemons (OrbStack) reject the docker-java
    // default (1.32) and require >= 1.40. docker-java reads either the env var or
    // the `api.version` system property.
    environment("DOCKER_API_VERSION", "1.41")
    systemProperty("api.version", "1.41")
    // Ryuk (the resource reaper) can misbehave with rootless/alt sockets; disabling
    // is fine for local/CI ephemeral containers.
    environment("TESTCONTAINERS_RYUK_DISABLED", "true")
}

tasks.withType<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar> {
    archiveClassifier.set("all")
    mergeServiceFiles()
}
