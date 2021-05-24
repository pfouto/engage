import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.4.21"
    application
    id("com.github.johnrengelman.shadow") version "6.1.0"

}

group = "me.pfouto"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
    maven {
        url = uri("https://jitpack.io")
    }
}

dependencies {
    implementation("com.google.code.gson:gson:2.8.6")
    implementation("com.github.pfouto:babel-core:0.4.39")
    implementation("com.datastax.oss:java-driver-core:4.11.1")
    testImplementation(kotlin("test-junit"))
}

tasks.test {
    useJUnit()
}

application {
    mainClassName = "MainKt"
}

tasks {
    withType<KotlinCompile> {
        kotlinOptions.jvmTarget = "11"
    }

    shadowJar {
        // defaults to project.name
        //archiveBaseName.set("${project.name}-fat")
        // defaults to all, so removing this overrides the normal, non-fat jar
        archiveClassifier.set("")
    }
}

tasks.withType<Jar> {
    manifest {
        attributes["Main-Class"] = application.mainClassName
        attributes["Multi-Release"] = "true"
    }
}