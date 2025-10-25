plugins {
    application
    java
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

group = "work.lcod"
version = "0.1.0-SNAPSHOT"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
    withSourcesJar()
}

application {
    mainClass = "work.lcod.kernel.cli.Main"
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("info.picocli:picocli:4.7.6")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.1")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.17.1")

    testImplementation("org.junit.jupiter:junit-jupiter:5.11.0")
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
}

tasks.test {
    useJUnitPlatform()
}

tasks.jar {
    manifest {
        attributes(
            "Main-Class" to application.mainClass.get(),
            "Implementation-Title" to "lcod-kernel-java",
            "Implementation-Version" to project.version
        )
    }
}

tasks.shadowJar {
    archiveBaseName.set("lcod-run")
    archiveClassifier.set("")
    archiveVersion.set(project.version.toString())
    mergeServiceFiles()
}

tasks.build {
    dependsOn(tasks.shadowJar)
}
