import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import org.gradle.api.tasks.JavaExec
import org.gradle.api.tasks.bundling.Jar
plugins {
    application
    java
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

group = "work.lcod"
version = "0.1.15"

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
    implementation("org.tomlj:tomlj:1.1.0")
    implementation("org.apache.commons:commons-csv:1.11.0")
    implementation("org.graalvm.js:js:25.0.1")
    implementation("org.graalvm.js:js-scriptengine:25.0.1")
    implementation("org.graalvm.truffle:truffle-api:25.0.1")
    implementation("org.graalvm.sdk:graal-sdk:25.0.1")

    testImplementation("org.junit.jupiter:junit-jupiter:5.11.0")
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
        showExceptions = true
        showCauses = true
        showStackTraces = true
    }
    reports {
        html.required.set(true)
        junitXml.required.set(true)
    }
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

tasks.register("lcodRunJar") {
    group = "distribution"
    description = "Build the lcod-run fat jar suitable for java -jar"
    dependsOn(tasks.named<ShadowJar>("shadowJar"))
    doLast {
        val jar = tasks.named<ShadowJar>("shadowJar").get().archiveFile.get().asFile
        logger.lifecycle("lcod-run jar available at ${jar.absolutePath}")
    }
}

tasks.register<JavaExec>("specTests") {
    group = "verification"
    description = "Run LCOD spec fixtures using the Java kernel"
    mainClass.set("work.lcod.kernel.tooling.SpecTestRunner")
    classpath = sourceSets.main.get().runtimeClasspath
    val argsProp = (project.findProperty("specArgs") as String?)?.trim()
    if (!argsProp.isNullOrBlank()) {
        args = argsProp.split(" ").filter { it.isNotBlank() }
    }
}

tasks.register("lcodRunnerLib") {
    group = "distribution"
    description = "Assemble the thin kernel jar and its runtime dependencies for embedding"
    dependsOn(tasks.named("jar"))

    val outputDir = layout.buildDirectory.dir("lcod-runner")

    doLast {
        val jarTask = tasks.named<Jar>("jar").get()
        val jarFile = jarTask.archiveFile.get().asFile
        val targetLibDir = outputDir.get().asFile.resolve("libs")
        if (!targetLibDir.exists()) targetLibDir.mkdirs()

        copy {
            from(jarFile)
            into(targetLibDir)
        }

        copy {
            from(configurations.runtimeClasspath.get().filter { it.name.endsWith(".jar") })
            into(targetLibDir)
        }

        logger.lifecycle("Embedding bundle available at ${targetLibDir.absolutePath}")
    }
}
