import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import org.gradle.api.tasks.testing.logging.TestLogEvent.*

plugins {
  java
  application
  id("com.github.johnrengelman.shadow") version "7.0.0"
  id("io.spring.dependency-management") version "1.0.1.RELEASE"
  id("io.freefair.lombok") version "6.4.3"
}

group = "com.cheetahdigital.senderspoc"
version = "1.0.0-SNAPSHOT"

repositories {
  mavenCentral()
}

val commonsCodecVersion = "1.15"
val flywayVersion = "8.2.0"
val junitJupiterVersion = "5.8.2"
val jacksonVersion = "2.13.1"
val mySqlConnectorVersion = "8.0.28"
val slf4jApiVersion = "1.7.36"
val vertxVersion = "4.2.7"

val mainVerticleName = "com.cheetahdigital.senderspoc.SendersVerticle"
val launcherClassName = "io.vertx.core.Launcher"

val watchForChange = "src/**/*"
val doOnChange = "${projectDir}/gradlew classes"

application {
  mainClass.set(launcherClassName)
}

dependencyManagement {
  imports {
    mavenBom("org.apache.logging.log4j:log4j-bom:2.17.2")
  }
}

dependencies {
  implementation("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")
  implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$jacksonVersion")
  implementation("commons-codec:commons-codec:$commonsCodecVersion")
  implementation("io.vertx:vertx-config")
  implementation("io.vertx:vertx-config-yaml")
  implementation("io.vertx:vertx-core")
  implementation("io.vertx:vertx-mysql-client")
  implementation("io.vertx:vertx-redis-client")
  implementation(platform("io.vertx:vertx-stack-depchain:$vertxVersion"))
  implementation("io.vertx:vertx-sql-client-templates")
  implementation("io.vertx:vertx-web")
  implementation("io.vertx:vertx-zookeeper")
  implementation("org.apache.logging.log4j:log4j-api")
  implementation("org.apache.logging.log4j:log4j-core")
  implementation("org.apache.logging.log4j:log4j-slf4j-impl")
  implementation("org.flywaydb:flyway-core:$flywayVersion")
  implementation("org.slf4j:slf4j-api:$slf4jApiVersion")

  runtimeOnly("mysql:mysql-connector-java:$mySqlConnectorVersion")

  testImplementation("io.vertx:vertx-junit5")
  testImplementation("org.junit.jupiter:junit-jupiter:$junitJupiterVersion")
}

java {
  sourceCompatibility = JavaVersion.VERSION_11
  targetCompatibility = JavaVersion.VERSION_11
}

tasks.withType<ShadowJar> {
  archiveClassifier.set("fat")
  manifest {
    attributes(mapOf("Main-Verticle" to mainVerticleName))
  }
  mergeServiceFiles()
}

tasks.withType<Test> {
  useJUnitPlatform()
  testLogging {
    events = setOf(PASSED, SKIPPED, FAILED)
  }
}

tasks.withType<JavaExec> {
  args = listOf(
    "run",
    mainVerticleName,
    "--redeploy=$watchForChange",
    "--launcher-class=$launcherClassName",
    "--on-redeploy=$doOnChange"
  )
}
