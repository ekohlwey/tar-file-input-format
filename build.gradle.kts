plugins {
    id("java")
}

group = "com.edkohlwey.hadoop"
version = "1.0.0"

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

repositories {
    mavenCentral()
}

dependencies {
    val hadoopVersion = "3.3.6"
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.junit.vintage:junit-vintage-engine")
    testImplementation("org.hamcrest:hamcrest:2.2")
    testImplementation("org.apache.hadoop:hadoop-minicluster:${hadoopVersion}")
    implementation("org.apache.hadoop:hadoop-mapreduce-client-core:${hadoopVersion}")
    implementation("org.apache.hadoop:hadoop-common:${hadoopVersion}")
    implementation("org.apache.commons:commons-compress:1.21")
}

tasks.test {
    useJUnitPlatform()
}