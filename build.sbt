// Bare settings apply to every subproject (sbt 2).
organization := "com.jamesward"

// Published libraries use the Scala LTS; applications override with Scala Next below.
scalaVersion := "3.9.0"

scalacOptions ++= Seq(
  "-language:strictEquality",
  "-deprecation",
  "-Werror",
)

licenses := Seq("Apache 2.0" -> uri("https://opensource.org/licenses/apache-2.0"))

homepage := Some(uri("https://github.com/jamesward/zio-iae"))

developers := List(
  Developer(
    "jamesward",
    "James Ward",
    "james@jamesward.com",
    uri("https://jamesward.com")
  )
)

ThisBuild / versionScheme := Some("semver-spec")

val scalaNext = "3.10.0"

val zioVersion = "2.1.27"

val zioSchemaVersion = "1.9.0"

val zioTest = Seq(
  "dev.zio" %% "zio-test-sbt" % zioVersion % Test,
)

fork := true

// JDK 25: allow the sun.misc.Unsafe and native access that scala-library and netty use, instead of warning.
javaOptions ++= Seq("--enable-native-access=ALL-UNNAMED", "--sun-misc-unsafe-memory-access=allow")

// Library (Scala LTS, published)

lazy val core = (project in file("modules/core")).settings(
  name := "iae-core",
  libraryDependencies ++= Seq(
    "dev.zio" %% "zio-streams"           % zioVersion,
    "dev.zio" %% "zio-schema-derivation" % zioSchemaVersion,
    "dev.zio" %% "zio-schema-json"       % zioSchemaVersion,
  ) ++ zioTest,
)

lazy val awsClient = (project in file("modules/aws-client")).settings(
  name := "iae-aws-client",
  libraryDependencies ++= Seq(
    "dev.zio"                %% "zio-http"              % "3.11.6",
    "dev.zio"                %% "zio-schema-derivation" % zioSchemaVersion,
    "dev.zio"                %% "zio-schema-json"       % zioSchemaVersion,
    "org.scala-lang.modules" %% "scala-xml"             % "2.5.0",
  ) ++ zioTest,
)

lazy val testkit = (project in file("modules/testkit"))
  .dependsOn(core, awsClient)
  .settings(
    name := "iae-testkit",
    libraryDependencies ++= Seq(
      "io.floci" % "testcontainers-floci-aws" % "2.17.0",
    ) ++ zioTest,
  )

lazy val aws = (project in file("modules/aws"))
  .dependsOn(core, awsClient, testkit % Test)
  .settings(name := "iae-aws", libraryDependencies ++= zioTest)

lazy val engine = (project in file("modules/engine"))
  .dependsOn(aws, testkit % Test)
  .settings(name := "iae-engine", libraryDependencies ++= zioTest)

// Applications (Scala Next, not published)

lazy val cli = (project in file("modules/cli"))
  .dependsOn(engine)
  .enablePlugins(JavaAppPackaging)
  .settings(name := "iae-cli", scalaVersion := scalaNext, publish / skip := true)

lazy val agent = (project in file("modules/agent"))
  .dependsOn(engine)
  .enablePlugins(JavaAppPackaging)
  .settings(name := "iae-agent", scalaVersion := scalaNext, publish / skip := true)

lazy val webhook = (project in file("modules/webhook"))
  .settings(name := "iae-webhook", scalaVersion := scalaNext, publish / skip := true)

lazy val photoApp = (project in file("examples/photo-app"))
  .dependsOn(aws, testkit % Test)
  .enablePlugins(JavaAppPackaging)
  .settings(
    name := "photo-app",
    scalaVersion := scalaNext,
    publish / skip := true,
    libraryDependencies ++= zioTest,
  )

lazy val root = (project in file("."))
  .aggregate(core, awsClient, aws, testkit, engine, cli, agent, webhook, photoApp)
  .settings(name := "zio-iae", publish / skip := true)

// SkillsJars: extract agent Skills with `./sbta extractSkillsJars`
skillsJarsOutputDir := Some(file(".kiro/skills"))

libraryDependencies += "com.jamesward" % "skills" % "0.0.21" % Skills
