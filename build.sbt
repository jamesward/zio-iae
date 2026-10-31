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

// Library (Scala LTS, published)

lazy val core = (project in file("modules/core")).settings(name := "iae-core")

lazy val awsClient = (project in file("modules/aws-client")).settings(name := "iae-aws-client")

lazy val aws = (project in file("modules/aws")).dependsOn(core, awsClient).settings(name := "iae-aws")

lazy val testkit = (project in file("modules/testkit")).dependsOn(aws).settings(name := "iae-testkit")

lazy val engine = (project in file("modules/engine")).dependsOn(aws).settings(name := "iae-engine")

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

lazy val root = (project in file("."))
  .aggregate(core, awsClient, aws, testkit, engine, cli, agent, webhook)
  .settings(name := "zio-iae", publish / skip := true)

// SkillsJars: extract agent Skills with `./sbta extractSkillsJars`
skillsJarsOutputDir := Some(file(".kiro/skills"))

libraryDependencies += "com.jamesward" % "skills" % "0.0.21" % Skills
