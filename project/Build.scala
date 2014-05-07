import com.typesafe.sbt.SbtScalariform
import sbt._
import sbt.Keys._

object Build extends sbt.Build {

  lazy val eskka = Project(
    id = "eskka",
    base = file("."),
    settings = standardSettings
  ).settings(
      mainClass := Some("eskka.Boot"),

      resolvers ++= Seq(Resolver.sonatypeRepo("releases"), Resolver.typesafeRepo("releases")),

      libraryDependencies ++= Seq(

        "com.google.guava" % "guava" % "16.0.1",

        "org.elasticsearch" % "elasticsearch" % v.elasticsearch,

        "com.typesafe.akka" %% "akka-actor" % v.akka,
        "com.typesafe.akka" %% "akka-contrib" % v.akka,
        "com.typesafe.akka" %% "akka-cluster" % v.akka,

        "com.typesafe.akka" %% "akka-multi-node-testkit" % v.akka % "test"

      )
    )

  lazy val pack = TaskKey[File]("pack")

  def standardSettings = Defaults.defaultSettings ++ Seq(
    organization := "eskka",
    scalaVersion := "2.11.0",
    version := "0.2.0-SNAPSHOT",
    scalacOptions := Seq("-unchecked", "-deprecation", "-feature", "-encoding", "utf8")
  ) ++ packTask ++ formatSettings

  def packTask = pack <<= (name, update, packageBin in Compile, target, version) map {
    (name, updateReport, jar, out, v) =>
      val archive = out / s"$name-$v.zip"
      IO.zip(updateReport.matching(configurationFilter("runtime")).map(f => f -> f.getName) ++ Seq(jar -> jar.getName), archive)
      archive
  }

  object v {
    val elasticsearch = "1.1.1"
    val akka = "2.3.2"
  }

  lazy val formatSettings = SbtScalariform.scalariformSettings ++ Seq(
    SbtScalariform.ScalariformKeys.preferences in Compile  := formattingPreferences,
    SbtScalariform.ScalariformKeys.preferences in Test     := formattingPreferences
  )

  def formattingPreferences = {
    import scalariform.formatter.preferences._
    FormattingPreferences()
      .setPreference(AlignParameters, true)
      .setPreference(AlignSingleLineCaseStatements, true)
  }

}
