import sbt._
import sbt.Keys._

import com.typesafe.sbt.SbtScalariform._
import com.typesafe.sbt.S3Plugin._

object Build extends sbt.Build {

  lazy val eskka = Project(
    id = "eskka",
    base = file("."),
    settings = Defaults.defaultSettings
  ).settings(
      organization := "eskka",
      scalaVersion := "2.11.4",
      version := "0.13.0-SNAPSHOT",
      scalacOptions := Seq("-unchecked", "-deprecation", "-feature", "-encoding", "utf8")
    ).settings(
      scalariformSettings ++ Seq(
        ScalariformKeys.preferences in Compile := formattingPreferences,
        ScalariformKeys.preferences in Test := formattingPreferences
      ): _*
    ).settings(
      s3Settings ++ Seq(
        credentials += Credentials(Path.userHome / ".s3credentials"),
        S3.host in S3.upload := "eskka.s3.amazonaws.com",
        mappings in S3.upload <<= (name, version, target) map { (name, v, out) => Seq((out / s"$name-$v.zip", s"$name-$v.zip")) },
        S3.upload <<= S3.upload dependsOn pack
      ): _*
    ).settings(
      packTask
    ).settings(

      resolvers ++= Seq(Resolver.sonatypeRepo("releases"), Resolver.typesafeRepo("releases")),

      libraryDependencies ++= Seq(

        "org.elasticsearch" % "elasticsearch" % v.elasticsearch % "provided",

        "com.typesafe.akka" %% "akka-actor" % v.akka,
        "com.typesafe.akka" %% "akka-cluster" % v.akka

      )
    )

  lazy val pack = TaskKey[File]("pack")

  def packTask = pack <<= (name, update, packageBin in Compile, target, version) map {
    (name, updateReport, jar, out, v) =>
      val archive = out / s"$name-$v.zip"
      IO.zip(updateReport.matching(configurationFilter("runtime")).map(f => f -> f.getName) ++ Seq(jar -> jar.getName), archive)
      archive
  }

  def formattingPreferences = {
    import scalariform.formatter.preferences._
    FormattingPreferences()
      .setPreference(AlignParameters, true)
      .setPreference(AlignSingleLineCaseStatements, true)
  }

  object v {
    val elasticsearch = "1.4.1"
    val akka = "2.3.7"
  }

}
