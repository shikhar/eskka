import sbt._
import sbt.Keys._

import com.typesafe.sbt.SbtScalariform._

object Build extends sbt.Build {

  lazy val eskka = Project(
    id = "eskka",
    base = file("."),
    settings = standardSettings ++ scalariformSettings
  ).settings(
      mainClass := Some("eskka.Boot"),

      resolvers += Resolver.sonatypeRepo("releases"),

      libraryDependencies ++= Seq(
        "com.google.guava" % "guava" % "16.0.1",

        "org.elasticsearch" % "elasticsearch" % v.elasticsearch,

        "com.typesafe.akka" %% "akka-actor" % v.akka,
        "com.typesafe.akka" %% "akka-contrib" % v.akka,
        "com.typesafe.akka" %% "akka-cluster" % v.akka,
        "com.typesafe.akka" %% "akka-slf4j" % v.akka
      )
    )

  def standardSettings = Defaults.defaultSettings ++ Seq(
    organization := "eskka",
    scalaVersion := "2.10.4",
    version := "0.1",
    scalacOptions := Seq("-unchecked", "-deprecation", "-feature", "-encoding", "utf8")
  )

  object v {
    val elasticsearch = "1.1.0"
    val akka = "2.3.1"
  }

}