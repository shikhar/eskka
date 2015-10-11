import sbt._
import sbt.Keys._

import java.io.File

import com.typesafe.sbt.S3Plugin._

object Build extends sbt.Build {

  lazy val eskka = Project(
    id = "eskka",
    base = file(".")
  ).settings(
      organization := "eskka",
      description := "Elasticsearch discovery plugin using Akka Cluster",
      scalaVersion := "2.11.7",
      version := "0.13.0-SNAPSHOT",
      scalacOptions := Seq("-unchecked", "-deprecation", "-feature", "-encoding", "utf8")
    ).settings(
        s3Settings ++ Seq(
          credentials += Credentials(Path.userHome / ".s3credentials"),
          S3.host in S3.upload := "eskka.s3.amazonaws.com",
          mappings in S3.upload <<= (name, version, target) map { (name, v, out) => Seq((out / s"$name-$v.zip", s"$name-$v.zip")) },
          S3.upload <<= S3.upload dependsOn pack
        ): _*
      ).settings(
          pack <<= packTask
        ).settings(

            resolvers ++= Seq(Resolver.sonatypeRepo("releases"), Resolver.typesafeRepo("releases")),

            libraryDependencies ++= ("org.elasticsearch" % "elasticsearch" % V.elasticsearch % "provided") :: List(
              "com.typesafe.akka" %% "akka-actor" % V.akka,
              "com.typesafe.akka" %% "akka-cluster" % V.akka,
              "com.typesafe.akka" %% "akka-cluster-tools" % V.akka
            ).map(_.exclude(org = "io.netty", name = "*")) // ES provides Netty
          )

  lazy val pack = TaskKey[File]("pack")

  def packTask = Def.task {
    val archivePath = target.value / s"${name.value}-${version.value}.zip"

    val pluginDescriptorTempFile = {
      val f = File.createTempFile(name.value, "tmp")
      IO.write(f,
        s"""name=${name.value}
           |version=${version.value}
           |description=${description.value}
           |site=false
           |jvm=true
           |classname=eskka.EskkaDiscoveryPlugin
           |java.version=${V.java}
           |elasticsearch.version=${V.elasticsearch}
           |""".stripMargin)
      f
    }

    val jar = (packageBin in Compile).value

    val dependencies = update.value.select(configuration = configurationFilter("runtime"))

    IO.zip(List(jar -> jar.getName, pluginDescriptorTempFile -> "plugin-descriptor.properties") ++ dependencies.map(f => f -> f.getName),
      archivePath)

    pluginDescriptorTempFile.delete()

    archivePath
  }

  object V {
    val java = "1.8"
    val elasticsearch = "2.0.0-rc1"
    val akka = "2.4.0"
  }

}
