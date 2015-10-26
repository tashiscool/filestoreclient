import sbt._
import sbt.Keys._

object BuildSettings {
  val buildVersion = "2.3-SNAPSHOT"

  val buildSettings = Defaults.defaultSettings ++ Seq(
    organization := "com.github",
    version := buildVersion,
    scalaVersion := "2.10.4",
    scalacOptions ++= Seq("-unchecked", "-deprecation", "-target:jvm-1.6"),
    crossScalaVersions := Seq("2.11.1", "2.10.4"),
    crossVersion := CrossVersion.binary,
    shellPrompt := ShellPrompt.buildShellPrompt
  ) ++ Publish.settings ++ com.typesafe.sbt.SbtScalariform.scalariformSettings
}

object Publish {
  def targetRepository: Project.Initialize[Option[sbt.Resolver]] = version { (version: String) =>
    val nexus = "http://search.maven.com/nexus/content/repositories/"
    if (version.trim.endsWith("SNAPSHOT"))
      Some("Internal Snapshot Repository" at nexus + "ccp-snapshot")
    else
      Some("Internal Release Respository" at nexus + "ccp")

  }
  lazy val settings = Seq(
    publishMavenStyle := true,
    publishTo <<= targetRepository,
    publishArtifact in Test := false,
    pomIncludeRepository := { _ => false }
  )
}

// Shell prompt which show the current project,
// git branch and build version
object ShellPrompt {

  object devnull extends ProcessLogger {
    def info(s: => String) {}

    def error(s: => String) {}

    def buffer[T](f: => T): T = f
  }

  def currBranch = (
    ("git status -sb" lines_! devnull headOption)
      getOrElse "-" stripPrefix "## "
    )

  val buildShellPrompt = {
    (state: State) => {
      val currProject = Project.extract(state).currentProject.id
      "%s:%s:%s> ".format(
        currProject, currBranch, BuildSettings.buildVersion
      )
    }
  }
}

object FileStoreClientBuild extends Build {

  import BuildSettings._

  lazy val reactivemongo = Project(
    "filestore-client",
    file("."),
    settings = buildSettings ++ Seq(
      resolvers ++= Seq(
        "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository",
        "Internal Snapshot Repository" at "http://search.maven.com/nexus/content/repositories/snapshot",
        "Internal Release Respository" at "http://search.mavencom/nexus/content/repositories/release",
        "Spy Repository" at "http://files.couchbase.com/maven2",
        "Sonatype Release" at "https://oss.sonatype.org/content/repositories/releases",
        "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
        "Mirror" at "http://mirrors.ibiblio.org/pub/mirrors/maven2/",
        "Rhinofly Internal Repository" at "http://maven-repository.rhinofly.net:8081/artifactory/libs-release-local",
        "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases"
      ),
      libraryDependencies ++= Seq(
        "org.reactivemongo" %% "reactivemongo" % "0.10.5.0.akka23",
        "nl.rhinofly" %% "play-s3" % "5.0.2",
        "com.github.scala-incubator.io" %% "scala-io-file" % "0.4.3",
        "com.typesafe.play" %% "play" % "2.3.3",
        "junit" % "junit" % "4.8" % "test",
        "com.google.guava" % "guava" % "18.0",
        "com.typesafe" % "config" % "1.2.1",
        "com.github.seratch" %% "awscala" % "0.5.2"
      )
    )
  )
}
