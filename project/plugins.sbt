resolvers += "jgit-repo" at "http://download.eclipse.org/jgit/maven"

resolvers += "Sonatype OSS Releases" at "https://oss.sonatype.org/service/local/staging/deploy/maven2"

addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "0.6.2")

addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.4.0")

