name := "trainWatson"
version := "1.0"
scalaVersion := "2.10.6"
//javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
resolvers += "Restlet" at "http://maven.restlet.com"
resolvers += "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases"
resolvers += "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"
val ivyLocal = Resolver.file("local", file(Path.userHome.absolutePath + "/.ivy2/local"))

transitiveClassifiers := Seq("sources", "javadoc")

libraryDependencies ++= Seq(
  ("com.ibm.watson.developer_cloud" % "java-sdk" % "3.0.1").
    exclude("commons-logging", "commons-logging")
)
mainClass in (Compile, run) := Some("com.ibm.cicto.TrainWatsonImage")



