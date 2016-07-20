name := "bpmNextTestClient"
version := "1.0"
scalaVersion := "2.10.6"
//javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
resolvers += "Restlet" at "http://maven.restlet.com"
resolvers += "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases"
resolvers += "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"
val ivyLocal = Resolver.file("local", file(Path.userHome.absolutePath + "/.ivy2/local"))

//libraryDependencies ++= {
//  val sprayVersion = "1.3.1"
//  val akkaVersion = "2.3.9"
//  Seq(
//    "io.spray" %% "spray-json"   % "1.3.2",
//    "com.typesafe.akka" %% "akka-actor" % akkaVersion,
//    "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
//    "io.spray" %  "spray-client" % sprayVersion,
//    "ch.qos.logback" % "logback-classic" % "1.0.7"
//  )
//}
//libraryDependencies += "com.typesafe.play" %% "play-ws" % "2.4.8"

transitiveClassifiers := Seq("sources", "javadoc")

libraryDependencies ++= Seq(
  ("com.typesafe.play" %% "play-ws" % "2.4.8").
    exclude("commons-logging", "commons-logging")
)
mainClass in (Compile, run) := Some("com.ibm.cicto.MyApp")



