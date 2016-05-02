name := "CallWatsonImage"
version := "1.0"
scalaVersion := "2.10.6"
javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
resolvers += "Restlet" at "http://maven.restlet.com"
resolvers += "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases"
resolvers += "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"
val ivyLocal = Resolver.file("local", file(Path.userHome.absolutePath + "/.ivy2/local"))

libraryDependencies += "com.ibm.watson.developer_cloud" % "java-sdk" % "2.9.0"
libraryDependencies += "commons-codec" % "commons-codec" % "1.10"



transitiveClassifiers := Seq("sources", "javadoc")

libraryDependencies ++= Seq(
  "com.ibm.watson.developer_cloud" % "java-sdk" % "2.9.0"
)



