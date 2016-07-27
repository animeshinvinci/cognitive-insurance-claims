name := "CallWatsonImage"
version := "2.0"
scalaVersion := "2.10.6"
javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
resolvers += "Restlet" at "http://maven.restlet.com"
resolvers += "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases"
resolvers += "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"

//Add for local debug build of Watson Java-SDK
resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"
val ivyLocal = Resolver.file("local", file(Path.userHome.absolutePath + "/.ivy2/local"))

libraryDependencies += "com.ibm.watson.developer_cloud" % "java-sdk" % "3.0.1"
libraryDependencies += "commons-codec" % "commons-codec" % "1.10"
libraryDependencies += "commons-io" % "commons-io" % "2.5"


transitiveClassifiers := Seq("sources", "javadoc")

libraryDependencies ++= Seq(
  "com.ibm.watson.developer_cloud" % "java-sdk" % "3.0.1", 
  "commons-io" % "commons-io" % "2.5" 
)



