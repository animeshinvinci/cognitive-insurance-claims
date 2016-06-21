name := "MessageHub_Spark_Bluemix"
version := "1.0"
scalaVersion := "2.10.6"

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
resolvers += "Restlet" at "http://maven.restlet.com"
resolvers += "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases"
val ivyLocal = Resolver.file("local", file(Path.userHome.absolutePath + "/.ivy2/local"))

//Here are the Spark Dependencies
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.6.0" // % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.6.0"  % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-yarn" % "1.6.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.6.0"% "provided"


//Add Kafka 0.9 Support
libraryDependencies += "org.apache.kafka" % "kafka-log4j-appender" % "0.9.0.0"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.9.0.0"
libraryDependencies += "org.apache.kafka" %% "kafka" % "0.9.0.0"

//Add Object Storage/Swift Support
libraryDependencies += "org.apache.hadoop" % "hadoop-openstack" % "2.6.0" % "provided"


transitiveClassifiers := Seq("sources", "javadoc")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-yarn" % "1.6.0" % "provided" excludeAll ExclusionRule(organization = "org.apache.hadoop"),
  "org.apache.hadoop" % "hadoop-client" % "2.4.0" % "provided",
  "org.apache.hadoop" % "hadoop-yarn-client" % "2.4.0" % "provided",
  "org.apache.hadoop" % "hadoop-hdfs" % "2.4.0" % "provided",
  "org.scalaz"                  %% "scalaz-core"                 % "7.1.3",
  "org.scalaz"                  %% "scalaz-effect"               % "7.1.3"
  
)

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last

    case PathList("org", "apache", xs @ _*) => MergeStrategy.last
    case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
    case PathList("scala", xs @ _*) => MergeStrategy.discard
    case "about.html" => MergeStrategy.rename
    case x => old(x)
  }
}

