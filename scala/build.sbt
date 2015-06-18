name := "thunder_streaming"

version := "0.1.0.dev"

scalaVersion := "2.10.3"

libraryDependencies += "org.apache.hadoop" % "hadoop-core" % "1.2.0" % "provided"

resolvers += "Thrift" at "http://people.apache.org/~rawson/repo/"
resolvers += "Apache HBase" at "https://repository.apache.org/content/repositories/releases"

libraryDependencies += "org.apache.hbase" % "hbase" % "0.98.7-hadoop1" 
libraryDependencies += "org.apache.hbase" % "hbase-client" % "0.98.7-hadoop1" 
libraryDependencies += "org.apache.hbase" % "hbase-server" % "0.98.7-hadoop1" % "provided"
libraryDependencies += "org.apache.hbase" % "hbase-common" % "0.98.7-hadoop1" 
libraryDependencies += "org.apache.hbase" % "hbase-protocol" % "0.98.7-hadoop1" 

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.2.1" % "provided" excludeAll(
  ExclusionRule(organization = "org.apache.hadoop")
  )

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.2.1" % "provided" excludeAll(
  ExclusionRule(organization = "org.apache.hadoop")
  )

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"

resolvers += "spray" at "http://repo.spray.io/"

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Spray Repository" at "http://repo.spray.cc/")
