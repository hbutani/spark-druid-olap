name := "spark-druid-olap"

version := "0.0.1"

organization := "org.sparklinedata"

scalaVersion := "2.10.4"

parallelExecution in Test := false

crossScalaVersions := Seq("2.10.4", "2.11.6")

sparkVersion := "1.4.0"

spName := "Sparklinedata/spark-druid-olap"

//spAppendScalaVersion := true

scalacOptions += "-feature"


// All Spark Packages need a license
licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))


// Add Spark components this package depends on, e.g, "mllib", ....
sparkComponents ++= Seq("sql")

credentials += Credentials(Path.userHome / ".github.cred")

// uncomment and change the value below to change the directory where your zip artifact will be created
// spDistDirectory := target.value

// add any Spark Package dependencies using spDependencies.
// e.g. spDependencies += "databricks/spark-avro:0.1"

resolvers += "JitPack.IO" at "https://jitpack.io"

val httpclientVersion = "4.5"
val json4sVersion = "3.2.11"
val scalatestVersion = "2.2.4"
val sparkdateTimeVersion = "0.0.1"

libraryDependencies ++= Seq(
  "org.apache.httpcomponents" % "httpclient" % httpclientVersion,
  "org.json4s" %% "json4s-jackson" % json4sVersion,
  "com.github.SparklineData" % "spark-datetime" % sparkdateTimeVersion,
  "org.scalatest" %% "scalatest" % scalatestVersion % "test"
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
