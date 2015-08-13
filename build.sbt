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

resolvers ++= Seq(
  "JitPack.IO" at "https://jitpack.io",
  Resolver.sonatypeRepo("public")
)

val httpclientVersion = "4.5"
val json4sVersion = "3.2.10"
val scalatestVersion = "2.2.4"
val sparkdateTimeVersion = "9f6589a01fdd50250a8e155d5710490e1dd353b3"
val scoptVersion = "3.3.0"

libraryDependencies ++= Seq(
  "org.apache.httpcomponents" % "httpclient" % httpclientVersion,
  //"org.json4s" %% "json4s-native" % json4sVersion,
  "org.json4s" %% "json4s-ext" % json4sVersion,
  "com.github.SparklineData" % "spark-datetime" % sparkdateTimeVersion,
  "com.github.scopt" %% "scopt" % scoptVersion,
  "org.scalatest" %% "scalatest" % scalatestVersion % "test",
  "com.databricks" %% "spark-csv" % "1.1.0" % "test"
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

test in assembly := {}
