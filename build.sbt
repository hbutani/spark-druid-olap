name := "spark-druid-olap"

version := "0.0.1"

organization := "SparklineData"

scalaVersion := "2.10.4"

parallelExecution in Test := false

crossScalaVersions := Seq("2.10.4", "2.11.6")

sparkVersion := "1.5.1"

spName := "SparklineData/spark-druid-olap"

//spAppendScalaVersion := true

scalacOptions += "-feature"


// All Spark Packages need a license
licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))


// Add Spark components this package depends on, e.g, "mllib", ....
sparkComponents ++= Seq("sql", "hive")

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
val sparkdateTimeVersion = "bf5693a575a1dea5b663e4e8b30a0ba94c21d62d"
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

spShortDescription := "Spark Druid Package" // Your one line description of your package

spDescription := """Spark-Druid package enables'Logical Plans' written against a raw event dataset
                     to be rewritten to take advantage of a Drud Index of the Event data. It
                     comprises of a 'Druid DataSource' that wraps the 'raw event dataset', and a
                     'Druid Planner' that contains a set of Rewrite Rules to convert
                     'Project-Filter-Aggregation-Having-Sort-Limit' plans to Druid Index Rest calls.""".stripMargin
