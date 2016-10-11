
import sbt._

/**
  * to set a sparkversion run:
  * {{{buld/sbt compile -DsparkVersion=1.6.2}}}
  */
object SparkShim {

  val sparkVersion = sys.props.getOrElse("sparkVersion", default = "1.6.2")
  val sparkNamExt = if (sparkVersion == "1.6.1") "-onesixone" else ""
  val sparkVersion_161 = "1.6.1"
  val sparkVersion_162 = "1.6.2"
  val guava_version = "16.0.1"

  val spark161Dependencies = Seq(
    "com.google.guava" % "guava" % guava_version % "provided" force(),
    "org.apache.spark" %% "spark-core" % sparkVersion_161 % "provided",
    "org.apache.spark" %% "spark-sql" % sparkVersion_161 % "provided",
    "org.apache.spark" %% "spark-hive" % sparkVersion_161 % "provided",
    "org.apache.spark" %% "spark-hive-thriftserver" % sparkVersion_161 % "provided"
  )

  val spark162Dependencies = Seq(
    "com.google.guava" % "guava" % guava_version % "provided" force(),

    "org.apache.spark" %% "spark-core" % sparkVersion_162 % "provided",
    "org.apache.spark" %% "spark-sql" % sparkVersion_162 % "provided",
    "org.apache.spark" %% "spark-hive" % sparkVersion_162 % "provided",
    "org.apache.spark" %% "spark-hive-thriftserver" % sparkVersion_162 % "provided"
  )

  val sparkDependencies =
    if (sparkVersion == sparkVersion_161 ) spark161Dependencies else spark162Dependencies

}
