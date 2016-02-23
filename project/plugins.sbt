// You may use this file to add plugin dependencies for sbt.

resolvers += "Spark Packages repo" at "https://dl.bintray.com/spark-packages/maven/"

addSbtPlugin("org.spark-packages" %% "sbt-spark-package" % "0.2.0")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.6.0")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.8.2")
