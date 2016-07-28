// You may use this file to add plugin dependencies for sbt.

resolvers ++= Seq(
  "Central" at "https://oss.sonatype.org/content/repositories/releases/"
)

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.8.2")

addSbtPlugin("com.github.gseitz" % "sbt-release" % "0.8.5")

addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.0.0")

addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.0.5")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.6.0")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.8.2")
