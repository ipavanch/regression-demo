name := "regression-demo"

version := "1.0"

scalaVersion := "2.11.4"

//lib resolvers
resolvers ++= Seq(
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/",
  "Restlet Repositories" at "http://maven.restlet.org",
  "Hortonworks Repositories" at "http://repo.hortonworks.com/content/repositories/releases/"
)
resolvers += Resolver.sonatypeRepo("snapshots")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.1", // % "provided",
  "org.apache.spark" %% "spark-sql" % "2.2.1", // % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.2.1" // % "provided"
)


assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}