name := "NYSK"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core"  % "2.1.0" % "provided",
	"org.apache.spark" %% "spark-mllib" % "2.1.0" % "provided"
)

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}
