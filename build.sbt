name := "ScreamingTwitter"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.3.1",
  "org.apache.spark" %% "spark-streaming" % "1.3.1",
  "org.apache.spark" %% "spark-streaming-twitter" % "1.3.1",
  "com.typesafe" % "config" % "1.2.1"
)

resolvers ++= Seq(
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Akka Repository" at "http://repo.akka.io/releases/"
)