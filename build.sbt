name := "Repayment"

version := "1.0"

scalaVersion := "2.10.6"
    
libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.5.2" % "provided",
    "org.apache.spark" %% "spark-mllib" % "1.5.2"
 )
