name := "EphGraph"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies <<= scalaVersion {
    scala_version => Seq(
          ("org.apache.spark" % "spark-core_2.10" % "1.2.0").
          exclude("org.eclipse.jetty.orbit", "javax.mail.glassfish").
          exclude("org.eclipse.jetty.orbit", "javax.activation").
          exclude("com.esotericsoftware.kryo", "minlog").
          exclude("com.esotericsoftware.minlog", "minlog").
         exclude("commons-beanutils", "commons-beanutils").
          exclude("commons-beanutils", "commons-beanutils-core").
          exclude("commons-logging", "commons-logging").
          exclude("org.slf4j", "jcl-over-slf4j").
          exclude("org.apache.hadoop", "hadoop-yarn-common").
          exclude("org.apache.hadoop", "hadoop-yarn-api").
          exclude("org.eclipse.jetty.orbit", "javax.transaction").
          exclude("org.eclipse.jetty.orbit", "javax.servlet"),
          ("org.apache.spark" %% "spark-graphx" % "1.2.0").
          exclude("org.eclipse.jetty.orbit", "javax.mail.glassfish").
          exclude("org.eclipse.jetty.orbit", "javax.activation").
          exclude("com.esotericsoftware.kryo", "minlog").
          exclude("com.esotericsoftware.minlog", "minlog").
         exclude("commons-beanutils", "commons-beanutils").
          exclude("commons-beanutils", "commons-beanutils-core").
          exclude("commons-logging", "commons-logging").
          exclude("org.slf4j", "jcl-over-slf4j").
          exclude("org.apache.hadoop", "hadoop-yarn-common").
          exclude("org.apache.hadoop", "hadoop-yarn-api").
          exclude("org.eclipse.jetty.orbit", "javax.transaction").
          exclude("org.eclipse.jetty.orbit", "javax.servlet")
        )
}

//libraryDependencies ++= Seq (
 // "org.apache.spark" % "spark-core_2.11" % "1.2.0",
  //"org.apache.spark" % "spark-graphx_2.11" % "1.2.0"
  //"org.apache.spark" %% "spark-core_2.11" % "1.2.0",
  // "org.apache.spark" %% "spark-graphx" % "1.2.0"
//)
