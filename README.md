# spark-powerbi-connector
This is the source code of Spark to PowerBI connector using PowerBI REST client. 

## Project References

### Maven

#### Repository
    <repository>
      <id>spark-powerbi-connector</id>
      <url>https://raw.github.com/hdinsight/spark-powerbi-connector/maven-repo/</url>
      <snapshots>
        <enabled>true</enabled>
        <updatePolicy>always</updatePolicy>
      </snapshots>
    </repository>

#### Dependency
    <dependency>
      <groupId>com.microsoft.azure</groupId>
      <artifactId>spark-powerbi-connector_2.10</artifactId>
      <version>0.6.1</version>
    </dependency>
    
### SBT

#### Repository
    scalaVersion := "2.10.0"
    
    resolvers += "spark-powerbi-connector" at "https://raw.github.com/hdinsight/spark-powerbi-connector/maven-repo"

#### Dependency
    libraryDependencies ++= Seq(
        "com.microsoft.azure" %% "spark-powerbi-connector" % "0.6.1"
    )