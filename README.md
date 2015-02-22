
Spark-Allegrograph
=================


Description
=
This project outputs a single fat that may be submitted via spark-submit command or uploaded to spark job-server.
All jobs implement proper job-server interface and the configuration layer is unified.
A job may be configured via cli params (spark-submit) or input HOCON/json configuration file (job-server)
In HOCON file always use the long parameter names. CLI params may be short.


Building
=
In order to build the target jar a single time action is required - running
an ant task that imports custom dependencies into local maven repository:

<pre># ant</pre>

If the maven repository is synced then

<pre># mvn package</pre>

should build the target jar located in target/ directory: mldemo-driver-jobs-0.0.1-SNAPSHOT.jar
 

