#!/bin/sh

cd src/
mvn package
$SPARK_INSTALL/bin/spark-submit --class "SpeedZones" --master yarn ./target/simple-project-1.0.jar
cd -