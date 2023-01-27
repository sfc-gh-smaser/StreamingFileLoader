#!/bin/sh
 javac -cp lib/snowflake-ingest-sdk-1.0.2-beta.7.jar:lib/slf4j-api-2.0.6.jar:lib/slf4j-simple-2.0.6.jar:classes -d classes src/snowflake/demo/*.java -Xlint:unchecked

 jar cfm StreamingFileLoader.jar manifest.txt -C classes snowflake src snowflake.properties sample1.csv



