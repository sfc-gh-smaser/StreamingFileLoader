#!/bin/sh

java -cp lib/snowflake-ingest-sdk-1.0.2-beta.7.jar:lib/slf4j-api-2.0.6.jar:lib/slf4j-simple-2.0.6.jar --add-opens=java.base/java.nio=ALL-UNNAMED -jar StreamingFileLoader.jar snowflake.properties sample1.csv
