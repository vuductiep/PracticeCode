# PracticeCode
Submit to spark on amazon web service

spark-submit --class "MixTest.sparkBasicStats" --master local --driver-memory 5G --executor-memory 5G --packages org.apache.spark:spark-mllib-local_2.11:2.0.1,com.amazonaws:aws-java-sdk:1.11.0,org.apache.hadoop:hadoop-common:2.7.3,org.apache.hadoop:hadoop-aws:2.7.3,commons-net:commons-net:3.5,asm:asm:3.3.1,org.xerial.snappy:snappy-java:1.1.2 scala_testing_2.11-0.1.jar 