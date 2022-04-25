hdfs dfs -rm -r -f hw
rm *.class rm *.jar
javac -classpath "$(yarn classpath)" -d . CleanMapper.java
javac -classpath "$(yarn classpath)":. -d . Clean.java
jar -cvf Clean.jar *.class



hdfs dfs -mkdir hw 

hdfs dfs -mkdir hw/input

hdfs dfs -put nft_unclean.csv hw/input
hdfs dfs -ls hw/input
hadoop jar Clean.jar Clean hw/input/nft_unclean.csv /user/yz5077/hw/output
hdfs dfs -cat hw/output/part-r-00000
