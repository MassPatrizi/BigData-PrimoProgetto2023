# NON ESEGUIRE, COPIARE E INCOLLARE SUL TERMINALE DOPO AVER CREATO IL CLUSTER

# copy the project folder
scp -i ~/.ssh/pem/sajobigdatakeys.pem -r /Users/alessandro/Development/BigData/BigData-PrimoProgetto2023/Job2/Hadoop hadoop@ec2-54-174-74-38.compute-1.amazonaws.com:~

# copy the dataset
scp -i ~/.ssh/pem/sajobigdatakeys.pem -r /Users/alessandro/Development/BigData/BigData-PrimoProgetto2023/ReviewsCleaned.csv hadoop@ec2-54-174-74-38.compute-1.amazonaws.com:~

# connect to the master node
ssh -i .ssh/pem/sajobigdatakeys.pem hadoop@ec2-54-174-74-38.compute-1.amazonaws.com

# creare folders in the master node
hdfs dfs -mkdir /input
hdfs dfs -mkdir /output
hdfs dfs -put ReviewsCleaned.csv /input/ReviewsCleaned.csv

# launch mapReduce job
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -file Hadoop/mapper.py -mapper Hadoop/mapper.py -file Hadoop/reducer.py -reducer Hadoop/reducer.py -input /input/ReviewsCleaned.csv -output /output/job2output

# list the output directory
hdfs dfs -ls /output/job2output

# print on screen the job2output
hdfs dfs -head /output/job2output/part-00000

