# NON ESEGUIRE IL FILE, COPIARE E INCOLLARE SUL TERMINALE DOPO AVER CREATO IL CLUSTER

# copy the project folder
scp -i ~/.ssh/pem/sajobigdatakeys.pem -r /Users/alessandro/Development/BigData/BigData-PrimoProgetto2023/Job2/Hive hadoop@ec2-18-232-150-3.compute-1.amazonaws.com:~

# copy the dataset
scp -i ~/.ssh/pem/sajobigdatakeys.pem -r /Users/alessandro/Development/BigData/BigData-PrimoProgetto2023/decupled_ReviewsCleaned.csv hadoop@ec2-18-232-150-3.compute-1.amazonaws.com:~

# connect to the master node
ssh -i ~/.ssh/pem/sajobigdatakeys.pem hadoop@ec2-18-232-150-3.compute-1.amazonaws.com

# creare folders in the master node
hdfs dfs -mkdir /input
hdfs dfs -mkdir /output
hdfs dfs -put ReviewsCleaned.csv /input/ReviewsCleaned.csv

# launch hive
hive --f ~/Hive/job2cluster.hql

# list the output directory
hdfs dfs -ls /output/job2output10x

# print on screen the job2output
hdfs dfs -head /output/job2output10x/part-00000

