# NON ESEGUIRE, COPIARE E INCOLLARE SUL TERMINALE DOPO AVER CREATO IL CLUSTER

# copy the project folder
scp -i ~/.ssh/pem/sajobigdatakeys.pem -r /Users/alessandro/Development/BigData/BigData-PrimoProgetto2023/Job2/Spark hadoop@ec2-3-87-95-72.compute-1.amazonaws.com:~

# copy the dataset
scp -i ~/.ssh/pem/sajobigdatakeys.pem -r /Users/alessandro/Development/BigData/BigData-PrimoProgetto2023/decupled_ReviewsCleaned.csv hadoop@ec2-3-87-95-72.compute-1.amazonaws.com:~

# connect to the master node
ssh -i ~/.ssh/pem/sajobigdatakeys.pem hadoop@ec2-3-87-95-72.compute-1.amazonaws.com

# creare folders in the master node
hdfs dfs -mkdir /input
hdfs dfs -mkdir /output
hdfs dfs -put ReviewsCleaned.csv /input/ReviewsCleaned.csv

# launch spark
time spark-submit --master local ~/Spark/job2spark.py --input_path file:///home/hadoop/decupled_ReviewsCleaned.csv --output_path ~/output

# list the output directory
hdfs dfs -ls ~/output

# print on screen the job2output
hdfs dfs -head ~/output/part-00000

# remove the output directory
hdfs dfs -rmr ~/output 
