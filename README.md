# Please add your team members' names here. 

## Team members' names 

1. Student Name: Alex Morales

   Student UT EID: am95779

2. Student Name: Jerry Ming

   Student UT EID: jm98623

3. Student Name: Arul Yamdagni

   Student UT EID: ay7494

 

##  Course Name: CS378 - Cloud Computing 

##  Unique Number: 51515
    


# Project Report
Image of All Workers Running:
![Workers Running](img/all-workers.png)

Image of All Finished Jobs:
![Finished Jobs](img/finished_jobs.png)

GPS Error Hadoop Job 1:
![GPS Hadoop](img/gps-hadoop.png)

Errors By Taxi Job 1:
![Taxi Errors 1](img/taxi-hadoop-1.png)

Errors By Taxi Job 2:
![Taxi Errors 1](img/taxi-hadoop-2.png)

Top Ten Drivers Job 1:
![Top Ten Drivers 1](img/driver-hadoop-1.png)

Top Ten Drivers Job 2:
![Top Ten Drivers 2](img/driver-hadoop-2.png)



# Project Template

# Running on Laptop     ####

Prerequisite:

- Maven 3

- JDK 1.6 or higher

- (If working with eclipse) Eclipse with m2eclipse plugin installed


The java main class is:

edu.cs.utexas.HadoopEx.WordCount 

Input file:  Book-Tiny.txt  

Specify your own Output directory like 

# Running:




## Create a JAR Using Maven 

To compile the project and create a single jar file with all dependencies: 
	
```	mvn clean package ```



## Run your application
Inside your shell with Hadoop

Running as Java Application:

```java -jar target/MapReduce-GPSErrors-jar-with-dependencies.jar taxi-data-sorted-small.csv.bz2 output1```

```java -jar target/MapReduce-TaxiErrors-jar-with-dependencies.jar taxi-data-sorted-small.csv.bz2 inter2 output2```

```java -jar target/MapReduce-TopDrivers-jar-with-dependencies.jar taxi-data-sorted-small.csv.bz2 inter3 output3```

Or through Maven

```mvn exec:java@gps -Dexec.args="taxi-data-sorted-small.csv.bz2 output1"```

```mvn exec:java@taxi -Dexec.args="taxi-data-sorted-small.csv.bz2 inter2 output2"```

```mvn exec:java@driver -Dexec.args="taxi-data-sorted-small.csv.bz2 inter3 output3"```

Or has hadoop application

```hadoop jar your-hadoop-application.jar edu.cs.utexas.HadoopEx.WordCount arg0 arg1 ... ```



## Create a single JAR File from eclipse



Create a single gar file with eclipse 

*  File export -> export  -> export as binary ->  "Extract generated libraries into generated JAR"
