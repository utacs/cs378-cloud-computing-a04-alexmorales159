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
## Task 1 (Errors in GPS):
```
1	198453
2	149629
3	113354
4	85924
5	63662
6	50864
7	104927
8	180293
9	219755
10	230093
11	223244
12	228677
13	242793
14	241225
15	251143
16	247963
17	215000
18	244767
19	304665
20	317971
21	296600
22	291543
23	279601
24	247429
```

# Task 2 (Taxis with the Highest GPS Error Rates):
```
0219EB9A4C74AAA118104359E5A5914C	1.0
0D752625D41FAFA8CED8F259651E624C	1.0
0EE3FFCBDFD8B2979E87F38369A28FD9	1.0
12CE65C3876AAB540925B368E8A0E181	1.0
FE757A29F1129533CD6D4A0EC6034106	1.0
```

# Task 3 (Most Efficient Drivers in Terms of Earnings per Minute):
```
FD2AE1C5F9F5FBE73A6D6D3D33270571	4095.0
A7C9E60EEE31E4ADC387392D37CD06B8	1260.0
D8E90D724DBD98495C1F41D125ED029A	630.0
E9DA1D289A7E321CC179C51C0C526A73	231.3
74071A673307CA7459BCF75FBD024E09	210.0
95A921A9908727D4DC03B5D25A4B0F62	210.0
42AB6BEE456B102C1CF8D9D8E71E845A	191.55
FA587EC2731AAB9F2952622E89088D4B	180.0
28EAF0C54680C6998F0F2196F2DA2E21	180.0
E79402C516CEF1A6BB6F526A142597D4	144.55
```

## Image of All Workers Running:
![Workers Running](img/all-workers.png)

## Image of All Finished Jobs:
![Finished Jobs](img/finished_jobs.png)

## GPS Error Hadoop Job 1:
![GPS Hadoop](img/gps-hadoop.png)

## Errors By Taxi Job 1:
![Taxi Errors 1](img/taxi-hadoop-1.png)

## Errors By Taxi Job 2:
![Taxi Errors 1](img/taxi-hadoop-2.png)

## Top Ten Drivers Job 1:
![Top Ten Drivers 1](img/driver-hadoop-1.png)

## Top Ten Drivers Job 2:
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
