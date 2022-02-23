# BIG DATA MANAGEMENT - Assignment 2
This is an assignment for the course of Big Data Management (T-764-DATA) taken in Reykjavik University during the Spring semester 2022.  
This project is developed by: [Alessandro Talmi](https://github.com/Tale152) and [Elisa Tronetti](https://github.com/ElisaTronetti).

## Main Goal

The primary purpose of this assignment is to implement K-Means clustering on Spark.  
The goals can be classified in:
- Setting up an environment where it is possible to run and test the code using Spark
- Developing and running a "complex" pipeline on Spark (in Scala/Java)
- Implementing optimizations and evaluating against the original code as baseline reference point
- Writing a technical report about the work and findings.

## Techical Setup

The setup chosen is the following:
- java version > **1.8**
- scala version **2.12.10**
- sbt version **TODO**
- spark version **3.0.3**
- hadoop version **2.7.4**


## How To Run

First compile the code and generate the jar via sbt using:  
```
sbt compile
sbt package
```

You must have a spark master and at least one worker running on your machine. Follow [Spark documentation](https://spark.apache.org/docs/3.0.3/spark-standalone.html#launching-spark-applications) to correctly run them.

Then you can run the program:
```
sbt "run -m=<SPARK MASTER IP> -d=<FILE PATH> -v=<KMEANS VERSION> -c=<CENTROIDS NUMBER> -sl"
```

Mandatory arguments:  
- **-m** is the spark master ip address, which is currently running. 
- **-d** is the file path that is going to be used to perform K-Means clustering: it must be a .seq file.

Optional arguments:  
- **-v** if ommitted it is going to be execute the default K-Means version implemented. The versions currently available are: DEFAULT, CENTROID_STABILITY.
- **-c** accepts a number to specify the number of centroids that k-means is going to use. If not specified, it is automatically set as 100 centroids.
- **-sl** is the option that enables spark logging: just ommit this to disable it and to read only the program output.

## K-Means Version Implemented

### Default
The default version of K-Means select from the beginning of the Array as many centroids as specified.  
Then it is going to be calculated for each point of the Array the Euclidean distance of that point from all the centroids.  
For each point is going to be returned a tuple which contains the id of the centroid which is the nearest to the point, a counter that is used to find the total number of points in the specific cluster and the actual point.  
Once that is done, for each centroid is going to be calculated the total number of point in that cluster and the average of all the points. The average is the new cluster centroid that is going to be used for the next iteration of the argorithm.  

The main distinctive traits is the __end coindition__: this version of K-Means just continues until it has finished to compute all the iterations, even in the case that a stable version has been already found.

### Centroid Stability
Thi K-Means version is very similar to the Default one, with only one main difference that helps to increase the computational efficiency.  

The __end conditions__ in this version are:
- if the maximum number of iterations is reached
- if the centroids values are stable respection a specified tolerance. Again it is used the Euclidean distance to calculate the distance between the previous iteration centroids and the currents: if the distance is less then the tolerance, then the algorithm can stop and it will return the results.

## Code structure