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
- sbt version **1.5.5**
- spark version **3.0.3**
- hadoop version **2.7.4**


## How To Run

First compile the code and generate the jar via sbt using:  
```
sbt compile
sbt package
```

You must have a spark master and at least one worker running on your machine. Follow [Spark documentation](https://spark.apache.org/docs/3.0.3/spark-standalone.html#launching-spark-applications) to correctly run them.

Then you can run the program (arguments can be placed in any order):
```
sbt "run -m=<SPARK MASTER IP> -d=<FILE PATH> -cs=<CENTROIDS SELECTOR> -cn=<CENTROIDS NUMBER> -mr=<MAP-REDUCE VERSION> -ec=<END CONDITION> -sl"
```

Mandatory arguments:  
- **-m** is the spark master ip address, which is currently running. 
- **-d** is the file path that is going to be used to perform K-Means clustering: it must be a .seq file.

Optional arguments:  
- **-cs** determines how the centroids will be chosen. If omitted "first_n" is used.
- **-cn** accepts a number to specify the number of centroids that k-means is going to use. If not specified, it is automatically set as 100 centroids.
- **-mr** sets how the map-reduce operation will be performed. When omitted "default" will be chosen.
- **-ec** determines the end condition that terminates the computation ("max" if omitted).
- **-sl** is the option that enables spark logging: just omit this to disable it and to read only the program output.

## Versions implemented
### Centroids selection (-cs)
- **first_n**: selects from the beginning of the data Array as many centroids as specified by -cn.
- **evenly_spaced**: selects a specified number of centroids evenly spaced in the data vector.

### Map-reduce (-mr)
- **default**:<br />
For each point of the Array the Euclidean distance of that point from all the centroids is calculated.  
Then, for each point is going to be returned a tuple which contains the id of the centroid which is the nearest to the point, a counter that is used to find the total number of points in the specific cluster and the actual point.  
Once that is done, for each centroid is going to be calculated the total number of point in that cluster and the average of all the points. The average is the new cluster centroid that is going to be used for the next iteration of the algorithm.
- **early_halting**:<br />
This implementation does the exact same thing as the default one but it tries to enhance performances using the early halting technique when calculating the distance between every point and every centroid.
- **early_halting_unraveling**:<br />
In combination with the early halting, it also reduce the number of loop during the computation of the algorithm, decreasing the computational costs.
### End condition (-ec)
- **max**:<br />
This version of just continues to compute K-Means until all the iterations are executed, even in the case that a stable version has been already found.
- **similarity**:<br />
If the centroids values are stable respection a specified tolerance. Euclidean distance is used to calculate the distance between the previous iteration centroids and the currents: if the distance is less than the tolerance, then the algorithm can stop and it will return the results. In the unfortunate circumstance that stability is never reached, computation will stop when max iterations are reached.

## Code structure
The core of the code is in the package __kMeans__, which contains the actual computational logic of the K-Means clustering algorithm.  
When running the program, as seen before, there are different options that can be set, which define how the data is actually going to be handled. These can be find in the __versions__ package: the objects present are **InitCentroidSelectors**, **MapReduces** and **EndConditions**, and in each of them there is the implementation of different versions to get the result.  
The **KMeans** object defines the logic structure of the algorithm; in order to create the object easily there is the **KMeansBuilder**, in which are set also the algorithm options taken as input.  

The other package is __utilis__, in which there are utility objects used for example to handle arguments, to upload the data file or to create the SparkContext correctly.
