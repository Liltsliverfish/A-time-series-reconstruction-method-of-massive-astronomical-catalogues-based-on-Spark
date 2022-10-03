# A-time-series-reconstruction-method-of-massive-astronomical-catalogues-based-on-Spark
A method for time-series reconstruction of massive astronomical catalogues.

This is the main source code of the method, which can reconstruct all celestial objects' time series data for astronomical catalogues.

The program running on the Linux platform or Windows platform, which is implemented in scala. Its capabilities are based on specialized sky partitioning and Spark cluster.

### Code Architecture

The code contains two parts, ETL(extract-transform-load) preprocessing, mathcing calculation. You need to have original catalogues, and small data samples are provided in the **Data** directory. Then run program **ETL preprocessing** to generate format files. Next, run program **matching** to mark celestial objects. 

**CMP** directory is the comparative experiments for diferent Join algorithm between two tables.

### Prerequisites

This program running on the Spark cluster. The following environment configuration serves as a reference

- JDK version: 1.8.0_261
- Scala version: 2.12.12
- Redis version: 5.0.14.1
- Hadoop version: 3.1.2
- Spark version: 3.0.0

### Operating guide

##### 1. Setting up a Spark Cluster. Please refer to [https://spark.apache.org/](https://spark.apache.org/) for details.
##### 2. Package the source code as a JAR package (The JAR package is required for yarn-cluster submission).
##### 3. Submit the JAR package to the Spark cluster. You can select the yarn-client or yarn-cluster mode. The following is the code of yarn-client mode: 
```
bin/spark-shell --master yarn --deploy-mode client --executor-memory 8g --num-executors 32 --executor-cores 2
```




