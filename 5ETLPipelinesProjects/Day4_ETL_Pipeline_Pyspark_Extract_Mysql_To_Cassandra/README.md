# Prerequisites:
	1. Java
	2. Python
	3. Spark
	4. MySQL Server
	5. Cassandra Server
	6. Python Package Installer (pip install pyspark)
	7. Pycharm IDE (Coding & Development)

# Project Setup:
	1. Create a New Python Project and Set Virtual environment.
	2. Write python "main" script.
	3. Edit "Run Configuration" with "pyspark_home"
	4. Test the script.
	5. Verify result in "Cassandra"

# Problems :: Schema related errors with pyspark with cassandra:
> Caused by: java.util.NoSuchElementException: Columns not found in table etl_course.course2: _id

> pyspark.sql.utils.AnalysisException: Couldn't find table course in etl_course - Found similar tables in that keyspace: etl_course.course2

# Solutions :: Create first KEYSPACE and then TABLE with all the colum definition.
## Using commandline CLI:
> cmd:/> <cassandra_home>/bin/cqlsh.bat

> cqlsh>> CREATE KEYSPACE etl_course WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};

> cqlsh>> use etl_course;

> cqlsh>> CREATE TABLE etl_course.course2( \
 			course_id text PRIMARY KEY, \
     		course_name text, \
  			author_name text, \
  			no_of_reviews text \
 		) \ 

> cqlsh>> DROP TABLE etl_course.course2;

> cqlsh>> DROP KEYSPACE etl_course;

## Using Cassandra GUI client:
[![SC2 Video](https://img.youtube.com/vi/zCHe3V50kVs/0.jpg)](https://www.youtube.com/watch?v=zCHe3V50kVs)

> Apache Cassandra Introduction: \
[![SC2 Video](https://img.youtube.com/vi/AgT_hopun-c/0.jpg)](https://www.youtube.com/watch?v=AgT_hopun-c&list=RDCMUC9xghV-TcBwGvK-aEMhpt5w&index=41)

> Cassandra - System design | What is Cassandra: \
[![SC2 Video](https://img.youtube.com/vi/y9wgnS-5Qxg/0.jpg)](https://www.youtube.com/watch?v=y9wgnS-5Qxg)

> Cassandra Internals and Choosing a Distribution: \
[![SC2 Video](https://img.youtube.com/vi/uossfVwxWXk/0.jpg)](https://www.youtube.com/watch?v=uossfVwxWXk)

> Cassandra Write Architecture Practical \
[![SC2 Video](https://img.youtube.com/vi/1pOQFuIpawU/0.jpg)](https://www.youtube.com/watch?v=1pOQFuIpawU&list=RDCMUC9xghV-TcBwGvK-aEMhpt5w&index=20)

> Cassandra Architecture: How Read and Write are Done: \
[![SC2 Video](https://img.youtube.com/vi/JEwkI0W-wAk/0.jpg)](https://www.youtube.com/watch?v=JEwkI0W-wAk)

> Apache Cassandra Installation: \
[![SC2 Video](https://img.youtube.com/vi/Ty147JhU0hg/0.jpg)](https://www.youtube.com/watch?v=Ty147JhU0hg)

> Cassandra Multi Node Cluster Setup: \
[![SC2 Video](https://img.youtube.com/vi/MceviB8j1mY/0.jpg)](https://www.youtube.com/watch?v=MceviB8j1mY&list=PLLa_h7BriLH1hYHxg9rq8w5Fq7dhbyKZb&index=7)

> Cassandra Backup and Restore: \
[![SC2 Video](https://img.youtube.com/vi/Uw1hez8Ry7c/0.jpg)](https://www.youtube.com/watch?v=Uw1hez8Ry7c)