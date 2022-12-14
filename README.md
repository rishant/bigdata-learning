# Spark ETL Project Setup 
    Required Pieces of software: 
        1. Java 
        2. python 
        3. spark
        4. hadoop_winutils 
        5. pycharm

### Step:1 - Download & Install Java 8|11|17 + Windows Environment "JAVA_HOME"
![img_11.png](readme_resources/img_11.png)
### Step:2 - Download & Install Python 3.8 or later + Windows Environment "PYTHONPATH"
![img_10.png](readme_resources/img_10.png)

### Step:3 - Download and Setup Spark in Windows
#### Step:3.1 - Setup Spark 
![img.png](readme_resources/img.png)
![img_12.png](readme_resources/img_12.png)
#### Step:3.2 - Setup Hadoop Windows Utilities
![img_1.png](readme_resources/img_1.png)

### Step:4 - Download intellij pyCharm IDE Community Edition
#### Step:4.1 - Create Python Project for "pyspark"
![img_2.png](readme_resources/img_2.png)
![img_3.png](readme_resources/img_3.png)

#### Step:4.2 - Setup/Install "pyspark" for python Project
![img_4.png](readme_resources/img_4.png)
![img_5.png](readme_resources/img_5.png)
![img_6.png](readme_resources/img_6.png)

#### Step:4.3 - Edit runtime configuration for RUN "python + pyspark"
![img_7.png](readme_resources/img_7.png)
![img_8.png](readme_resources/img_8.png)
![img_9.png](readme_resources/img_9.png)

### Step:5 - Testing, Run & verify Script "data_pipline.py"
Run pyspark python code using spark-submit job:
> - cmd:/> spark-submit --packages org.mongodb.spark:mongo-spark-connector:10.0.3 .\1PythonPracitices\pyspark-practices\mongo_db_testing_main.py
> - cmd:/> spark-submit .\1PythonPracitices\pyspark-practices\main.py

# Python Virtual Environment Creation and Restore using requirements.txt file
- https://www.akamai.com/blog/developers/how-building-virtual-python-environment

    > **Steps to setup git clone projects in intellij** \
        cmd:/> git clone https://github.com/rishant/bigdata-learning.git \
        ------ \
    **Step: 1 - Goto the project folder** \
        cmd:/> cd <project_floder>bigdata-learning\4PythonSpark \
        ----- \
    **Step: 2 - Use below command to create Virtual Environment** \
        cmd:/> pip install venv \
        cmd:/> .\venv\Scripts\activate \
        (venv)  cmd:/> pip install -r requirements.txt \
        ----- \
    **Step: 3 - Run pyspark python code using CLI spark-submit job:** \
        (venv)  cmd:/> spark-submit --packages org.mongodb.spark:mongo-spark-connector:10.0.3 mongo_db_testing_main.py \
        (venv)  cmd:/> spark-submit main.py \
        ----- \
    **Step: 4 - Use below command to freeze Virtual Environment and CREATE requirements.txt** \
        (venv)  cmd:/> pip freeze > requirements.txt

# Python Learning References:
	- https://pythonexamples.org/python-enum/
	- https://www.educba.com/software-development/software-development-tutorials/python-tutorial/
	
[![SC2 Video](https://img.youtube.com/vi/sCOw5y1RQpY/0.jpg)](https://www.youtube.com/watch?v=sCOw5y1RQpY&t=48857s)

# Installation & Setup Video References:
[![SC2 Video](https://img.youtube.com/vi/3LTSSzBZvXE/1.jpg)](https://www.youtube.com/watch?v=3LTSSzBZvXE&t=2s)
[![SC2 Video](https://img.youtube.com/vi/ZVW3AJwGy8E/1.jpg)](https://www.youtube.com/watch?v=ZVW3AJwGy8E)
[![SC2 Video](https://img.youtube.com/vi/cxdDL_EsByQ/1.jpg)](https://www.youtube.com/watch?v=cxdDL_EsByQ)
[![SC2 Video](https://img.youtube.com/vi/CrLXa9hEprE/1.jpg)](https://www.youtube.com/watch?v=CrLXa9hEprE)
[![SC2 Video](https://img.youtube.com/vi/9-v9WporLrI/1.jpg)](https://www.youtube.com/watch?v=9-v9WporLrI)

# Spark Learning:
[![SC2 Video](https://img.youtube.com/vi/_C8kWso4ne4/1.jpg)](https://www.youtube.com/watch?v=_C8kWso4ne4&t=3979s)
[![SC2 Video](https://img.youtube.com/vi/cZS5xYYIPzk/1.jpg)](https://www.youtube.com/watch?v=cZS5xYYIPzk&t=543s)
[![SC2 Video](https://img.youtube.com/vi/GygBAobkC_4/1.jpg)](https://www.youtube.com/watch?v=GygBAobkC_4)
[![SC2 Video](https://img.youtube.com/vi/WyZmM6K7ubc/1.jpg)](https://www.youtube.com/playlist?list=PLZoTAELRMXVNjiiawhzZ0afHcPvC8jpcg)

# Other Video References:
[![SC2 Video](https://img.youtube.com/vi/zUiTu8HJ3Tg/1.jpg)](https://www.youtube.com/watch?v=zUiTu8HJ3Tg)
[![SC2 Video](https://img.youtube.com/vi/t90FMQr9WWc/1.jpg)](https://www.youtube.com/watch?v=t90FMQr9WWc)
[![SC2 Video](https://img.youtube.com/vi/5c-d5YZ3cc8/1.jpg)](https://www.youtube.com/watch?v=5c-d5YZ3cc8&list=PLZoTAELRMXVN_8zzsevm1bm6G-plsiO1I&index=4)
[![SC2 Video](https://img.youtube.com/vi/_hf_y-_sj5Y/1.jpg)](https://www.youtube.com/watch?v=_hf_y-_sj5Y&list=PLZoTAELRMXVN7QGpcuN-Vg35Hgjp3htvi&index=1)
[![SC2 Video](https://img.youtube.com/vi/tSRCACUmnII/1.jpg)](https://www.youtube.com/watch?v=tSRCACUmnII)
https://www.youtube.com/c/UnfoldDataScience/playlists

# Other References:
- https://sparkbyexamples.com/spark/add-multiple-jars-to-spark-submit-classpath/
