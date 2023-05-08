# PING

## Compile PING
First you need to install maven to build PING. Then in the root folder where pom.xml exists, run:
```
mvn package
```
A target folder should be created if everything builds correctly with the jar inside.

## Run PING
To run PING you first need to install java (version 16+), Apache Spark (version 2.2+) and Hadoop (version 2.7+). Also hdfs command (inside of bin/ in hadoop) should be set as an environment variable(be visible as hdfs inside PING).

To run each of the components of PING (partitioner, query translator, query executor) the appropriate script should be used.
Inside of the folder #[scripts](https://github.com/giannisvassiliou/PING_ISWC_2023/tree/main/scripts)# the appropriate scripts can be found. For the hierarchical levels and indexes (run_summary)  For partitioner (run_partitioner.sh), for query translator (run_translator.sh), for progressive query answering (run_pqa.sh) and for exact query answering (run_queries.sh).

Each script should be modified accordingly with the steps bellow. In every script modify all fields of spark submit (master, memory,path_to_jar etc.)



### Script Argument

* **dataset_path:** path of dataset (local in disk)

* **summary_output_folder:** output folder that conatains the hierarchical levels (.nt) and the indexes (property_level, subject_indexes, object_index)  

* **subject_indexes_folder:** input folder of subject_indexes files (instance_level_x files)

* **dataset_name:** a specific name for each dataset (same name must be used in all procedures for a specific dataset)

* **hdfs_path:** hdfs base folder path

* **dataset_hdfs_path:** hdfs path of dataset (.nt)

* **levels_hdfs_path:** hdfs path of levels (.nt)

* **sparql_input_folder:** input folder of sparql queries

* **translated_queries_folder:** the folder with the result SQL queries translated from the input sparql queries

 
### Firstly, run the summary.jar to create the hierarchical levels and the corresponding indexes
```
./run_psummary.sh  dataset_path summary_output_folder
``` 
### To partition data using PING use the script run_partitioner like this:
```
./run_partitioner.sh  dataset_name hdfs_path dataset_hdfs_path levels_hdfs_path
```
### To translate sparql queries use the script run_translator like this:
```
./run_translator.sh  dataset_name hdfs_path sparql_input_folder property_level subject_indexes_folder object_index
```
### To have progressive query answering of the translated queries use the script run_pqa like this:
``` 
./run_pqa.sh  dataset_name hdfs_path translated_queries_folder
```
### To execute the translated queries use the script run_query like this:
``` 
./run_queries.sh  dataset_name hdfs_path translated_queries_folder
```
