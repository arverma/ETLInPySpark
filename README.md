# Beginner Guide on writing PySpark ETL pipeline in production

```shell
|-- job_config.json # contain the sources and sink information
|-- main.py # driver program from where the execution starts 
|-- src # module contains the actual ETL code
|   |-- __init__.py
|   |-- transform.py # place to write your transformations(business logic)
|   `-- utils.py # place to write utility and helper function for reading, writing e.t.c 
`-- tests # module to write test cases
    |-- __init__.py
    |-- conftest.py
    |-- resources
    |   |-- expected
    |   |   `-- fact_joined_with_lookup.csv
    |   `-- input
    |       |-- citytier_pincode.csv
    |       `-- orders_data.csv
    `-- test_src
        |-- test_transform.py
        `-- test_utils.py

|-- Makefile # used to build egg file
|-- setup.py # used to build egg file
|-- README.md
```
### Prerequisites to run the job
1. Upload the files present under `tests/resources/input/` to any GCS bucket.
2. Update the `job_config.json` file with the source and sink location
Note: If you are testing  

### Package code into .egg file
```shell
$ cd ETLInPySpark
$ make build
```

### Submit Job: GCP
```shell
$ cd ETLInPySpark
$ gcloud dataproc jobs submit pyspark --cluster=<CLUSTER_NAME> --region=<REGION> \
--py-files ./dist/etlinpyspark-0.0.1-py3.9.egg \
--files job_config.json \
--properties=spark.submit.deployMode=cluster main.py \
-- --config_file_name job_config.json --env dev
```
### Submit Job: Local
```shell
$ cd ETLInPySpark
$ spark-submit main.py --config_file_name job_config.json --env dev
```

### Run test: Local
```shell
$ cd ETLInPySpark
$ pytest
```

