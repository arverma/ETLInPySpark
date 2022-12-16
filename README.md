# How you can write your ETL pipeline using PySpark in Prod

### Package code into egg file
```shell
$ cd ETLInPySpark
$ make build
```

### Submit Job
```shell
gcloud dataproc jobs submit pyspark --cluster=<CLUSTER_NAME> --region=<REGION> \
--py-files ./dist/etlinpyspark-0.0.1-py3.9.egg \
--files job_config.json \
--properties=spark.submit.deployMode=cluster main.py \
-- --config_file_name job_config.json --env dev
```

### Run test
```shell
$ cd ETLInPySpark
$ pytest
```

