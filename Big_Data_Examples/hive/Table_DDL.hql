---Create table employee-delimited by |---

CREATE EXTERNAL TABLE employee (
empid Long,
firstname string,
lastname string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/employee.db/employee';

---Create table department-delimited by |---

CREATE EXTERNAL TABLE department (
deptid Long,
deptname string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/department.db/department';

---Create table customer stored as avro file format---

CREATE EXTERNAL TABLE customers
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION 'hdfs:///user/hive/warehouse/retail_stage.db/customers'
TBLPROPERTIES ('avro.schema.url'='hdfs://quickstart.cloudera/user/cloudera/retail_stage/sqoop_import_customers.avsc');
