CREATE EXTERNAL TABLE IF NOT EXISTS `stedi-datalake-digx`.`customer_landing` (
  `customername` int,
  `email` string,
  `phone` string,
  `birthday` string,
  `serialnumber` string,
  `registrationdate` bigint,
  `lastupdatedate` bigint,
  `sharewithresearchasofdate` bigint,
  `sharewithfriendsasofdate` bigint,
  `sharewithpublicasofdate` bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://stedi-datalake-digx/customer/landing/'
TBLPROPERTIES ('classification' = 'json');