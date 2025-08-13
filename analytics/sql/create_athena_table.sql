-- Replace ${BUCKET} with your bucket
CREATE EXTERNAL TABLE IF NOT EXISTS iot_raw (
  device_id string,
  temperature double,
  humidity double,
  ts string
)
PARTITIONED BY (
  year string,
  month string,
  day string,
  hour string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://${BUCKET}/raw/'
TBLPROPERTIES ('has_encrypted_data'='false');

-- Discover partitions when you've ingested some data:
MSCK REPAIR TABLE iot_raw;
