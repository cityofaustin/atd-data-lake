# Appendix: Performance Metrics



Performance metrics in the ETL processes looks like:
* Logging the numbers of records manipulated for each day in each ETL process
* During the JSON Canonicalization stage, logging a representative metric for each unit handled within the data source

The main purposes of logging these is to understand the overall health of the system, so that visualizations can be shown in a dashboard format. If entire ETL stages fail, then they should be obviously absent from the dashboard. If measurements for units are missing (e.g. intersections are out for GRIDSMART) or are significantly off from the average, then it should also be evident on the dashboard.

This document describes the metrics that are created, the efforts in Knack to visualize them (which can conceivably be duplicated using other visualization platforms), and has technical notes on creating the database tables that store the daily metrics.



## Database Creation



```sql
CREATE TABLE api.etl_perfmet_job (
  id serial,
  data_source varchar NOT NULL,
  stage varchar NOT NULL,
  seconds real,
  records integer,
  processing_date timestamp with time zone NOT NULL,
  collection_start timestamp with time zone,
  collection_end timestamp with time zone,
  PRIMARY KEY (processing_date, data_source, stage)
);

COMMENT ON TABLE api.etl_perfmet_job IS
  'Austin Transportation performance metrics job log';

GRANT ALL ON api.etl_perfmet_job TO super_user;
GRANT USAGE, SELECT ON SEQUENCE api.etl_perfmet_job_seq TO super_user;
```

> Describe the columns.



```sql
CREATE TABLE api.etl.perfmet_obs (
  id serial,
  data_source varchar NOT NULL,
  sensor_name varchar NOT NULL,
  data_type varchar NOT NULL,
  data real,
  expected real,
  collection_date timestamp with time zone NOT NULL,
  timestamp_min timestamp with time zone,
  timestamp_max timestamp with time zone,
  PRIMARY KEY (collection_date, data_source, data_type, sensor_name)
);

COMMENT ON TABLE api.etl_perfmet_obs IS
  'Austin Transportation performance metrics observation log';

GRANT ALL ON api.etl_perfmet_obs TO super_user;
GRANT USAGE, SELECT ON SEQUENCE api.etl_perfmet_obs_seq TO super_user;
```

> Describe the columns.

