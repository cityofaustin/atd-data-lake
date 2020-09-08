# Appendix: Catalog

*[(Back to Docs Catalog)](index.md)*

This document describes how the Catalog database is created that allows for indexing of items that are stored within the Data Lake.

## PostgreSQL Server Connection
These are instructions for accessing the test server so that tables can be created for PostgreSQL accessible by PostgREST.

### How to Connect and Modify PostgreSQL + PostgREST
These instructions assume that PostgREST has been configured and operating by ATD. The PostgREST server must be on the same subnet as the PostgreSQL database.

1. SSH to PostgREST server.
1. To see current running Docker containers: `sudo docker ps -a`. Note that PostgREST should be listed as the only running container.
1. Connect to the postgresql backend with Docker/psql:
   ```bash
    sudo docker run \
    --rm \
    -it \
    --name psql \
    -p 5432:5432 \
    postgres bash -c "PGPASSWORD=*** psql -h ***.rds.amazonaws.com -p 5432 -U *** -d atd01"
    ```
1. You're connected. Make changes as nessary and use `\q` to quit psql.
1. You'll need to restart the PostgREST server in order to access your modifications through the API:
   ```bash
   sudo docker restart ec2-user_server_1
   ```
1. Hit the API to make sure it's running again:
   ```bash
   curl http://transportation-data-test.austintexas.io/
   ```

When creating tables, pay attention to extra schema and privileges needed for PostgREST to work with the new table:

You should build your new tables under the `api` schema. PostrgREST is configured to only serve tables from that schema.

Once you have your table(s) in the `api` schema, you need to grant DB privileges to the anonymous user, `web_anon`. That statement is:

```sql
GRANT SELECT ON api.<your_table_name> TO web_anon;
```

Note that if you were to grant INSERT and UPDATE to `web_anon`, anyone on the Internet would be able to modify records in that schema via the PostgREST endpoint. We have an authenticated user for that purpose, `super_user`. Use this statement:

```sql
GRANT SELECT, UPDATE, INSERT, DELETE ON api.<your_table_name> TO super_user
```

Authentication happens with JSON web tokens (JWT). A new JWT must be issued for `super_user` so that data can be written.

Also see https://github.com/cityofaustin/transportation-data/blob/ac45d62f704628dde97c2f2035f37ce6f32545c0/postgresql/schema.api/schema.sql#L25

#### Sequences

There is a special consideration for permissions around sequences that must happen for PostgREST to update respective tables. For example, the `api.data_lake_catalog` table below uses a `serial` type to maintain a quick integer-based unique identfier. For that to work with PostgREST, the permissions must be assigned like this (you can see the sequence name by running in `psql`: `\d api.<your_table_name>`)

```sql
GRANT USAGE, SELECT ON SEQUENCE api.<your_sequence_name> TO super_user;
```

### JSON Web Token

This allows write/update access to PostgREST databases that are granted permissions for the `super_user` user. It will be advantageous for there to be a procedure to generate tokens.

## Catalog Notes

This section now pertains to creating and accessing the Catalog.

### Create Catalog

This is what will be done to create the catalog table that is accessible through PostgREST:

```sql
CREATE TABLE IF NOT EXISTS api.data_lake_catalog (
  id SERIAL,
  repository TEXT NOT NULL,
  data_source TEXT NOT NULL,
  id_base TEXT NOT NULL,
  id_ext TEXT NOT NULL,
  pointer TEXT NOT NULL,
  collection_date TIMESTAMP WITH TIME ZONE NOT NULL,
  collection_end TIMESTAMP WITH TIME ZONE,
  processing_date TIMESTAMP WITH TIME ZONE,
  metadata JSONB,
  PRIMARY KEY (collection_date, repository, data_source, id_base, id_ext)
);

COMMENT ON TABLE api.data_lake_catalog IS
  'Austin Transportation Data Lake catalog that identifies archived files';

CREATE INDEX data_lake_catalog_date_idx ON api.data_lake_catalog
  (collection_date);

GRANT ALL ON api.data_lake_catalog TO super_user;
GRANT USAGE, SELECT ON SEQUENCE api.data_lake_catalog_id_seq TO super_user;
```

The columns are described in the [technical documentation](https://github.com/cityofaustin/atd-data-lake/blob/master/docs/tech_architecture.md#data-lake-catalog).

### Access PostgREST
The catalog is to be accessed through PostgREST. The API at https://github.com/cityofaustin/pypgrest can be helpful, and it has been run under Python 3.6. See the examples in the Readme file.

The API requires the JSON Web Token for write access.

I created a test table, too, that can be practiced on, prefilled with values "test1", "test2" and "test3":

```sql
CREATE TABLE api.test_table (
  id SERIAL PRIMARY KEY,
  value TEXT
);
GRANT ALL ON api.test_table TO super_user;
GRANT USAGE, SELECT ON SEQUENCE api.test_table_id_seq TO super_user;
GRANT SELECT ON api.test_table TO web_anon;
```

Without the JSON Web Token, all we can do is to select on tables that are given the `web_anon` privilege. (`test_table` has this, but `data_lake_catalog` does not, because we don't want to make the catalog public). Notice the table name is at the end of the URL, and the select parameters are in the `params`. The API key parameter (as seen in the readme) is also omitted because in this example we're just selecting:

```python
from pypgrest import Postgrest
pgrest = Postgrest("http://transportation-data-test.austintexas.io/test_table")
params = {
    "select": "value",
    "limit": 100
}
pgrest.select(params=params)
```

Here, I'll add an element to the table, query on it, delete it, and try querying on it again.

```python
from pypgrest import Postgrest
pgrest = Postgrest("http://transportation-data-test.austintexas.io/test_table", auth="***")
myStr = "Final test!"
pgrest.insert({"value": myStr})
if pgrest.select({"select": "value", "value": "eq.%s" % myStr}):
    print("Present 1")
pgrest.delete({"value": "eq.%s" % myStr})
if pgrest.select({"select": "value", "value": "eq.%s" % myStr}):
    print("Present 2")
```


> from pypgrest import Postgrest
>>> catalog=Postgrest("https://transportation-data.austinmobility.io/data-lake-catalog", auth="****")
>>> rec={"repository":"test","data_source":"test","id_base":"test","id_ext":"test","pointer":"test","collection_date":"2020-01-01"}

Further instructions on parameters: https://postgrest.org/en/v4.1/api.html. It looks like "AND" operators in queries are easy-- just add key/value pairs to select calls-- but, "OR" operators require the use of stored procedures. To insert a stored procedure, it appears that one would need to get into the database with psql as seen in a previous section.

Further example on using PostgREST and date functions: https://github.com/cityofaustin/transportation-data-utils/blob/master/tdutils/jobutil.py; see the use of the "arrow" package, and anything that calls `Job._query()`. (This talks directly to PostgREST without the use of pypgrest, but the parameters and data types are the same as what's used by pypgrest; pypgrest just hides HTTP requests a bit).

The approach used for programming is to use these calls first to do a proof-of-concept with the catalog, and then later put an API wrapper around it that will hide the PostgREST details and possibly bundle in the S3 accesses.

### Reading and Writing the Catalog
Example of writing to the catalog (updating the entry if this is run repeatedly, as the upsert keys off of `(collection_date, repository, data_source, id_base, id_ext)`. That is, if these are the same as what's already in the table, then the other fields get updated, or if no row exists with the key, then a new row is written):

```python
from pypgrest import Postgrest
import arrow
import json
catalog = Postgrest("http://transportation-data-test.austintexas.io/data_lake_catalog", auth="***")

# Insert or update new element to catalog:
repository = "raw"
data_source = "bt"
id_base = "Austin_bt"
id_ext = "txt"
pointer = "2018/07/06/bt/Austin_bt_07-06-2018.txt"
collection_date = "2018-07-06T00:00:00-06:00"
processing_date = arrow.now().format()
metadata = json.dumps({"element": "True"})

catalog.upsert({"repository": repository, "data_source": data_source,
"id_base": id_base, "id_ext": id_ext, "pointer": pointer,
"collection_date": collection_date, "processing_date": processing_date, "metadata": metadata})
```

Example of reading everything from the catalog:

```python
# Get everything from catalog:
recs = catalog.select({"select": "*"})
```

Example of reading from the catalog, in this example returning only the newest collected entities for Bluetooth from the "raw" bucket:

```python
# Get newest date from catalog for raw/bt:
repository = "raw"
data_source = "bt"
recs = catalog.select({"select": "id_base,id_ext,collection_date",
  "repository": "eq.%s" % repository,
  "data_source": "eq.%s" % data_source,
  "order": "collection_date.desc",
  "limit": 1})

# Get all items in raw/bt for that newest date:
recsDate = catalog.select({"select": "repository,data_source,id_base,id_ext,pointer,collection_date,processing_date,metadata",
  "repository": "eq.%s" % repository,
  "data_source": "eq.%s" % data_source,
  "collection_date": "eq.%s" % recs[0]["collection_date"]})
```
