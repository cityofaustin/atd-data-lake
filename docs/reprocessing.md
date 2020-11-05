# Reprocessing Published Data

This describes procedures for reprocessing data that has already been published. An example of this need is when, during deployment of newer ETL code, older and newer processes were writing GRIDSMART aggregations to Socrata at the same time. While Socrata does have the ability to upsert on a key column (which holds hashes of the contents of the other columns), the older code hashed on a different naming of intersection files than the newer code! As a result, duplicate rows were written.

## Bad Days in Socrata

This speaks directly about wiping a date range in Socrata and rewriting records from a known-good source in the Data Lake. It is certainly possible for this to be automated with code, but for the time being is a manual process.

### Purging the Data in Socrata

As mentioned, each record in Socrata contains a unique key column which in the case of the ATD public data are MD5 hashes of the contents of respective records. (While these are *assumed* to be unique among potentially millions of records, there actually is no guarantee that a hash collision doesn't occur which would cause a row to be confused and overwritten).

Removing data in Socrata is not easy, as the Socrata web UI provides no means of doing it interactively. To delete rows, the unique identifiers-- the hashes-- need to be presented to Socrata through its API. The [DataSync](https://socrata.github.io/datasync/) utility is helpful for dealing with the API calls. However, DataSync needs the hashes.

To obtain the hashes, use the filtering functionality in Socrata to limit the records to the days that need purging, and download the contents as a CSV file. Then, in DataSync, load in the file and use only the column that contains the hashes. (It's easier to use Excel to prepare the CSV with only the hash column than it is to use the DataSync UI to ignore all of the non-hash columns.) Refer to credentials in `config/config_app.py` and `config/config_secret.py`, as well as the URL and the dataset ID (e.g. "sh59-i6y9")

### Updating the Catalog

The ATD Data Lake code keeps track of date ranges' worth of data that are published to Socrata. While it is possible to force a republishing without updating the catalog, it may be better to manipulate the catalog to allow it to reflect the records that have been removed. Check which items represent the older published data that was just purged in a client that's connected to the catalog database:

```sql
SET TIMEZONE='America/Chicago';
SELECT repository, data_source, id_base, id_ext, collection_date
FROM api.data_lake_cat_new
WHERE data_source = 'gs'
  AND repository = 'socrata'
  AND (collection_date, COALESCE(collection_end, collection_date + INTERVAL '1 day'))
    OVERLAPS (timestamp '2020-10-01', timestamp '2020-10-02')
ORDER BY collection_date, id_base, id_ext;
```

With that, a `DELETE FROM` query can remove the respective records.

### Rewriting the Records

Finally, the code that publishes the "ready" repository's data can be run. If the deploy process is set up to check for missing data over a time range that overlaps the data that had been removed, then the update will automatically happen the next time the process is scheduled to run. Otherwise, the process needs to be started manually.

To do this, log into the virtual machine with SSH and then run the process. This is an example that runs the GRIDSMART publisher over the first day of Oct. 2020.

```bash
docker run -d --rm -v ~/git:/app --network=host -w /app/atd-data-lake/atd_data_lake ctrdocker/tdp python -u gs_agg_extract_soc.py -s 2020-10-01 -e 2020-10-02
```

If the catalog were not updated, this process should do nothing. (Note that the -F parameter would force the update regardless of what the catalog says.)
