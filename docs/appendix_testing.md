# Appendix: Testing

*[(Back to Docs Catalog)](index.md)*

This document captures notes on testing the first rendition of codes that write to and read from the Data Catalog.

## On Automated Testing
One of the items that still needs to be completed are efforts to automate testing. Through automation, many of the tests documented here won't need to be run manually.

Already, progress has been made toward assisting in testing. These are largely supported by features that can be enabled with command-line parameters. These are the parameters of interest:
* **-o** or **--output_filepath:** In addition to writing output files to the cloud or publishing service that the ETL code is designed to write to, this option will cause one or more files to also be written to the given directory, named in the same way that the file would be named on the cloud service. If the ETL process writes to a publishing service, then the output file may be a CSV file.
* **-0** or **--simulate:** This performs all of the ETL process, but doesn't write to the cloud or publishing service, nor is the Catalog updated. If `-o` is specified, then one or more local files are still written out. This can allow code to be run without committing anything to cloud services or the Catalog.
* **--debug:** Using configuration code set up in the `config.config_app` package, this causes target repositories to be changed to debug names. Currently, this is the repository name with "-test" appended to the end. Code could also be set up to write to debug publishers, or to use an alternate PostgREST endpoint for the Catalog and performance metrics.

### Manual ETL Running
Refer to the "Manually Testing an ETL Process" section of the [Platform Setup](platform_setup.md) document for information on manually starting an interactive Docker container for testing ETL processes.

## Testing the Data Lake Codes
This section describes brief tests for the various codes that comprise the Data Lake transformations.

Note that many of these tests appear repetetive. Because of the streamlining efforts, the same code is largely executed regardless of which ETL script is started.

> **TODO:** We could limit the extensive missing-entry testing to one data source because the same code is being run regardless of data source.

### Bluetooth Ingest (raw)
I'm using my Windows-based laptop to test this.

I have these test files:

```
C:\Dev\coa_dev>dir \dev\test
 Volume in drive C is OSDisk
 Volume Serial Number is CEE0-0FCE

 Directory of C:\dev\test

02/07/2019  06:10 PM    <DIR>          .
02/07/2019  06:10 PM    <DIR>          ..
07/07/2018  11:59 PM         6,741,490 Austin_btmatch_07-07-2018.txt
07/08/2018  11:59 PM         5,679,321 Austin_btmatch_07-08-2018.txt
07/09/2018  11:59 PM         9,024,513 Austin_btmatch_07-09-2018.txt
07/07/2018  11:59 PM        35,357,428 Austin_bt_07-07-2018.txt
07/08/2018  11:59 PM        30,961,094 Austin_bt_07-08-2018.txt
07/09/2018  11:59 PM        42,287,430 Austin_bt_07-09-2018.txt
07/10/2018  11:59 PM        42,700,606 Austin_bt_07-10-2018.txt
```

Get to the correct directory:

```dos
cd c:\dev\atd-data-lake\src
```

Stage file distinction test:

```dos
move c:\dev\test\Austin_bt_07-07-2018.txt c:\dev
```
> **TODO:** Note that this can be updated to use a test file that travels along with the repository.

This is probably a good time to clear out the catalog.

Attempt to update from the temporary directory; updates should occur.

```dos
python bt_insert_lake.py -d c:\dev\test -s 2018-07-01 -e 2018-07-12
```
> **TODO:** Consider using --debug and setting up the test PostgREST endpoint.

Check the catalog. On a psql console into the catalog:

```sql
SELECT * FROM api.data_lake_catalog WHERE data_source = 'bt' AND collection_date BETWEEN '2018-07-01' AND '2018-07-12';
```

Run again to verify that no updates occur:

```dos
python bt_insert_lake.py -d c:\dev\test -s 2018-07-01 -e 2018-07-12
python bt_insert_lake.py -d c:\dev\test -r 2018-07-01 -e 2018-07-12
```

Check if force works; updates should occur:

```dos
python bt_insert_lake.py -d c:\dev\test -s 2018-07-01 -e 2018-07-12 -F
```

Go to Section ① of "Bluetooth Canonicalization (raw → rawjson)" below to test the canonicalization transformation.

To test a present missing file, move the missing file back into the directory and run again. (The first command shouldn't pick up the file because it is out of the date range).

```dos
move c:\dev\Austin_bt_07-07-2018.txt c:\dev\test
python bt_insert_lake.py -d c:\dev\test -r 2019-01-01
python bt_insert_lake.py -d c:\dev\test -s 2018-07-01 -e 2018-07-12
```

Verify that the catalog updated with this new file. On the database console:

```sql
SELECT * FROM api.data_lake_catalog WHERE data_source = 'bt' AND collection_date BETWEEN '2018-07-07' AND '2018-07-08';
```

Make sure days-old logic runs. (Because of dates of files in that test directory, nothing should update):

```dos
python bt_insert_lake.py -d c:\dev\test -s 1
```

Go to Section ② of "Bluetooth Canonicalization (raw → rawjson)" below to check for missing entries.

To check for the presenting of new data, run this in the test environment:
```dos
python bt_insert_lake.py -d path_to_awam* -s 2
```

On the next day, run again to see if the new file is picked up.

### Wavetronix Socrata Ingest (raw)

Update from a couple days ago. Updates should occur from last run date. Example:

```dos
python wt_insert_lake.py -s 2
```

Verify the catalog populated:

```sql
SELECT * FROM api.data_lake_catalog WHERE data_source = 'wt' AND collection_date >= now() - INTERVAL '5 days';
```

Go to Section ① of "Wavetronix Canonicalization (raw → rawjson)" below to test the canonicalization transformation.

Updates should not occur again, unless forced:

```dos
python wt_insert_lake.py -s 2
python wt_insert_lake.py -s 2 -F
```

### GRIDSMART Ingest (raw)
This will need to be run from the Linux-based script server that's on the ATD network since it attempts to access GRIDSMART devices. Set up the environment:

```bash
sudo docker run -it -v ~/git:/app --rm --network=host -w /app/atd-data-lake ctrdocker/tdp /bin/bash
```

Update from a day ago. Updates should occur from last run date. Example:

```bash
python gs_insert_lake.py -s 5
```

Verify the catalog populated. In `psql` or comparable utility:

```sql
SELECT * FROM api.data_lake_catalog WHERE data_source = 'gs' AND collection_date >= now() - INTERVAL '5 days';
```

Go to Section ① of "GRIDSMART Canonicalization (raw → rawjson)" below to test the canonicalization transformation.

Updates should not occur again, unless forced:

```dos
python gs_insert_lake.py -s 5
python gs_insert_lake.py -s 5 -F
```

Go to Section ② of "GRIDSMART Canonicalization (raw → rawjson)" below to check for missing entries.

To check for the presenting of new data, run this in the test environment overnight and verify proper operation. (TODO: Expand this, and document command-lines for running the Launcher)

### Bluetooth Canonicalization (raw → rawjson)

Try running this from a Linux terminal. Set up the environment (substitute paths for yours):

```bash
cd ~/git/atd-data-lake/src
```

① Check that update occurs for the test files:

```bash
python bt_json_standard.py -s 2018-07-01 -e 2018-07-12
```

Check the catalog. On a console into the catalog:

```sql
SELECT * FROM api.data_lake_catalog WHERE data_source = 'bt' AND collection_date BETWEEN '2018-07-01' AND '2018-07-12';
```

Verify that the update doesn't repeat:

```bash
python bt_json_standard.py -s 2018-07-01 -e 2018-07-12
```

Check if the update can be forced:

```bash
python bt_json_standard.py -s 2018-07-01 -e 2018-07-12 -F
```

② Check that missing files are detected:

```bash
python bt_json_standard.py -s 2018-07-01 -e 2018-07-12
```

Verify that the catalog updated with this new file. On the database console:

```sql
SELECT * FROM api.data_lake_catalog WHERE data_source = 'bt' AND collection_date BETWEEN '2018-07-07' AND '2018-07-08';
```

### Wavetronix Canonicalization (raw → rawjson)
① Check that update occurs for the test files:

```bash
python wt_json_standard.py -s 5
```

Verify the catalog populated:

```sql
SELECT * FROM api.data_lake_catalog WHERE data_source = 'wt' AND collection_date >= now() - INTERVAL '5 days';
```

No update should repeat unless forced:

```bash
python wt_json_standard.py -s 2
python wt_json_standard.py -s 2 -F
```

② Check that missing files are detected:
```
python wt_json_standard.py -s 5
```

### GRIDSMART Canonicalization (raw → rawjson)
① Check that update occurs for the test files:

```bash
python gs_json_standard.py -s 2
```

Verify the catalog populated:

```sql
SELECT * FROM api.data_lake_catalog WHERE data_source = 'gs' AND collection_date >= now() - INTERVAL '5 days';
```

No update should repeat unless forced:

```bash
python gs_json_standard.py -s 2
python gs_json_standard.py -s 2 -F
```

② Check that missing files are detected:

```bash
python gs_json_standard.py -s 2
```

### Enrichment (rawjson → ready)

*TODO: Complete this section for each of the data types*. Follow procedures similar to those printed above for testing the "rawjson" to "ready" transformation. This will be factored into the automated process.
