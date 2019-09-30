# Appendix: Testing

*[(Back to Docs Catalog)](index.md)*

This document captures notes on testing the first rendition of codes that write to and read from the Data Catalog. One of the items that will be considered for the FY20 efforts will be to automate testing. Through automation, many of the tests documented here won't need to be run manually.

## Testing the Data Lake Codes
This section describes brief tests for the various codes that comprise the Data Lake transformations.

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

Get to the correct directory and set the environment:

```dos
cd c:\dev\coa_dev\aws_transport
set PYTHONPATH=.;c:\dev\coa_dev
```

Stage file distinction test:

```dos
move c:\dev\test\Austin_bt_07-07-2018.txt c:\dev
```

This is probably a good time to clear out the catalog.

Attempt to update from the temporary directory; updates should occur.

```dos
python bt_insert_lake.py -d c:\dev\test -r 2018-07-01 -m 2018-07-01
```

Check the catalog. On a psql console into the catalog:

```sql
SELECT * FROM api.data_lake_catalog WHERE data_source = 'bt' AND collection_date BETWEEN '2018-07-01' AND '2018-07-12';
```

Run again to verify that no updates occur:

```dos
python bt_insert_lake.py -d c:\dev\test -r 2018-07-01 -m 2018-07-01
python bt_insert_lake.py -d c:\dev\test -m 2018-07-01
python bt_insert_lake.py -d c:\dev\test -m 2018-07-01 -M
```

Check if force works; updates should occur:

```dos
python bt_insert_lake.py -d c:\dev\test -r 2018-07-01 -m 2018-07-01 -F
```

Go to Section ① of "Bluetooth Canonicalization (raw → rawjson)" below to test the canonicalization transformation.

To test a present missing file; first, don't check for missing. No update should occur.

```dos
move c:\dev\Austin_bt_07-07-2018.txt c:\dev\test
python bt_insert_lake.py -d c:\dev\test -r 2019-01-01 -m 2018-07-01
```

Now check for the missing file; update should occur.

```dos
python bt_insert_lake.py -d c:\dev\test -r 2019-01-01 -m 2018-07-01 -M
```

Verify that the catalog updated with this new file. On the database console:

```sql
SELECT * FROM api.data_lake_catalog WHERE data_source = 'bt' AND collection_date BETWEEN '2018-07-07' AND '2018-07-08';
```

Make sure months-old logic runs. (Because of dates of files in that test directory, nothing should update):

```dos
python bt_insert_lake.py -d c:\dev\test -r 2019-01-01 -m 1
```

Go to Section ② of "Bluetooth Canonicalization (raw → rawjson)" below to check for missing entries.

To check for the presenting of new data, run this in the test environment overnight and verify proper operation. (*TODO: Expand this, and document command-lines for running the Launcher*)

### Wavetronix Socrata Ingest (raw)

This section will need to be revised when Wavetronix data is drawn from the KITS database.

Verify latest data is available:
https://data.austintexas.gov/Transportation-and-Mobility/Radar-Traffic-Counts/i626-g7ub

Update from a couple days ago. Updates should occur from last run date. Example:

```dos
python wt_insert_lake.py -r 2019-03-18 -m 2019-03-15
```

Verify the catalog populated:

```sql
SELECT * FROM api.data_lake_catalog WHERE data_source = 'wt' AND collection_date >= '2019-03-15';
```

Go to Section ① of "Wavetronix Canonicalization (raw → rawjson)" below to test the canonicalization transformation.

Updates should not occur again, unless forced:

```dos
python wt_insert_lake.py -r 2019-03-18 -m 2019-03-15
python wt_insert_lake.py -r 2019-03-18 -m 2019-03-15 -F
```

Detect missing entries; missing entries should update:

```dos
\software\Python37\python wt_insert_lake.py -r 2019-03-18 -m 2019-03-15 -M
```

Go to Section ② of "Wavetronix Canonicalization (raw → rawjson)" below to check for missing entries.

To check for the presenting of new data, run this in the test environment overnight and verify proper operation. (*TODO: Expand this, and document command-lines for running the Launcher*)

### GRIDSMART Ingest (raw)
This will need to be run from the Linux-based script server that's on the ATD network since it attempts to access GRIDSMART devices. Set up the environment:

```bash
cd ~/git/coa_dev/aws_transport
export PYTHONPATH=$PYTHONPATH:~/git/coa_dev
```

Update from a day ago. Updates should occur from last run date. Example:

```bash
python gs_insert_lake.py -r 2019-03-19 -m 2019-03-18
```

Verify the catalog populated:

```sql
SELECT * FROM api.data_lake_catalog WHERE data_source = 'gs' AND collection_date >= '2019-03-19';
```

Go to Section ① of "GRIDSMART Canonicalization (raw → rawjson)" below to test the canonicalization transformation.

Updates should not occur again, unless forced:

```dos
python gs_insert_lake.py -r 2019-03-19 -m 2019-03-18
python gs_insert_lake.py -r 2019-03-19 -m 2019-03-18 -F
```

Detect missing entries; missing entries should update:

```dos
python gs_insert_lake.py -r 2019-03-19 -m 2019-03-18 -M
```

Go to Section ② of "GRIDSMART Canonicalization (raw → rawjson)" below to check for missing entries.

To check for the presenting of new data, run this in the test environment overnight and verify proper operation. (TODO: Expand this, and document command-lines for running the Launcher)

### Bluetooth Canonicalization (raw → rawjson)

Try running this from a Linux terminal. Set up the environment (substitute paths for yours):

```bash
cd ~/git/coa_dev/aws_transport
export PYTHONPATH=$PYTHONPATH:~/git/coa_dev
```

① Check that update occurs for the test files:

```bash
python bt_json_standard.py -r 2018-07-01 -m 2018-07-01
```

Check the catalog. On a console into the catalog:

```sql
SELECT * FROM api.data_lake_catalog WHERE data_source = 'bt' AND collection_date BETWEEN '2018-07-01' AND '2018-07-12';
```

Verify that the update doesn't repeat:

```bash
python bt_json_standard.py -r 2018-07-01 -m 2018-07-01
```

Check if the update can be forced:

```bash
python bt_json_standard.py -r 2018-07-01 -m 2018-07-01 -F
```

② Check that missing files are not detected unless specifically directed. No updates should occur:

```bash
python bt_json_standard.py -r 2018-07-01 -m 2018-07-01
```

Update should occur of the newly-added file:

```bash
python bt_json_standard.py -r 2018-07-01 -m 2018-07-01 -M
```

Verify that the catalog updated with this new file. On the database console:

```sql
SELECT * FROM api.data_lake_catalog WHERE data_source = 'bt' AND collection_date BETWEEN '2018-07-07' AND '2018-07-08';
```

### Wavetronix Canonicalization (raw → rawjson)
① Check that update occurs for the test files:

```bash
python wt_json_standard.py -r 2019-03-18 -m 1
```

Verify the catalog populated:

```sql
SELECT * FROM api.data_lake_catalog WHERE data_source = 'wt' AND collection_date >= '2019-03-15';
```

No update should repeat unless forced:

```bash
python wt_json_standard.py -r 2019-03-18 -m 1
python wt_json_standard.py -r 2019-03-18 -m 1 -F
```

② Check that missing files are not detected unless specifically directed. No updates should occur:

```bash
python wt_json_standard.py -r 2019-03-20 -m 1
```

Enable uploading of missing files:
```
python wt_json_standard.py -r 2019-03-20 -m 1 -M
```

### GRIDSMART Canonicalization (raw → rawjson)
① Check that update occurs for the test files:

```bash
python gs_json_standard.py -r 2019-03-19 -m 1
```

Verify the catalog populated:

```sql
SELECT * FROM api.data_lake_catalog WHERE data_source = 'gs' AND collection_date >= '2019-03-19';
```

No update should repeat unless forced:

```bash
python gs_json_standard.py -r 2019-03-19 -m 1
python gs_json_standard.py -r 2019-03-19 -m 1 -F
```

② Check that missing files are not detected unless specifically directed. No updates should occur:

```bash
python gs_json_standard.py -r 2019-03-20 -m 1
```

Enable uploading of missing files:

```bash
python gs_json_standard.py -r 2019-03-20 -m 1 -M
```

### Bluetooth Enrichment (rawjson → ready)

*TODO: Complete this section*. This will be factored into the automated process.

### Wavetronix Enrichment (rawjson → ready)

*TODO: Complete this section*

### GRIDSMART Enrichment (rawjson → ready)

*TODO: Complete this section*
