# ATD Data Lake Architecture: Wavetronix

*[(Back to Technical Architecture)](tech_architecture.md)*

Data is composed of radar vehicle counts mid-block. The next figure overviews the system architecture for this data type. *Grayed-out items in the diagram depict items that are currently not implemented.*

| Wavetronix System Architecture <br><img src="Figures/new_wt_overview.png">
|---

## Layer 1 Raw Data

To get started, Wavetronix data was read from the ["Radar Traffic Counts"](https://data.austintexas.gov/Transportation-and-Mobility/Radar-Traffic-Counts/i626-g7ub) Socrata page. Since the end-goal is to eventually get Wavetronix data *out* to Socrata, this is to be replaced with grabbing Socrata data from a more immediate source that's closer to the sensors themselves. Currently, the approach will be to read records from the KITS database.

The current extractions are CSV files read from Socrata via the API. An excerpt of a CSV file is below:

```
curdatetime,day,day_of_week,detid,detname,direction,hour,int_id,intname,minute,month,occupancy,row_id,speed,timebin,volume,year
2019-06-02T01:30:01.000,2,0,31,Lane2,None,1,10,LamarBroken Spoke,30,6,2,003efbd6407ba2ff65031feddd817b6d,38,01:30,48,2019
2019-06-02T01:45:01.000,2,0,93,NB_in,NB,1,24,LOOP 360LAKEWOOD,45,6,0,00b25d9657fe0195b466e6fdce76d735,44,01:45,5,2019
2019-06-02T02:15:00.000,2,0,43,NB,NB,2,14,Robert E LeeBarton Springs,15,6,0,0123f20364f8fee57d42cf3727d4ec4c,29,02:15,11,2019
2019-06-02T02:00:00.000,2,0,84,SB_in,SB,2,22,LAMARCOLLIER,0,6,2,01b2c2997b7738c3145205209028db81,40,02:00,55,2019
2019-06-02T02:00:00.000,2,0,85,NB_in,NB,2,22,LAMARCOLLIER,0,6,1,0312403e5f78c48467b27dbf47eb8842,41,02:00,24,2019
```

When the final structure is in place, this will be documented more in detail.

## Layer 2 JSON Data
The JSON transformation of the above data appears as this:

```json
{
	"header": {
		"data_source": "wavetronix",
		"origin_filename": "wavetronix_2019-06-02.csv",
		"target_filename": "wavetronix_2019-06-02.json",
		"collection_date": "2019-06-02 00:00:00-05:00",
		"processing_date": "2019-06-03 04:30:03.404929-05:00"
	},
	"data": [{
			"curdatetime": 2,
			"day": 0,
			"day_of_week": 31,
			"detid": "Lane2",
			"direction": "None",
			"hour": 1,
			"int_id": 10,
			"intname": "LamarBroken Spoke",
			"minute": 30,
			"month": 6,
			"occupancy": 2,
			"row_id": "003efbd6407ba2ff65031feddd817b6d",
			"speed": 38,
			"timebin": "01:30",
			"volume": 48,
			"year": 2019
		}, ...
	]
}
```
