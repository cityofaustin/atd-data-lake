"""
perfmet_knack.py: Handling of loading Knack data for performance metrics

Kenneth Perrine - Center for Transportation Research, The University of Texas at Austin
April 18, 2020
"""
from argparse import ArgumentParser, RawDescriptionHelpFormatter
import time
import datetime

import pandas as pd

import _setpath
import knackpy
from drivers.devices import perfmet_postgrest
from config import config_app
from util import date_util

PROGRAM_DESC = "Transfers performance metrics to Knack, clearing out old data in Knack."

# Number of days of the week to do a sampling:
SAMPLE_DAYS = 7

# Number of Knack API calls to regulate per second:
REGULATE_CALLS = 9

# Maxumum number of times to retry a call in regulate()
MAX_TRIES = 5

# Defines the field names for the Knack "Jobs" view:
KNACK_JOB_VIEW = {"scene": "scene_4",
                  "view": "view_4",
                  "fields": {"data_source": "field_9",
                             "stage": "field_10",
                             "seconds": "field_11",
                             "records": "field_12",
                             "processing_date": "field_13",
                             "collection_times": "field_14"}
                  }

# Defines the field names for the Knack "Obs" view:
KNACK_OBS_VIEW = {"scene": "scene_5",
                  "view": "view_5",
                  "fields": {"data_source": "field_15",
                             "sensor_name": "field_16",
                             "data_type": "field_17",
                             "data": "field_18",
                             "expected": "field_19",
                             "collection_date": "field_20",
                             "timestamp_range": "field_21",
                             "timestamp_range_min": "field_23",
                             "timestamp_range_max": "field_24"}
                  }

def retrieveJobs():
    "Obtains all job information from Knack as raw data."
    kJob = regulate(lambda: knackpy.App(app_id=config_app.KNACK_PERFMET_ID,
                                        api_key="knack").get(scene=KNACK_JOB_VIEW["scene"],
                                                             view=KNACK_JOB_VIEW["view"],
                                                             generate=True))
    kJob = [job.raw for job in kJob]
    return kJob

def retrieveObservations():
    "Obtains all observations information from Knack as raw data."
    kObs = regulate(lambda: knackpy.App(app_id=config_app.KNACK_PERFMET_ID,
                                        api_key="knack").get(scene=KNACK_OBS_VIEW["scene"],
                                                             view=KNACK_OBS_VIEW["view"],
                                                             generate=True))
    kObs = [obs.raw for obs in kObs]
    return kObs
    
def delete(jobData, obsData):
    "Uses the IDs in the jobs and observations raw returns to clear out records in Knack."
    for record in obsData:
        regulate(lambda: record_view(record,
                                     app_id=config_app.KNACK_PERFMET_ID,
                                     api_key="knack",
                                     method="delete",
                                     scene=KNACK_OBS_VIEW["scene"],
                                     view=KNACK_OBS_VIEW["view"]))
    for record in jobData:
        regulate(lambda: record_view(record,
                                     app_id=config_app.KNACK_PERFMET_ID,
                                     api_key="knack",
                                     method="delete",
                                     scene=KNACK_JOB_VIEW["scene"],
                                     view=KNACK_JOB_VIEW["view"]))

def localTimeStruct(timeStr):
    "Returns the 'specific times' time structure for Knack from the given time string."
    ourTime = date_util.localize(date_util.parseDate(timeStr))
    return {"date": ourTime.strftime("%m/%d/%Y"),
            "hours": int(ourTime.strftime("%I")),
            "minutes": ourTime.minute,
            "am_pm": ourTime.strftime("%p")}

def uploadJobs(jobs):
    "Pushes jobs out to Knack."
    fields = KNACK_JOB_VIEW["fields"]
    for _, job in jobs.iterrows():
        record = {fields["data_source"]: job["data_source"],
                  fields["stage"]: job["stage"],
                  fields["seconds"]: job["seconds"],
                  fields["records"]: job["records"],
                  fields["processing_date"]: localTimeStruct(job["processing_date"])}
        if job["collection_start"]:
            record[fields["collection_times"]] = {"times": [{"from": localTimeStruct(job["collection_start"]),
                                                             "to": localTimeStruct(job["collection_end"])}]}
        regulate(lambda: record_view(record,
                                     app_id=config_app.KNACK_PERFMET_ID,
                                     api_key="knack",
                                     method="create",
                                     scene=KNACK_JOB_VIEW["scene"],
                                     view=KNACK_JOB_VIEW["view"]))

def processObs(perfMetDB, jobs, dataSource, stage, obsType, sampleDays=SAMPLE_DAYS, calcExpected=False):
    "Reads observations from the database and prepares them for sending to Knack."
    print("Processing new Knack '%s' observations..." % dataSource)
    rec = jobs[(jobs["data_source"] == dataSource) & (jobs["stage"] == stage)].copy()
    if len(rec) == 0:
        print("WARNING: No entry for '%s'/'%s' was found in etl_perfmet_job!" % (dataSource, stage))
        return None
    
    # Get processing record that covers the latest date:
    rec.sort_values("collection_end", ascending=False, inplace=True)
    rec = rec.iloc[0]
    
    # Retrieve observations for the given date range:
    lateDate = date_util.localize(date_util.parseDate(rec["collection_end"]))
    earlyDate = lateDate - datetime.timedelta(days=sampleDays)
    observations = perfMetDB.readAllObs(lateDate, earlyDate=earlyDate, dataSource=dataSource, obsType=obsType)
    if not observations:
        print("WARNING: No observations are found for '%s', type '%s'." % (dataSource, obsType))
        return None
    observations = pd.DataFrame(observations)
    observations["collection_date"] = observations["collection_date"].apply(lambda t: date_util.localize(date_util.parseDate(t)))
    observations.sort_values("collection_date", ascending=False, inplace=True)

    # Pick out the one that covers the latest date:
    yesterday = lateDate - datetime.timedelta(days=1)
    # TODO: If we end up processing hourly, then we'll need to change this beginning time mechanism.
    obsSubset = observations[(observations["collection_date"] <= lateDate) & (observations["collection_date"] >= yesterday)]
    obsSubset = obsSubset.loc[obsSubset.groupby("sensor_name")["collection_date"].idxmax()]
    maxes = observations.loc[observations.groupby("sensor_name")["collection_date"].idxmax()]
    avgs = observations.groupby("sensor_name")["data"].mean()
    ret = pd.DataFrame()
    for index, obs in maxes.iterrows():
        # TODO: There's probably some fancy merge that we could do to make this process easier.
        if index not in obsSubset.index:
            # No recent entry available for this day! Make a fake entry, which is the most recent entry that had data.
            # That way, we can see when the data stopped.
            rec = maxes.loc[index].copy()
            rec["data"] = -1
        else:
            rec = obsSubset.loc[index].copy()
        if calcExpected:
            rec["expected"] = avgs[obs["sensor_name"]]
        ret = ret.append(rec)
    _uploadObs(yesterday, ret)
    return ret

def _uploadObs(targetDate, observations):
    "Called by processObs."        
    fields = KNACK_OBS_VIEW["fields"]
    for _, obs in observations.iterrows():
        record = {fields["data_source"]: obs["data_source"],
                  fields["sensor_name"]: obs["sensor_name"],
                  fields["data_type"]: obs["data_type"],
                  fields["data"]: obs["data"],
                  fields["expected"]: obs["expected"],
                  fields["collection_date"]: localTimeStruct(obs["collection_date"])}
        if obs["timestamp_min"]:
            record[fields["timestamp_range"]] = {"times": [{"from": localTimeStruct(obs["timestamp_min"]),
                                                            "to": localTimeStruct(obs["timestamp_max"])}]}
            day = date_util.roundDay(date_util.localize(obs["collection_date"]))
            record[fields["timestamp_range_min"]] = max((date_util.localize(date_util.parseDate(obs["timestamp_min"])) - day).total_seconds() / 3600, 0)
            record[fields["timestamp_range_max"]] = min((date_util.localize(date_util.parseDate(obs["timestamp_max"])) - day).total_seconds() / 3600, 24)
        regulate(lambda: record_view(record,
                                     app_id=config_app.KNACK_PERFMET_ID,
                                     api_key="knack",
                                     method="create",
                                     scene=KNACK_OBS_VIEW["scene"],
                                     view=KNACK_OBS_VIEW["view"]))

def regulate(function):
    """
    Adds a time delay to repeated calls to this function in order to avoid 'Service Unavailable'
    errors when accessing Knack.
    """
    if not hasattr(regulate, "counter"):
        regulate.counter = 0
    if not hasattr(regulate, "lastTime"):
        regulate.lastTime = datetime.datetime.now()

    curTime = datetime.datetime.now()
    if (curTime - regulate.lastTime).total_seconds() >= 1:
        regulate.lastTime = curTime
        regulate.counter = 0
    if regulate.counter >= REGULATE_CALLS:
        if (curTime - regulate.lastTime).total_seconds() < 1:
            time.sleep(1 - (curTime - regulate.lastTime).total_seconds())
    regulate.counter += 1
    for tries in range(MAX_TRIES):
        try:
            return function()
        except Exception as exc:
            if not (tries < MAX_TRIES and str(exc).endswith("connection termination")):
                raise exc
            print("WARNING: Got transmission exception in regulate() on Try #%d. Trying again." % (tries + 1))

"""
** Knack API Utility Function **

This is copied/pasted from knackpy and altered to provide view-based access. View-based is
far preferable to object-based because view-based updates and deletes don't count against
the access limits imposed by Knack.
"""
def record_view(
    data,
    app_id=None,
    api_key=None,
    id_key="id",
    method=None,
    scene=None,
    view=None,
    timeout=10,
):

    """
    Knack API request wrapper creating, updating, and deleting Knack records.
    """
    endpoint = "https://api.knack.com/v1/pages/{}/views/{}/records".format(scene, view)
    
    if method != "create":
        _id = data[id_key]
        endpoint = "{}/{}".format(endpoint, _id)

    if method == "create":
        method = "POST"

    elif method == "update":
        method = "PUT"

    elif method == "delete":
        method = "DELETE"

    else:
        raise Exception("Invalid method: {}".format(method))

    headers = {"x-knack-application-id": app_id,
               "x-knack-rest-api-key": api_key,
               "Content-type": "application/json"}

    return knackpy.api._request(data=data,
                                url=endpoint,
                                headers=headers,
                                method=method,
                                timeout=timeout)

def main():
    # Parse command-line parameter:
    parser = ArgumentParser(description=PROGRAM_DESC, formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument("-r", "--last_run_date", help="last run date, in YYYY-MM-DD format with optional time zone offset (default: yesterday)")
    args = parser.parse_args()

    date_util.setLocalTimezone(config_app.TIMEZONE)
    if args.last_run_date:
        lastRunDate = date_util.parseDate(args.last_run_date, dateOnly=True)
        print("perfmet_knack: Last run date: %s" % str(lastRunDate))
    else:
        lastRunDate = date_util.roundDay(date_util.localize(datetime.datetime.now())) - datetime.timedelta(days=1)

    # Find the most recent day for performance metrics:
    print("Finding most recent processing date...")
    perfMetDB = perfmet_postgrest.PerfMetDB(needsObs=True)
    recent = perfMetDB.getRecentJobsDate()
    if not recent:
        print("ERROR: No recent processing date is found in the performance metrics DB.")
        return -1
    recent = date_util.roundDay(date_util.localize(date_util.parseDate(recent)))
    if recent < lastRunDate:
        print("ERROR: No processing date exists after %s" % str(lastRunDate))
        return -1
    print("The most recent processing date is %s." % str(recent))

    # Performe the activities:
    print("Retrieving old Knack entries...")
    jobData = retrieveJobs()
    obsData = retrieveObservations()

    print("Deleting old Knack entries...")
    delete(jobData, obsData)
    
    print("Uploading new Knack Jobs entries...")
    # Perform all retrieval and custom processing to get data ready for Knack:
    jobs = perfMetDB.readAllJobs(recent)
    jobs = pd.DataFrame(jobs)
    jobs = jobs.sort_values("processing_date").groupby(["data_source", "stage"]).tail(1)
    jobs["stage"].replace("Socrata Agg.", "Socrata", inplace=True)
    jobs["stage"].replace("Ingest", "a. Ingest", inplace=True)
    jobs["stage"].replace("Standardize", "b. Standardize", inplace=True)
    jobs["stage"].replace("Ready", "c. Ready", inplace=True)
    jobs["stage"].replace("Aggregate", "d. Aggregate", inplace=True)
    jobs["stage"].replace("Socrata", "e. Socrata", inplace=True)

    uploadJobs(jobs)
    
    # Deal with observations here:
    processObs(perfMetDB, jobs, "Bluetooth", "b. Standardize", "Unmatched Entries", calcExpected=True)
    processObs(perfMetDB, jobs, "GRIDSMART", "b. Standardize", "Vehicle Counts", calcExpected=True)
    
    print("Done.")
    return 1

if __name__ == "__main__":
    main()
