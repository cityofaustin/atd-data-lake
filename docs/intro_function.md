# ATD Data Lake Architecture: Intro and Functionlity

*[(Back to Docs Catalog)](index.md)*

There is a growing need for cities to make data they collect publicly accessible. Open data allows users to make meaningful insights and to address current and future planning challenges. The Austin Transportation Department (ATD) has launched the Data Lake, a data integration initiative to store and archive the vast amount of information gathered by sensors within Austin's city limits.

The Data Lake leverages low-cost Amazon Web Services (AWS) cloud storage solutions to store and archives sensor data while preserving its original integrity, thus allowing ATD to re-process the data at a later time to generate new analyses as the need arises.

ATD's Data Lake is a layered system where raw data is stored and processed intermediaries are stored as JSON files. The layered system leverages the significantly lower cost of cloud storage over computing power where the processed layers can be batched or processed nightly on small-scale cloud instances, depending on city resources. Further, to allow flexibility in meeting various data needs, end-user products (whether a database or a CSV file dump) depend on use-cases. The Data Lake architecture is explained below to show how it is intended to achieve its objectives.

## Overall Structure

The ATD Data Lake architecture has the following essential components:
1. A catalog (implemented with PostgREST, but not difficult to switch out to another implementation)
2. The "raw" Layer 1 repository (implemented with AWS S3, again, not difficult to switch to another provider)
3. The canonical raw data Layer 2 repository ("rawjson", implemented with AWS S3)
4. The processed data Layer 3 repository ("ready", implemented with AWS S3)
5. Published data target (implemented with Socrata)

Together, the components preserve the integrity of the sensor data, facilitate stop-and-go processing, and promote ease of data exchange.

<img src="figures/pipeline.png" width="900">

## Digging Deeper

The catalog contains pointers to the data files with collection and processing dates and other metadata. By keeping track of each processing step for each data file, the catalog allows flexibility in stop-and go processing. The "raw" repository has a year/month/day/data source file structure that preserves the integrity of the original data. Processes running within city infrastructure read data from sources, uploads them to the "raw" repository, and catalogs them. 

Similarly, the canonical raw data repository retains the integrity of raw data while making it accessible for further processing through unpacking files and canonizing to JSON. The idea is that if a mistake is made, processes such as unpacking files do not have to be repeated. To get data into the canonical (Layer 2) repository, an algorithm gathers the data files needing to be processed, fetches them from the first repository, and canonizes them to a standadized JSON format. Standardization is a good common practice for data integration efforts and a JSON specification provides a "portable representation of structured data" [1] and is less verbose than, for example, XML.

<img src="figures/data_lake.png" width="700">

The canonical raw (Layer 2) repository file structure follows that of the raw (Layer 1) repository, plus additional files that archive and catalog sensor information. A process within the cloud framework reads from the city infrastructure to attain the sensor information to then merge with the actual data. 

Finally, a process within the cloud framework gathers data files from Layer 2, then processes, catalogs and uploads the data into the third (Layer 3) repository. However, in processing data files from the second layer to the third, the data is aggregated and merged with sensor information. This way, all data files within Layer 3 are largely self-contained. As such, further temporal or spatial aggregations are easily computed and custom end-user products can be delivered. Again, JSON allows ease of data exchange through APIs.

![AWS Bucket Structure](figures/bucket_file_structure.png)

## Challenges and Design Criteria

The design of the Data Lake incorporates considerations on other designs learned about in other cities' open data efforts, including:

* Cloud services have a number of features that are proprietary and lead to "vendor lock-in." We wanted to minimize the use of vendor-specific APIs.
* Cloud services bill for various resources. We want to use resources in the most cost-effective way. In comparing cloud offerings, the flat-file storage capabilities of S3 was found to be far more cost-effective than relational or noSQL database offerings.
  * The cloud service landscape changes, so costs should be surveyed on a yearly basis.
  * There is a need to have a fine-grained ability to measure the costs on specific resources.
* Live data updates are not a high priority currently. An end-of-day batch processing model was targeted.
  * The code that performs the data transformations must be as simple as possible. Efforts in FY20 streamlined code, improved testability, and consolidated common functionlitiy into libraries.
  * The code can be structured to better facilitate line-by-line live updates.
* Different repositories offer different security and cost mechanisms. Sensitive data can be anonymized in downstream processing. Other parties may be given access to specific resources in such a way that the cost of data extraction may be passed on to them.

The following summarizes findings emerging from conference calls with data practitioners from Portland, Oregon, and Denver, Colorado:

* There is a fundamental difference between designing an architecture for archiving historical data and streaming sources of data (e.g., Portland's approach).
* Clearly articulated use cases can greatly help with communicating ideas and working toward goals.
* Positive aspects of Austin's design are its open source and its ability to process data at specific, predermined times over desired time ranges.
* Disadvantages include its lack of streaming ability and its dependence on custom coding.

## Dockless Mobility

In considering future data analysis capabilities, this project looked at the possibility of working with dockless mobility (e.g., scooter) data. Current major restrictions are:

* GPS coordinates are truncated to .001 (~100 x 100m block) 
* Trip trajectories are not available

The restrictions are largely imposed to protect personal identifiable information (PII). Data sharing agreements would be required in order to analyze data that offers more details. Questions were asked about what could be done if access to detailed data were available:

* What is the relationship between micro-mobility and transit? Do people choose to use scooters instead of transit? Or do scooters facilitate getting to/going from transit stops?
* What does a "tourist" trip trajectory look like vs. a "commuter" trip?

Efforts for establishing a data sharing agreement with a dockless mobility provider are currently being pursued and evaluated.

## Citations
[1] https://www.ietf.org/rfc/rfc4627.txt
