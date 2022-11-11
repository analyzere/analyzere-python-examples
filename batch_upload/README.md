# Batch Upload Tool

## Overview

The Python Batch Upload Tool demonstrates how Python can be leveraged to
retrieve data from sources like CSV and SQL and upload them to the Analyze
Re Prime platform. It is primarily intended as an example to illustrate the
use of the Analyze Re API through Python and how data can be easily
tranformed to meet the requirements of the Analyze Re platform.

However, it is also meant to serve as a useful stand-alone tool for
uploading batches of data to the Analyze Re platform as part of an
operational process.

## Data Format Requirements

The Batch Upload Tool can be used to create a large set of layers on the
platform and for each layer upload a corresponding loss set to the
platform.

The general data input format expected by the Batch Upload Tool consists of
two data sets: **Layer Definitions** and **Loss Set Data**. The schema of
each data set is described in detail below. Note, that the naming of the
relevant columns can be conveniently adjusted in the `config.ini`
configuration file.

### Layer Definitions
  
The layer definitions is a tabular data set that specifies each
individual layer that is to be created on the platform. This definition
contains information about the layer type, any financial terms associated
with the layer and any metadata that should be stored with the layer. The
definition also includes additional metadata about the loss set
associated with the layer.

The general schema of the layer definitions data set is as follows:

| Column Name | Description | Required | Default |
|-|-|-|-|
|`layer_id` | User-defined ID for the layer. This is used as a key to identify loss sets in the loss sets data set. | Yes | None |
|`loss_set_ccy` | Three-letter currency code of the currency of loss values in the loss set. | No | Fall-back to `currency`. |
|`loss_set_start_date` | For YELT loss sets, the absolute start date a reference for `sequence` offsets. | Yes for YELT loss sets. | None |
|`layer_type` | `CatXL`, `QuotaShare`, `AggXL`, or `Generic` | Yes | None |
|`description` | Value of the layer's description field to be stored. | No | None |
|`inception_date` | Inclusive date of the contract's inception. | No | None |
|`expiry_date` | Exclusive date of the contract's expiry. | No | None |
|`participation` | The contract's participation or share. | Yes | None |
|`premium` | The contract's upfront premium amount. | No | None |
|`premium_ccy` | The contract premium's three-letter currency code. | No | Fall-back to `currency`. |
|`attachment` | The contract's occurrence attachment (CatXL, AggXL, and Generic only). | Yes | None |
|`attachment_ccy` | The contract's occurrence attachment's three-letter currency code. | No | Fall-back to `currency`. |
|`limit` | The contract's occurrence limit (CatXL, AggXL, and Generic only). | Yes | None |
|`limit_ccy` | The contract's occurrence limit's three-letter currency code. | No | Fall-back to `currency`. |
|`aggregate_attachment`  | The contract's aggregate attachment (AggXL and Generic only). | Yes | None |
|`aggregate_attachment_ccy` | The contract's aggregate attachment's three-letter currency code. | No | Fall-back to `currency`. |
|`aggregate_limit` | The contract's aggregate limit (AggXL and Generic only). | Yes | None |
|`aggregate_limit_ccy` | The contract's aggregate limit's three-letter currency code. | No | Fall-back to `currency`. |
|`event_limit` | The contract's event limit (Quota Share only). | Yes | None |
|`event_limit_ccy` | The contract's event limit's three-letter currency code. | No | Fall-back to `currency`. |
|`franchise` | The contract's franchise deductible (CatXL, AggXL, and Generic only). | Yes | None |
|`franchise_ccy` | The contract's franchise deductible's three-letter currency code. | No | Fall-back to `currency`. |
|`reinstatements` | Reinstatement provisions with separate premium and brokerage specified for each reinstatement. For example `0.1;0.05\|0.07;0.05` refers to two reinstatements with the first reinstatement at 10% premium and 5% brokerage and the second reinstatement at 7% premium and 5% brokerage. | No | Fall back to `reinstatement_count`. |
|`reinstatement_count` | This field can be mutually exclusively used with `reinstatements`. When specified, this represents the total number of reinstatements and reinstatement premium and brokerage provisions are defined through the `reinstatement_premium` and `reinstatement_brokerage` fields. | No | `0` |
|`reinstatement_premium` | The reinstatement premium percentage for all reinstatements specified with `reinstatement_count`. | Yes | None |
|`reinstatement_brokerage` | The reinstatement brokerage percentage for reinstatements specified with `reinstatement_brokerage`. | Yes | None |
|`nth` | The n-thness of attachment for a CatXL layer. | No | `1` |
|`currency` | The layer currency as a fall-back where otherwise currency is not explicitly specified, but required. | Yes | None |

The naming of these columns can be adjusted in the `config.ini`
configuration file.

### Loss Set Data
  
The loss set data set is a tabular data set that contains all loss set
data for the collection of all loss sets associated with the layers in
the layer definitions. The loss sets in this data set are keyed / indexed
by a user-defined loss set ID, which in turn is used in the layer
definitions data set to associated a layer with a loss set.

The general schema of the loss set data set is as follows:

| Column Name | Description | Required | Default |
|-|-|-|-|
| `loss_set_id` | User-defined ID for the loss set. This ID must match the ID of the layer in the layer definitions data set. | Yes | None |
| `event_id` | Event ID for an event that is already defined in the event catalog. Used for ELT and YELT loss sets only. | Yes (ELT, YELT) | None |
| `loss` | The loss amount in the loss set currency. | Yes | None |
| `trial_id` | The trial ID, "year", scenario, etc. Used for YELT and YLT loss sets only | Yes (YELT, YLT) | None |
| `day` | The time offset of the occurrence from the start date in fractional days (0-based). Used for YELT loss sets only.| Yes (YELT) | None |
| `reinstatement_premium` | The amount of reinstatement premium received for the given occurrence or trial in loss set currency. Used for YELT and YLT loss sets only. | No | `0` |
| `reinstatement_brokerage` | The amount of reinstatement brokerage paid for the given occurrence or trial in loss set currency. Used for YELT and YLT loss sets only. | No | `0` |

The naming of these columns can be adjusted in the `config.ini`
configuration file.

## Setup

The Batch Upload example uses [Poetry](https://python-poetry.org/) for
package and dependency management. Poetry can be easily installed in your
system using either `pip` or `conda`.

Once Poetry is available on your system, the steps to setup the Batch
Upload tool are as follows:

1. Clone or download the Git repository
2. Change into the `batch_upload` folder within the repository
3. Have Poetry set up a virtual environment and install required
   dependencies:

   ```shell
   $ poetry install
   ```

4. Edit the `config.ini` file to adjust the column name mapping to align
   with the column naming conventions used in your files or database. The
   relevant sections in `config.ini` are called `[layer_columns]` and
   `[loss_set_columns]`.

## Usage

The Batch Upload tool is a command-line tool which allows data to be
extracted from different sources. Assuming you have set up a Poetry
environment as described above, you can run the tool with the following
command-line:

```shell
$ poetry run python batch_upload.py 
usage: batch_upload.py [-h] [--url URL] [--username USERNAME] [--password PASSWORD] [--config CONFIG] [--batch-id BATCH_ID] SOURCE ...
batch_upload.py: error: the following arguments are required: SOURCE
```

Without any additional command-line arguments the script will produce an
error as it requires the data source argument `SOURCE`, which specifies
from which data source data is extracted from.

Currently, the Batch Upload tools supports two types of data sources:

- `csv`: upload from CSV files
- `sql`: upload from SQL queries via ODBC connection

The data source is selected on the command-line as a separate argument that
specifies the data source type (e.g. `csv` or `sql`).

Depending on the selected data source, the tool may require additional
arguments which are described below in their respective sections.

### Common Arguments

In addition to the data source specific arguments, the tool also accepts
common arguments that are applicable regardless of the data source.

The common command-line arguments that can be specified regardless of the
data source are:

- `--config`: Path to the configuration file that should be used. (default:
  `config.ini`)
- `--url`: Base URL of the Analyze Re Prime API to connect to.
- `--username`: Username for authentication with the API.
- `--password`: Password for authentication with the API.
- `--batch-id`: A unique identifier for the batch run. The batch ID is
  included in the metadata of the objects created. It facilitates to
  quickly identifying objects created as part of a specific batch run.

The `--url`, `--username`, and `--password` arguments can also be specified
within the configuration file. When used on the command-line, their
command-line value overwrites the value specified in the configuration.

### CSV

When the `csv` data source is selected the Batch Upload tool requires a
number of additional arguments to the specified on the command-line:

```shell
$ poetry run python batch_upload.py csv
usage: batch_upload.py csv [-h] --layers LAYERS_CSV (--elt ELT | --yelt YELT | --ylt YLT)
batch_upload.py csv: error: the following arguments are required: --layers
```

- `--layers`: This argument specifies a CSV file that contains the [Layer
  Definitions](#layer-definitions) following the schema described above.
- `--elt` or `--yelt` or `--ylt`: Respectively and **mutually exclusively**
  specify a CSV file containing [Loss Set Data](#loss-set-data) following
  the schema described above.

For example, one might invoke the Batch Upload Tools to upload a set of
layers and ELT loss sets from CSV as follows:

```shell
$ poetry run python batch_upload.py csv --layers layer_definitions.csv --elt layer_elt_loss_sets.csv
```

### SQL

A selection of the `sql` data source on the command-line does not require
any additional command-line arguments. All configuration for this data
source is provided through the configuration file `config.ini` which
requires the following parameters as part of the `[sql]` section:

```ini
[sql]
# ODBC Driver String
driver = {ODBC Driver 18 for SQL Server}
# The host / server to connect to. On Windows this supports server 
# instances of the form HOST\INSTANCE, otherwise hostname and port must be 
# provided. 
server = tcp:localhost,1433
# The database to connect to.
database = master
# Database user's username
username = sa
# Database user's password
password = Strong@Passw0rd
# The type of loss set that is extracted from the server. Valid options are
# `elt`, `yelt`, and `ylt`.
loss_type = yelt
# The SQL query to be executed to return the Layer Definitions.
layers_query = 
    SELECT *
    FROM Layers
# The SQL query to be executed to return the Loss Set Data.
losses_query = 
    SELECT * 
    FROM YELTLosses

```

The `layers_query` and the `losses_query` should respectively
return [Layer Definitions](#layer-definitions) and [Loss Set
Data](#loss-set-data) following the schemas described above.

## Under the Hood

This section describes some of the inner workings of the tool with the goal
of serving two purposes:

1. Provide examples and starting points for other developers who want to
   develop applications using the Analyze Re API.
2. Offer enough orientation, such that other might contribute to the tool's
   future capabilities.

### Architecture

The tool's execution is divided into three main stages:

1. **Data Retrieval:** This is implemented in the `retriever` modules and
   provides an abstract high-level interface to retrieve data from a data
   source and capture this data in a Pandas DataFrame.
2. **Data Extraction:** Data extraction is implemented by the `extractor`
   modules which provide accessors to the Pandas DataFrame retrieved in the
   first stage. The data extraction module is also responsible for
   appropriate type conversation and parsing of additional structured
   elements in the data (e.g. reinstatement definitions).
3. **Data Upload:** The data upload stage implements the interface between
   the tools and the Analyze Re Prime API. All API specific code is
   isolated to this module. The module leverages the [Analyze Re Python
   bindings for the Analyze Re Prime
   API](https://pypi.org/project/analyzere/).

All three stages are orchestrated through the main program in
`batch_upload.py`. Although, the Data Upload module directly interfaces with
the Data Extraction modules to provide a higher-level access interface to
the data frames.

### Extending

There are likely a number of ways this tool can be extended with additional
functionality either for one's internal use cases or for the general
userbase of the tool. The most common extension use cases are likely:

#### Support for Additional Data Sources

In addition to the CSV and SQL/ODBC retriever modules, there is opportunity
for retriever modules that support different data sources. For example
different database systems that do not support SQL/ODBC, document stores
(e.g. MongoDB), or object storage systems (AWS S3).

Adding support for a new data source often simply only requires the
implementation of a new retriever module as long as it can provide the data
for downstream processing as Pandas DataFrames.

#### Support for Additional Contract Types

The addition of other contract types is mostly isolated to the uploader
module. It is the only module that performs a higher-level interpretation
of the data provided in the [Layer Definitions](#layer-definitions). The
extractors provide some interpretation, specifically for monetary unit
values and reinstatement provisions, but it is mostly generic and
tranferrable across contract types. Additional contract types can likely be
implemented in isolation in the uploader module.

#### Complex Data Retrieval Patterns

One drawback of the tool's current architecture is that all input data,
[Layer Definitions](#layer-definitions) and [Loss Set
Data](#loss-set-data), must be loaded into memory all at once using Pandas
DataFrames. That is, all data is retrieved at once and the tool break the
data up into individual entities. This architecture limits the scalability
of the tool and depending on available memory likely restricts the number
of layers and associated loss sets that can be uploaded in a single batch.

To change the data retrieval patterns used by the tool, large parts of its
architecture will likely have to be refactored. Specifically, there needs
to be relationship introduced between the uploader and retriever modules in
order for the uploader to retrieve data on demand. If Pandas DataFrames are
continued to be used as internal data structures, even if only to represent
a single layer or loss set, much functionality provided by the extractor
and the uploader modules can be preserved. In that case, the refactoring
should be isolated to orchestration and interface signature, but not
require significant change to the "business logic".

## Known Issues

- Metadata columns that should be `int` are read and stored as `float`.
- Multiple loss sets per layer are not yet supported.
