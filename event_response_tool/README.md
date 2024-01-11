# Event Response Tool

## Overview

The Event Response Tool aids disaster response analysis by supporting two methods, viz., Mixture Distribution method and Convolution method.

### Methodologies

#### Mixture Distribution method

The Mixture Distribution method aims to mimic the functionality of RMS Step Tool. In this method, the Simulation component of an existing Analysis Profile is updated with a weighted Simulation grid based on the individual weights of the events. All other components of the original Analysis Profile remains intact, and no modification is made to the structures (LayerViews) and Loss Sets.

#### Convolution method

In Convolution method, the user can create a new Analysis Profile with a dummy Event Catalog, and a dummy Simulation of N Trials or use an already created Analysis Profile (that was created using Convolution method) to update LayerViews and Loss Sets with new losses based on the indiviudal weights of the events. 

##### Analysis Profile created using Convolution method

The Analysis Profile created using Convolution method has the following components:

- **Event Catalog**: A dummy event catalog with one or more EventIDs starting from 1.

- **Simulation**: A simulation of N trials.

- **Loss Filters**: A set of loss filters representing every individual event in the event catalog.

- **Exchange Rates Profile**: If the default FX profile UUID is not configured in the `config/event_response_config.ini` file,
  the latest FX profile available on the server.

##### Transforming LayerViews and underlying ELTs using Convolution method

When the user provide a CSV containing a list of LayerViews or a PortfolioView UUID to be transformed, the tool performs the following operations:

- Iterate the LayerViews and retrieve the underlying ELTs.
- Filter the ELTs based on the EventIDs in the event weights CSV file.
- Multiply the Mean, StdC, StdI and ExposureValue by the weight provided in the event weight CSV.
- Change the EventID for each row to a single EventID in the event catalog (the tool converts all EventIDs to EventID 1).
- Output a resultant CSV with a summary of old and new LayerView metrics.

## Setup

The Event Response tool uses [Poetry](https://python-poetry.org/) for
package and dependency management. Poetry can be easily installed
using either `pip` or `conda`.

Once Poetry is installed, the steps to setup the Event Response tool are as follows:

1. Clone or download the Git repository.

1. Change into the `event_response_tool` folder within the repository.

1. Have Poetry set up a virtual environment and install required
   dependencies:

   ```shell
   $ poetry install
   ```

## Usage

The Event Response tool supports Jupyter notebook and command-line interface at the moment.

#### Jupyter notebook interface

For using the notebook interface, the following command can be used to initialize the tool once the poetry setup is successful:

```shell
   $ poetry run jupyter notebook
```

Once the notebook server is launched, navigate to `Event Response Tool.ipynb` to launch the tool.

#### Command-line interface

After setting up Poetry, the Event Response tool can be invoked from CLI by running the Python program `event_response.py`. Below are some of the examples for both the methodologies.

**Mixture Distribution** 

For updating an existing Analysis Profile with a new weighted Simulation grid, the command might look like this:

```shell
poetry run python event_response.py --url https://client-api.analyzere.net --username <username> --password <password> --method mixture_distribution --event_weights_csv sample_data/weightV2.csv --old_analysis_profile_uuid d64c21b5-18a0-4c05-ba34-5e114474c01c --max_trial_per_event 12
```

In the above command, parameters corresponding to Simualtion description and Analysis Profile description are not provided, so the default value in the config file will be used. 

**Convolution** 

To create a new Analysis Profile with dummy events, and dummy simulation of N trials, the commands could be configured like shown below.

```shell
poetry run python event_response.py --url https://client-api.analyzere.net --username <username> --password <password> --method convolution --new_analysis_profile --total_number_of_events 30 --trial_count 15 --analysis_profile_description test_convolution_ap
```

To update the LayerViews and LossSets with an already created Analysis Profile, the commands could be configured as,

```shell
poetry run python event_response.py --url https://client-api.analyzere.net --username <username> --password <password> --method convolution --no_new_analysis_profile --portfolio_view_uuid ffa44a3f-1ed1-eb74-a17b-0094cddde7c7 --analysis_profile_uuid_for_loss_update d0c263f1-34a5-4587-acd1-3644fbf376a8 --event_weights_csv sample_data/test_event_weights.csv
```
Note that we can also create a new Analysis Profile and use it for updating the LayerView/LossSet data by using `--new_analysis_profile` argument and then providing either `--portfolio_view_uuid` or `layer_views_csv` argument.

The complete list of arguments supported by command-line interface is as follows:
```shell
usage: event_response.py [-h] [--url URL] [--username USERNAME] [--password PASSWORD] [--event_weights_csv EVENT_WEIGHTS_CSV] --method {mixture_distribution,convolution}
                        [--old_analysis_profile_uuid OLD_ANALYSIS_PROFILE_UUID] [--max_trial_per_event MAX_TRIAL_PER_EVENT]
                        [--simulation_description_mixture_distribution SIMULATION_DESCRIPTION_MIXTURE_DISTRIBUTION]
                        [--analysis_profile_description_mixture_distribution ANALYSIS_PROFILE_DESCRIPTION_MIXTURE_DISTRIBUTION]
                        [--simulation_start_date_mixture_distribution SIMULATION_START_DATE_MIXTURE_DISTRIBUTION] [--new_analysis_profile] [--no_new_analysis_profile]
                        [--total_number_of_events TOTAL_NUMBER_OF_EVENTS] [--trial_count TRIAL_COUNT] [--catalog_description CATALOG_DESCRIPTION]
                        [--simulation_description SIMULATION_DESCRIPTION] [--analysis_profile_description ANALYSIS_PROFILE_DESCRIPTION]
                        [--simulation_start_date SIMULATION_START_DATE] [--layer_views_csv LAYER_VIEWS_CSV] [--portfolio_view_uuid PORTFOLIO_VIEW_UUID]
                        [--analysis_profile_uuid_for_loss_update ANALYSIS_PROFILE_UUID_FOR_LOSS_UPDATE]
```

## Config file
The tool also supports a configuration file `config/event_response_config.ini` where some of the optional arguments can be configured.

## Data Requirements

- The CSV containing the event weights (also known as the *Event Ensemble Table*) may look as follows:

  | Event ID | Weight |
  |-|-|
  | 1232142 | 0.25 |
  | 5634234 | 0.75 |
  | 4864843 | 0.5 |

- If a CSV containing a list of LayerViews to be transformed is used, it could follow the below format:
  | LayerViews |
  |-|
  | 12345abcd-6789-abcd-efgh-ijklmnopqr |
  | 67899abcd-8191-abcd-xyza-123lmnopqr |

- The tool can only operate on LayerViews and PortfolioViews at the moment.

- Only ELTs are supported.
