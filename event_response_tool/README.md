# Event Response Tool

## Overview

The purpose of the Event Response Tool is to provide RMS style disaster response analysis in PRIME platform. It is based on the idea that the RMS users are provided with a list of events and associated weights that are meant for constructing a weighted convolution of event distribution into a single new event. For each event, the mean, standard deviation and exposure value attributes are then multiplied by their respective weight and then aggregated into a single event.

To aid this type of analysis, the tool supports the following two functionalities:

**1. Creating and Updating specialized Event Response Analysis Profile**
**2. Transforming existing LayerViews and underlying ELTs**

#### Analysis Profile
The tool creates a specialized analysis profile that has the following components:

- **Event Catalog**: A dummy event catalog with one or more EventIDs starting from 1.

- **Simulation**: A simulation of size 'n' where n is the weighted sum of maximum trials per event.

- **Loss Filters**: A set of loss filters representing every individual event in the event catalog.

- **Exchange Rates Profile**: If the default FX profile UUID is not configured in the `config/event_response_config.ini` file,
the latest FX profile available on the server.

The tool also supports updating the simulation component of an existing event response analysis profile when the RMS users receive updated event weights. The user has to be provide the UUID of the existing analysis profile to the tool in order to use this feature.

#### Transforming LayerViews and underlying ELTs

When the user provide a CSV containing a list of LayerViews or a PortfolioView UUID with a set of LayerViews that are to be manipulated, the tool performs the following operations:

- Iterate the LayerViews and retrieve the underlying ELTs.
- Filter the ELTs based on the EventIDs in the event weights CSV file.  
- Multiply the Mean, StdC, StdI and ExposureValue by the weight provided by RMS.
- Change the EventID for each row to a single EventID in the event catalog.
- Output a resultant CSV with a summary of old and new LayerView metrics.

## Setup
The Event Response tool uses [Poetry](https://python-poetry.org/) for
package and dependency management. Poetry can be easily installed 
using either `pip` or `conda`.

Once Poetry is installed, the steps to setup the Event Response tool are as follows:
1. Clone or download the Git repository.
2. Change into the `event_response_tool` folder within the repository.
3. Have Poetry set up a virtual environment and install required
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
After setting up Poetry, the Event Response tool can be invoked from CLI by running the Python program `event_response.py`. For example,

```shell
   $ poetry run python event_response.py --event_weights_csv sample_data/test_event_weights.csv --portfolio_view_uuid 12345abcd-6789-abcd-efgh-ijklmnopqr
```
#### Input Arguments 
Depending on the type of operation to be performed, the tool may require different
arguments. The list of supported arguments are as follows:

- `analyzere_url`: Analyze Re URL
- `analyzere_username`: Analyze Re username
- `analyzere_password`: Analyze Re password
- `event_weights_csv`: The path of the CSV containing event weights **(required argument)**
- `layer_views_csv`: The path of the CSV containing list of layer_view UUIDs to be transformed
- `portfolio_view_uuid`: The UUID of the portfolio_view that contains the list of layer_view UUIDs to be transformed
- `total_number_of_events`: The total number of events to be created in the event catalog of event response analysis profile
- `trial_count`: The maximum number of trials per event
- `catalog_description`: The description of the new event catalog to be created
- `simulation_description`: - The description of the new simgrid to be created
- `analysis_profile_description`: The description of the new analysis profile to be created
- `old_analysis_profile_uuid`: The UUID of the analysis profile to be updated with a new simgrid

The above inputs are common to both the notebook and command-line interfaces. Note that each argument is to be prefixed with a `--` for the CLI.

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