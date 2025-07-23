# Radar Timeline Data

This scripts goal is to import transplant and treatment timeline data from various databases in the aim of enriching 
data currently within RADAR all the while adhering to validation levels between databases


## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites

The things you need before setting up the script locally.

* Env file to access keypass via rr connection manager 


### Installation

this project is written using uv for dep management see https://docs.astral.sh/uv/ for more info

``` shell
uv sync
```

## Usage

below are commands that can be chained together or used separately

``` text
Usage: timeline [OPTIONS]

  TimeLineData importer script

Options:
  -ap, --audit_path DIRECTORY  Directory to store the audit files  [required]
  -c, --commit                 Commit to server
  -tr, --test_run              Run on staging servers
  --help                       Show this message and exit.

```
### 📦 Running via uv
Show help using:
```shell
uv run timeline --help
```
Or, run the script directly:
```shell
uv run python .\scripts\main.py --help
```
## Deployment

Additional notes on how to deploy this on a live or release system. Explaining the most important branches, what pipelines they trigger and how to update the database (if anything special).



