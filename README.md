# Radar Timeline Data

This scripts goal is to import transplant and treatment timeline data from various databases in the aim of enriching 
data currently within RADAR all the while adhering to validation levels between databases
[![Contributors][contributors-shield]][contributors-url]
## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites

The things you need before setting up the script locally.

* Env file to access keypass via rr connection manager 


### Installation

A step by step guide that will tell you how to get the development environment up and running.

```
$ First step
$ Another step
$ Final step
```

## Usage

below are commands that can be chained together or used separately

```
Audit at directory
$ python radar_timeline_data/main.py -a /path/to/directory

Commit changes to server
$ python radar_timeline_data/main.py -c

Run on staging servers
$ python radar_timeline_data/main.py -tr

Commit with Audit at directory
$ python radar_timeline_data/main.py -c -a /path/to/directory
```

## Deployment

Additional notes on how to deploy this on a live or release system. Explaining the most important branches, what pipelines they trigger and how to update the database (if anything special).

## Additional Documentation and Acknowledgments

