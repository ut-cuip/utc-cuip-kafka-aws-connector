# Kafka AWS Utils

## Overview

The tools in this repository are designed to assist with migration and offloading of data from Kafka. The primary tool (`run.py`) should in theory be runnable as a CRON job (after an initial startup period during which it catches up on old data), as to save CPU consumption.

## Dependencies

This dependency list may grow to be out of date, so see `Pipfile` for an accurate display of dependencies. As of last modification, dependencies for this project include:

* Python 3.6.5 with the following libraries:
  * elasticsearch
  * pandas
  * tqdm
  * boto3
  * confluent-kafka
  * pyyaml

***You should probably be using PyEnv and Pipenv for this*** like we are. If you're not, feel free to use Python 3.6.5 and whatever package manager you wish, but if you do use PyEnv and Pipenv together this project will practically build and run itself. 

## Running

So, for the sake of those who may use this tool in the future once I'm no longer the primary developer for this, I have thoroughly documented in the CLI how to use this tool It's pretty straight forward:

1. Modify `config.yaml` to include the actual Kafka `bootstrap.servers`, `topics`, and `group-id` that you want to use.

2. **Make sure** that `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` are in your current environment; on Unix-based CLI's you can enable this via `export AWS_ACCESS_KEY_ID="<client key>"` and `export AWS_SECRET_ACCESS_KEY="<client secret>"` . 

   If you haven't done this and try to run the application, I've designed it to complain at you instead of letting `boto3` catch the issue and maybe crash with some obscure log to tell you what I just told you above.

3. Install the dependencies however you want - if you use PyEnv and Pipenv then it'll be easy (`pipenv install --dev` if you're going to develop, `pipenv install` otherwise)

4. Run using Python3 / Pipenv (`<pipenv run >python run.py`). If you want to specify how many workers to use, use the `-n=#` or `--num-workers=#` flag - if it's not in this format it'll also complain with details as to why it doesn't work. This value defaults to the number of threads your system has (for example, my 9th-gen Core i7 has 12 threads [it's 6-core HyperThreaded]) so the default is 12. It also outputs whatever you end up using to assure you you've done it right.

   Additionally you can use the `-h`, `--help` or `?` flag to get a help menu but it'll just tell you what I just did.

5. Sit back and let it run! Again, I'd suggest a CRON job once your historic data has all been migrated; it doesn't need to run *all the time* and should be fast enough as a CRON task to finish in ~1hr.