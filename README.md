# DAISY_VLDB

This is the code base for our paper "Data-Driven Insight Synthesis for Multi-Dimensional Data" accepted for publication in VLDB 2024.

## Environment

```bash
python3 -m venv venv
pip3 install -r requirements.py
source venv/bin/activate
```

## Label Insights (Section 2)

Following are the steps to run the automatic and interactive labeling process with [MTurk](https://www.mturk.com).

### Dataset Processing

1. Please [install postgresql](https://www.postgresql.org/download/) on your local machine.

2. Load the table into postgres. See sample SQL file at `dataset_processing/create_table.sql`.

3. Run all the queries given a domain sepecific language (DSL), generate the clusters, and generate visualizations. Please make sure to edit the `.py` files for several variables.

```bash
cd dataset_processing
python3 generateAllQueries.py
python3 getClusters.py
python3 generateViz.py <db_name> <input_file> <output_file> <viz_folder>
```

4. Serve the flask app for visualizations. By default it will be served on `localhost:5000`. Please use a desired reverse proxy server to redirect to requests. You will need a server with a public ip to be able to load visualizations on mturk.

```bash
cd image_server
./serve.sh
```

### MTurk Setup

<span style="color:red;">! Disclaimer: </span> please be aware that there can be potential unexpected charge from MTurk if you don't use the following code discreetly. Make sure that you understand the basic MTurk concepts and workflow before you proceed.

1. Edit `mturk/config.py`, `mturk/db_config.py`, and `mturk/db_info.py` with your own settings.

2. Edit `line 48-50 in mturk/mturk_api.py` with your own MTurk credentials. 

```bash
region_name = 'us-east-1'
aws_access_key_id = 'aws_access_key_id'
aws_secret_access_key = 'aws_secret_access_key'
```

3. Create your HIT type and obtain the HIT ID. Edit `line 109-121 in mturk/mturk_api.py` first, then:

```
python3 mturk_api.py create_hit_type
```

4. Edit `line 52-67 in mturk/mturk_api.py` with the HIT ID returned by step 3.

```json
environments = {
    "live": {
        "endpoint": "https://mturk-requester.us-east-1.amazonaws.com",
        "preview": "https://www.mturk.com/mturk/preview",
        "manage": "https://requester.mturk.com/mturk/manageHITs",
        "reward": "0.01",
        "hit_type": "HIT_TYPE_ID"  # production HIT type
    },
    "sandbox": {
        "endpoint": "https://mturk-requester-sandbox.us-east-1.amazonaws.com",
        "preview": "https://workersandbox.mturk.com/mturk/preview",
        "manage": "https://requestersandbox.mturk.com/mturk/manageHITs",
        "reward": "0.11",
        "hit_type": "HIT_TYPE_ID"
    },
}
```

5. Edit `mturk/HIT_generator.py` with your desired HIT template. The current file provides a example.

6. Run the MTurk annotation framework:

```
python3 mturk.py run
```