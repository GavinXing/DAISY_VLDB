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
