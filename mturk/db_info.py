# -*- coding: utf-8 -*-
# PJ: interesting_fact_code
# Created on: 7/22/20
# Author: Junjie Xing Github: @GavinXing

# sample db_info

db_info = {
    "austin_crime": {
        "name": "austin_crime",
        "schema": [("unique_key", "INTEGER", "Unique identifier for the record."),
                   ("address", "STRING", "Full address where the incident occurred."),
                   ("census_tract", "FLOAT", ""),
                   ("clearance_date", "TIMESTAMP", ""),
                   ("clearance_status", "STRING", ""),
                   ("council_district_code", "INTEGER",
                    "Indicates the council district code where the incident occurred."),
                   ("description", "STRING",
                    "The subcategory of the primary description."),
                   ("district", "STRING",
                    "Indicates the police district where the incident occurred."),
                   ("latitude", "FLOAT", ""),
                   ("longitude", "FLOAT", ""),
                   ("location", "STRING", ""),
                   ("location_description", "STRING",
                    "Description of the location where the incident occurred."),
                   ("primary_type", "STRING",
                    "The primary description of the NIBRS/UCR code."),
                   ("timestamp", "TIMESTAMP",
                    "Time when the incident occurred. This is sometimes a best estimate."),
                   ("x_coordinate", "INTEGER",
                    "The x coordinate of the location where the incident occurred"),
                   ("y_coordinate", "INTEGER",
                    "The y coordinate of the location where the incident occurred"),
                   ("year", "INTEGER",
                    "Indicates the year in which the incident occurred."),
                   ("zipcode", "STRING", "Indicates the zipcode where the incident occurred.")]
    }
}
