# -*- coding: utf-8 -*-
# PJ: interesting_fact_code
# Created on: 2020-06-08
# Author: Junjie Xing Github: @GavinXing

# Copyright 2017 Amazon.com, Inc. or its affiliates

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

# http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import boto3
import botocore
from botocore.exceptions import ParamValidationError, ClientError
import argparse
from HIT_generator import xml_generation
import logging
import re
import json
from datetime import datetime
from collections import Counter

# Before connecting to MTurk, set up your AWS account and IAM settings as
# described here:
# https://blog.mturk.com/how-to-use-iam-to-control-api-access-to-your-mturk-account-76fe2c2e66e2
#
# Follow AWS best practices for setting up credentials here:
# http://boto3.readthedocs.io/en/latest/guide/configuration.html

# Use the Amazon Mechanical Turk Sandbox to publish test Human Intelligence
# Tasks (HITs) without paying any money.  Sign up for a Sandbox account at
# https://requestersandbox.mturk.com/ with the same credentials as your main
# MTurk account.

# By default, HITs are created in the free-to-use Sandbox
logging.basicConfig(level=logging.INFO)
create_hits_in_live = False

region_name = 'us-east-1'
aws_access_key_id = 'aws_access_key_id'
aws_secret_access_key = 'aws_secret_access_key'

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

# use profile if one was passed as an arg, otherwise
# profile_name = sys.argv[1] if len(sys.argv) >= 2 else None
# session = boto3.Session(profile_name=profile_name)


# Test that you can connect to the API by checking your account balance
# user_balance = client.get_account_balance()

# In Sandbox this always returns $10,000. In live, it will be your acutal balance.
# print("Your account balance is {}".format(user_balance['AvailableBalance']))

# The question we ask the workers is contained in this file.
# question_sample = open("tmp/celrnbtlrwjbguvc.xml", "r").read()

# Example of using qualification to restrict responses to Workers who have had
# at least 80% of their assignments approved. See:
# http://docs.aws.amazon.com/AWSMechTurk/latest/AWSMturkAPI/ApiReference_QualificationRequirementDataStructureArticle.html#ApiReference_QualificationType-IDs
worker_requirements = [{
    'QualificationTypeId': '000000000000000000L0',
    'Comparator': 'GreaterThanOrEqualTo',
    'IntegerValues': [80],
    'RequiredToPreview': True,
}]

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)


def get_client(live):
    mturk_environment = environments["live"] if live else environments["sandbox"]
    client = boto3.client(
        service_name='mturk',
        region_name='us-east-1',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=mturk_environment['endpoint'],
    )
    return client


def create_hit_type(live=False):
    client = get_client(live)
    mturk_environment = environments["live"] if live else environments["sandbox"]

    response = client.create_hit_type(
        AssignmentDurationInSeconds=600,
        Reward=mturk_environment['reward'],
        Title='TITLE',
        Keywords='KEYWORDS',
        Description='DESCRIPTION'
    )

    print(response['HITTypeId'])


def create_hit_with_type(hit_data, live=False):
    client = get_client(live)
    mturk_environment = environments["live"] if live else environments["sandbox"]
    file_path = xml_generation(hit_data, trim=False)
    if file_path:
        question = open(file_path, "r").read()
        try:
            response = client.create_hit_with_hit_type(
                HITTypeId=mturk_environment["hit_type"],
                Question=question,
                LifetimeInSeconds=24 * 60 * 60 * 7,  # 7 days
                MaxAssignments=1
            )
        except ParamValidationError as error:
            logging.error(error)
            logging.info("Try regenerating XML with trimmed data.")
            file_path = xml_generation(hit_data, trim=True)
            question = open(file_path, "r").read()
            response = client.create_hit_with_hit_type(
                HITTypeId=mturk_environment["hit_type"],
                Question=question,
                LifetimeInSeconds=24 * 60 * 60 * 7,  # 7 days
                MaxAssignments=1
            )
        except ClientError as error:
            logging.error("ClientError!")
            logging.error(error)
            logging.info("Try regenerating XML with trimmed data.")
            file_path = xml_generation(hit_data, trim=True)
            question = open(file_path, "r").read()
            response = client.create_hit_with_hit_type(
                HITTypeId=mturk_environment["hit_type"],
                Question=question,
                LifetimeInSeconds=24 * 60 * 60 * 7,  # 7 days
                MaxAssignments=1
            )
        except Exception as e:
            raise e
        logging.info(mturk_environment['preview'] +
                     "?groupId={}".format(response["HIT"]["HITGroupId"]))
        return response["HIT"]["HITId"], response["HIT"]["HITGroupId"]
    return False


def create_hit(hit_data, live=False):
    client = get_client(live)
    mturk_environment = environments["live"] if live else environments["sandbox"]
    file_path = xml_generation(hit_data)
    if file_path:
        question = open(file_path, "r").read()
        # Create the HIT
        response = client.create_hit(
            MaxAssignments=3,
            LifetimeInSeconds=600,  # TODO what lifetime mean
            AssignmentDurationInSeconds=600,
            Reward=mturk_environment['reward'],
            Title='TITLE',
            Keywords='KEYWORDS',
            Description='DESCRIPTION',
            Question=question,
            QualificationRequirements=worker_requirements,
        )

        logging.info("Now in {} mode:".format("live" if live else "sandbox"))
        # The response included several fields that will be helpful later
        hit_type_id = response['HIT']['HITTypeId']
        hit_id = response['HIT']['HITId']
        print("\nCreated HIT: {}".format(hit_id))

        print("\nYou can work the HIT here:")
        print(mturk_environment['preview'] + "?groupId={}".format(hit_type_id))

        print("\nAnd see results here:")
        print(mturk_environment['manage'])
        return response['HIT']
    return False


def get_balance(live=True):
    client = get_client(live)
    user_balance = client.get_account_balance()
    print("Now in {} mode:".format("live" if live else "sandbox"))
    print("Your account balance is {}".format(
        user_balance['AvailableBalance']))


def get_hit(HitID, live=False):
    client = get_client(live)
    respoense = client.get_hit(
        HITId=HitID
    )
    print(respoense)


def process_ans(text):
    try:
        x = re.search("<FreeText>\[(.*)\]</FreeText>", text)
        ans = json.loads(x.group(1))
        return ans
    except Exception as e:
        logging.error(e)
        return None


def get_hit_ans(HitID, live=False):
    client = get_client(live)
    response = client.get_hit(
        HITId=HitID
    )
    # print(response)
    try:
        if response["HIT"]["HITStatus"] in ["Reviewable", "Reviewing"]:
            res = client.list_assignments_for_hit(
                HITId=HitID
            )
            if res["Assignments"][0]["AssignmentStatus"] in ["Submitted", "Approved"]:
                return process_ans(res["Assignments"][0]["Answer"])
    except Exception as e:
        logging.error(e)
        logging.error(response)
        return None
    return None


def get_hit_ans_print(HitID, live=False):
    client = get_client(live)
    response = client.get_hit(
        HITId=HitID
    )
    # print(response)
    try:
        if response["HIT"]["HITStatus"] in ["Reviewable", "Reviewing"]:
            res = client.list_assignments_for_hit(
                HITId=HitID
            )
            if res["Assignments"][0]["AssignmentStatus"] in ["Submitted", "Approved"]:
                print(process_ans(res["Assignments"][0]["Answer"]))
        else:
            print(response["HIT"]["HITStatus"])
    except Exception as e:
        logging.error(e)
        logging.error(response)
        return None


def get_hit_status_for_a_group(groupID, live=False):
    client = get_client(live)
    MaxResult = 100
    res = client.list_hits(MaxResults=MaxResult)
    hits = []
    logging.info("Loading HITs...")
    cnt = Counter()
    while "NextToken" in res.keys():
        hits.extend(res["HITs"])
        nexttoken = res["NextToken"]
        res = client.list_hits(NextToken=nexttoken, MaxResults=MaxResult)
    hits.extend(res["HITs"])
    logging.info("{} HITs loaded.".format(len(hits)))
    hits = [hit for hit in hits if hit["HITGroupId"] == groupID]
    for hit in hits:
        if hit["HITStatus"] != "Reviewable":
            print(hit["HITId"], hit["HITStatus"])
        cnt[hit["HITStatus"]] += 1
    print(cnt)


def delete_all_hits(HITStatus="", live=False):
    client = get_client(live)
    MaxResult = 100
    res = client.list_hits(MaxResults=MaxResult)
    hits = []
    logging.info("Loading HITs...")
    while "NextToken" in res.keys():
        hits.extend(res["HITs"])
        nexttoken = res["NextToken"]
        res = client.list_hits(NextToken=nexttoken, MaxResults=MaxResult)
        logging.info("{} HITs loaded.".format(len(hits)))
    hits.extend(res["HITs"])
    if HITStatus:
        hits = [hit for hit in hits if hit["HITStatus"] == HITStatus]
    for hit in hits:
        if hit["HITStatus"] == "Assignable":
            client.update_expiration_for_hit(
                HITId=hit["HITId"],
                ExpireAt=datetime(2015, 1, 1)
            )
        try:
            client.delete_hit(HITId=hit["HITId"])
        except Exception as e:
            logging.error(e)
            logging.info('Not deleted')
        else:
            logging.info('Deleted')


def main(args):
    if args.function == "balance":
        get_balance(args.live)
    elif args.function == "create_hit_type":
        create_hit_type(args.live)
    elif args.function == "delete_all":
        delete_all_hits(live=args.live)
    elif args.function == "get_ans":
        assert args.HITId != ""
        get_hit_ans_print(args.HITId)
    elif args.function == "get_hit_status_for_a_group":
        assert args.groupId != ""
        get_hit_status_for_a_group(args.groupId, live=args.live)
    elif args.function == "create_hit":
        client = get_client(args.live)
        mturk_environment = environments["live"] if args.live else environments["sandbox"]
        question = open(
            "section5-screenshot.html",
            "r").read()
        response = client.create_hit(
            MaxAssignments=3,
            LifetimeInSeconds=600,
            AssignmentDurationInSeconds=600,
            Reward=mturk_environment['reward'],
            Title='Interestingness of Database Facts',
            Keywords='interestingness, database, fact, machine learning',
            Description='Annotate the interestingness comparison between two facts extracted from a database',
            Question=question,
            QualificationRequirements=worker_requirements,
        )

        logging.info("Now in {} mode:".format(
            "live" if args.live else "sandbox"))
        # The response included several fields that will be helpful later
        hit_type_id = response['HIT']['HITTypeId']
        hit_id = response['HIT']['HITId']
        print("\nCreated HIT: {}".format(hit_id))

        print("\nYou can work the HIT here:")
        print(mturk_environment['preview'] + "?groupId={}".format(hit_type_id))

        print("\nAnd see results here:")
        print(mturk_environment['manage'])
        return response['HIT']
    else:
        raise NotImplementedError


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MTurk Functions')
    parser.add_argument('function', type=str,
                        choices=['balance', 'create_hit', 'create_hit_type', "delete_all", "get_ans",
                                 "get_hit_status_for_a_group"])
    parser.add_argument('--live', action="store_true")
    parser.add_argument('--HITId', type=str, default="")
    parser.add_argument('--groupId', type=str, default="")
    args = parser.parse_args()
    main(args)
