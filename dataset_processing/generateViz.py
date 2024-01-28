# -*- coding: utf-8 -*-
# PJ: interesting_fact_code
# Author: Junjie Xing Github: @GavinXing

import pickle
import uuid
from os import path, makedirs
import datetime
import argparse

import matplotlib.pyplot as plt


def get_viz(data, db_name, root_folder):
    names = [x for (x, _) in data]
    if isinstance(names[0], datetime.datetime):
        names = [x.strftime("%Y-%m-%d %H:%M:%S") for x in names]
    nums = [y for (_, y) in data]
    plt.pie(nums, labels=names, autopct='%0.f%%', shadow=False, startangle=90)
    # print(nums)
    # plt.show()
    f_name = "{}/{}.png".format(db_name, uuid.uuid4().hex)
    f_path = path.join(root_folder, f_name)
    plt.savefig(f_path, bbox_inches='tight')
    plt.clf()
    return f_name


def main(args):
    print("Visualization folder:", args.viz_folder)
    with open(args.input_file, "rb") as infile:
        data = pickle.load(infile)
    # create db viz folder
    if not path.exists(path.join(args.viz_folder, args.db_name)):
        makedirs(path.join(args.viz_folder, args.db_name))

    new_data = []
    for cluster in data:
        new_cluster = []
        for fact in cluster:
            if data:
                f_path = get_viz(data, fact[0], args.viz_folder)
                fact[-2] = data
                fact[-1] = f_path
                new_cluster.append(fact)
        if new_cluster:
            new_data.append(new_cluster)

    with open(args.output_file, "wb") as outfile:
        pickle.dump(new_data, outfile)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("db_name", type=str, help="database name")
    parser.add_argument("input_file", help="path to input file")
    parser.add_argument("output_file", help="path to output file")
    parser.add_argument("viz_folder", help="path to visualization folder")
    args = parser.parse_args()
    main(args)
