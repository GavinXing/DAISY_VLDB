# -*- coding: utf-8 -*-
# PJ: interesting_fact_code 
# Created on: 9/16/20
# Author: Junjie Xing Github: @GavinXing

from flask import Flask, send_from_directory

app = Flask(__name__, static_url_path='')


@app.route('/')
def root():
    return "Hello, World!"


@app.route('/images/<path:path>')
def send_image(path):
    return send_from_directory('images', path)


if __name__ == "__main__":
    app.run(port=5000)
