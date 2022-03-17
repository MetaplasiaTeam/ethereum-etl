# MIT License
#
# Copyright (c) 2020 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import json
import boto3

class S3ItemExporter:

  def __init__(self, bucket = 'ethereum-etl', prefix = ''):
    self.bucket = bucket
    self.s3 = boto3.resource('s3')

  def open(self):
    pass

  def export_items(self, items):
    for item in items:
      self.export_item(item)

  def export_item(self, item):
    item_type = item.get('type')
    if item_type is None:
      raise ValueError('type key is not found in item {}'.format(repr(item)))
    
    item_id = item.get('item_id')
    if item_id is None:
      raise ValueError('item_id key is not found in item {}'.format(repr(item)))
    
    key = prefix_by_item_type(item_type) + '/' + item_id + '.json'
    self.s3.Bucket(self.bucket).put_object(Key=key, Body=json.dumps(item))

  def close(self):
    pass

def prefix_by_item_type(item_type):
  return item_type