from cloudburst.client.ephe_common import *

def ad_event_preprocess(cloudburst, event_key):
    # parse event string
    # filter out inrelevent events before writing to trigger-cache
    pass

def ad_projection(cloudburst, *trigger_keys):
    # make projection
    pass

def ad_argument(cloudburst, *trigger_keys):
    # insert compaign ids
    pass

def map_ad_count(cloudburst, *trigger_keys):
    # count the number of ad per compaign
    pass

def reduce_ad_count(cloudburst, *trigger_keys):
    # collect results from map
    pass

"""
Create bucket and add triggers for coordination.

coord_addr = ''
client = CoordClient(coord_addr, None)

client.create_bucket('projection', SESSION)
client.create_bucket('argument', SESSION)
client.create_bucket('map_in_batch', NORMAL)
client.create_bucket('reduce_for_batch', NORMAL)

client.add_trigger('projection', 't1', UPON_WRITE, {'function': 'projection'})
client.add_trigger('argument', 't2', UPON_WRITE, {'function': 'argument'})
# TODO ADD
client.add_trigger('reduce_for_batch', 't3', BY_DYNAMIC_SET, {'function': 'reduce_ad_count'}) # or BY_DYNAMIC_COUNT?
client.add_trigger('map_in_batch', 't4', BY_PERIOD, {'function': 'map_ad_count', 'handler': 't3', 'period': 2, 'batch_size': 100})

"""