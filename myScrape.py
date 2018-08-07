import json
import os
import meetup.api
import pandas as pd
import numpy as np

# Fetching Data from meet up APIs
# @ Ruosi Wang Jun 26th, 2018

'''
---------------
 set parameters
---------------
'''

API_KEY = 'XXXXXXXXXXXXXXXXXXXXXXXXXXX'
URL_STRING = "http://api.meetup.com//"
MIN_MEMBER_NUM = 50
MIN_RSVP_NUM = 10

# libarary
BOSTON_CSA_TOP20 = {'Boston': 'MA',
                    'Worcester': 'MA',
                    'Providence': 'RI',
                    'Manchester': 'NH',
                    'Lowell': 'MA',
                    'Cambridge': 'MA',
                    'New Bedford': 'MA',
                    'Brockton': 'MA',
                    'Quincy': 'MA',
                    'Lynn': 'MA',
                    'Fall River': 'MA',
                    'Newton': 'MA',
                    'Nashua': 'NH',
                    'Warwick': 'MA',
                    'Cranston': 'RI',
                    'Somerville': 'MA',
                    'Lawrence': 'MA',
                    'Pawtucket': 'RI',
                    'Framingham': 'MA',
                    'Waltham': 'MA'}
FETCH_PARS = {'groups': {'get_func': {'city': 'fetch', 'state': 'BOSTON_CSA_TOP20[fetch]', 'country': '"US"', 'offset': 'off_val'},
                         'response_list': ['id', 'members', 'name', 'urlname', 'who', 'visibility', 'join_mode', 'rating', 'country', 'state', 'city', 'lon', 'lat', 'timezone'],
                         # 'response_dict': {'member_id': 'organizer', 'id': 'category', 'name': 'category', 'shortname': 'category'},
                         'response_dict': ['organizer: member_id', 'category: id', 'category: name', 'category: shortname'],
                         'filter_dict': {'members': MIN_MEMBER_NUM},
                         'columns_rename': {}},
              'events': {'get_func': {'group_id': 'fetch', 'fields': '"event_hosts,rsvp_rules,rsvpable,trending_rank,venue_visibility"', 'status': '"past"', 'offset': 'off_val'},
                         'response_list': ['id', 'name', 'visibility', 'headcount', 'waitlist_count', 'yes_rsvp_count', 'maybe_rsvp_count', 'time', 'created', 'updated', 'duration'],
                         # 'response_dict': {'average': 'rating', 'count': 'rating', 'id': 'group', 'urlname': 'group'},
                         'response_dict': ['rating: average', 'rating: count', 'group: id', 'group: urlname', 'venue: zip', 'venue: city', 'venue: lon', 'venue: lat', 'venue: address_1', 'venue: id'],
                         'filter_dict': {'yes_rsvp_count': MIN_RSVP_NUM},
                         'columns_rename': {}},
              'venues': {'get_func': {'event_id': 'fetch', 'offset': 'off_val'},
                         'response_list': ['id', 'name', 'city', 'state', 'lon', 'lat', 'rating', 'rating_count', 'event_id'],
                         'response_dict': {},
                         'filter_dict': {},
                         'columns_rename': {}},
              'rsvps': {'get_func': {'event_id': 'fetch', 'offset': 'off_val'},
                        'response_list': ['rsvp_id', 'response', 'created', 'mtime', 'guests'],
                        'response_dict': {'member_id': 'member', 'id': 'event', 'urlname': 'group'},
                        'filter_dict': {},
                        'columns_rename': {'member_member_id': 'member_id'}},
              'members': {'get_func': {'member_id': 'fetch', 'offset': 'off_val'}}}

# set directories
cwd = os.getcwd()
# directory to save fatched data
fetch_dir = 'fetched'
# make saving directory if it doesn't exist
if not os.path.exists(fetch_dir):
    os.makedirs(fetch_dir)
# make meetup api object
client = meetup.api.Client(API_KEY, overlimit_wait=True)

'''
-------------
functionss
-------------
'''


# Fetch Data and save
def fetch_data(fetch_id, fetch_list, fetch_target):
    # prepare
    fetched_data = []
    input_pars = ', '.join([f'{key}={value}' for key, value in FETCH_PARS[fetch_target]['get_func'].items()])
    fetch_code = f'client.Get{fetch_target.capitalize()}({input_pars})'
    print(fetch_code)
    # fetch
    for c, fetch in enumerate(fetch_list):
        off_val, do_fetch = 0, 1
        # take a breath
        print(c)
        # fetching data
        while do_fetch:
            try:
                get = eval(fetch_code)
                if get.results is not None:
                    results_list = get.results
                    if (fetch_target is 'venues'):
                        try:
                            results_list[0].update({'event_id': fetch})
                        except Exception as e:
                            pass
                    fetched_data += results_list
                    off_val += 1
                    # print(len(get.results))
                    if len(results_list) < 200:
                        do_fetch = 0
                else:
                    do_fetch = 0
            except Exception as e:
                off_val += 1
                pass
        # save as json file
    f_name = os.path.join(cwd, fetch_dir, f'{fetch_target}_{fetch_id:03d}.json')
    with open(f_name, 'w') as f:
        json.dump(fetched_data, f, indent=2)
        print('finished saving')


# cut id list into chuncks when it is too long
def get_fetch_chunks(ids, chunk_num):
    fetch_totalN = len(ids)
    fetch_chunk = [ids[i * chunk_num: min((i + 1) * chunk_num, fetch_totalN)] for i in range((fetch_totalN - 1) // chunk_num + 1)]
    return fetch_chunk


# integrated data fetching
def run_fetch(fetch_target, fetch_ids, chunk_num):
    fetch_chunks = get_fetch_chunks(fetch_ids, chunk_num)
    # resume if interrupted
    resume = 0
    fetched_file_list = [file for file in os.listdir(fetch_dir) if file.startswith(fetch_target)]
    if fetched_file_list:
        resume = len(fetched_file_list)
        fetch_chunks = fetch_chunks[resume:]
    # fetch data
    for fetch_id, fetch_list in enumerate(fetch_chunks):
        print(f'fetched {fetch_id} out of {len(fetch_chunks)}')
        fetch_data(fetch_id + resume, fetch_list, fetch_target)


# combine saved files together
def combine_all_fecthed_data(fetch_target, unique_key):
    combined = []
    combined_dict = dict()
    fetched_file_list = [file for file in os.listdir(fetch_dir) if file.startswith(fetch_target)]
    for c, file in enumerate(fetched_file_list):
        print(c)
        f_name = os.path.join(cwd, fetch_dir, file)
        with open(f_name) as f_read:
            contents = json.load(f_read)
        combined += contents
        # unique answer
        content_dict = {item[unique_key]: item for item in contents}
        combined_dict.update(content_dict)
    with open(f'{fetch_target}.json', 'w') as f_write:
        json.dump(list(combined_dict.values()), f_write, indent=2)
    with open(f'{fetch_target}_2.json', 'w') as f_write:
        json.dump(combined, f_write, indent=2)


# create pandas DataFrame from json files
def create_df_from_json(fetch_target):
    response_list = FETCH_PARS[fetch_target]['response_list']
    response_dict = FETCH_PARS[fetch_target]['response_dict']
    filter_dict = FETCH_PARS[fetch_target]['filter_dict']
    columns_mapper = FETCH_PARS[fetch_target]['columns_rename']

    # initialize dict object
    columns = response_list + [item.replace(': ', '_') for item in response_dict]
    dicts = {column: [] for column in columns}
    # read json file
    f_name = os.path.join(cwd, f'{fetch_target}.json')
    with open(f_name) as f:
        items = json.load(f)

        # transform json object to dict
        for c, item in enumerate(items):
            # list items
            for rsp in response_list:
                if rsp in item.keys():
                    dicts[rsp].append(item[rsp])
                else:
                    dicts[rsp].append(np.nan)
            # dict items
            for rsp in response_dict:
                key, value = rsp.split(': ')
                rsp = rsp.replace(': ', '_')
                if (key in item.keys()) & (value in item[key].keys()):
                    dicts[rsp].append(item[key][value])
                else:
                    dicts[rsp].append(None)
            print(f'finished {c} out of {len(items)}')

    # create dataframe from dict object
    df = pd.DataFrame.from_dict(dicts, orient='columns')
    if len(filter_dict):
        for key, value in filter_dict.items():
            df = df.loc[df[key] > value, :]
    # rename columns
    df = df.rename(columns=columns_mapper)
    return df


def create_df_from_parsed_json(fetch_target):
    response_list = FETCH_PARS[fetch_target]['response_list']
    response_dict = FETCH_PARS[fetch_target]['response_dict']
    filter_dict = FETCH_PARS[fetch_target]['filter_dict']
    columns_mapper = FETCH_PARS[fetch_target]['columns_rename']

    # initialize dict object
    # columns = response_list + [f'{value}_{key}' for key, value in response_dict.items()]
    columns = response_list + [item.replace(': ', '_') for item in response_dict]
    dicts = {column: [] for column in columns}
    # read json file
    fetched_file_list = [file for file in os.listdir(fetch_dir) if file.startswith(fetch_target)]
    for c, file in enumerate(fetched_file_list):
        print(c)
        f_name = os.path.join(cwd, fetch_dir, file)
        with open(f_name) as f_read:
            contents = json.load(f_read)
        # transform json object to dict
        for item in contents:
            # list items
            for rsp in response_list:
                if rsp in item.keys():
                    dicts[rsp].append(item[rsp])
                else:
                    dicts[rsp].append(np.nan)
            # dict items
            for rsp in response_dict:
                key, value = rsp.split(': ')
                rsp = rsp.replace(': ', '_')
                if key in item.keys():
                    if value in item[key].keys():
                        dicts[rsp].append(item[key][value])
                    else:
                        dicts[rsp].append(None)
                else:
                    dicts[rsp].append(None)
            # for key, value in response_dict.items():
            #     if value in item.keys():
            #         dicts[f'{value}_{key}'].append(item[value][key])
            #     else:
            #         dicts[f'{value}_{key}'].append('nan')
    # create dataframe from dict object
    df = pd.DataFrame.from_dict(dicts, orient='columns')
    if len(filter_dict):
        for key, value in filter_dict.items():
            df = df.loc[df[key] > value, :]
    # rename columns
    df = df.rename(columns=columns_mapper)
    return df

# for json files that are too big to read
def create_mapping_df_from_parsed_json(fetch_target, primary_key, foreign_key):
    # mapping between two identifications
    filter_dict = FETCH_PARS[fetch_target]['filter_dict']
    primary_key_name = list(primary_key.keys())[0]
    primary_key_content = list(primary_key.values())[0]
    columns = [foreign_key, primary_key_name] + list(filter_dict.keys())
    dicts = {column: [] for column in columns}
    # read json file
    fetched_file_list = [file for file in os.listdir(fetch_dir) if file.startswith(fetch_target)]
    for c, file in enumerate(fetched_file_list):
        print(c)
        f_name = os.path.join(cwd, fetch_dir, file)
        with open(f_name) as f_read:
            contents = json.load(f_read)
        # transform json object to dict
        for item in contents:
            if primary_key_name in list(item.keys()):
                for subitem in item[primary_key_name]:
                    dicts[primary_key_name].append(subitem[primary_key_content])
                    dicts[foreign_key].append(item[foreign_key])
                    for filter_key in list(filter_dict.keys()):
                        dicts[filter_key].append(item[filter_key])
            else:
                dicts[primary_key_name].append(None)
                dicts[foreign_key].append(item[foreign_key])
                for filter_key in list(filter_dict.keys()):
                    dicts[filter_key].append(item[filter_key])
            # list items
    # create dataframe from dict object
    df = pd.DataFrame.from_dict(dicts, orient='columns')
    if len(filter_dict):
        for key, value in filter_dict.items():
            df = df.loc[df[key] > value, :]
    # rename columns
    # df = df.rename(columns=columns_mapper)
    return df


'''
---------------
fetching groups
---------------
'''
fetch_target = 'groups'
cities = list(BOSTON_CSA_TOP20.keys())
run_fetch(fetch_target, cities, 1)
combine_all_fecthed_data(fetch_target, 'id')
df_groups = create_df_from_json(fetch_target)
df_groups.to_pickle('df_groups')


'''
---------------
fetching events
---------------
'''
fetch_target = 'events'
df_groups = pd.read_pickle('df_groups')
groups_ids = list(df_groups.id)
run_fetch(fetch_target, groups_ids, 50)
combine_all_fecthed_data(fetch_target, 'id')
df_events = create_df_from_json(fetch_target)
df_events.to_pickle('df_events')
events_ids = list(df_events.id)

'''
---------------
fetching rsvps
---------------
'''
fetch_target = 'rsvps'
df_events = pd.read_pickle('df_events')
events_ids = list(df_events.id)
run_fetch(fetch_target, events_ids, 1000)
combine_all_fecthed_data(fetch_target, 'rsvp_id')
df_rsvps = create_df_from_json(fetch_target)
df_rsvps.to_pickle('df_rsvps')
rsvps_ids = list(df_rsvps.rsvp_id)


'''
---------------
fetching venues
---------------
'''
fetch_target = 'venues'
df_events = pd.read_pickle('df_events')
events_ids = list(df_events.id)
run_fetch(fetch_target, events_ids, 1000)
combine_all_fecthed_data(fetch_target, 'event_id')
df_venues = create_df_from_json(fetch_target)
# df_venues = gen_df('venues')

'''
----------------
fetching members
----------------
'''
fetch_target = 'members'
df_rsvps = pd.read_pickle('df_rsvps')
member_ids = df_rsvps.member_id.unique()
run_fetch('members', member_ids, 1000)
# df_venues = gen_df('member')


'''
------------------------
extract event hosts info
------------------------
'''
fetch_target = 'events'
primary_key = {'event_hosts': 'member_id'}
foreign_key = 'id'
df_event_hosts = create_mapping_df_from_parsed_json(fetch_target, primary_key, foreign_key)
pd.to_pickle(df_event_hosts, 'df_event_hosts')


'''
------------------------
extract group topics info
------------------------
'''
fetch_target = 'groups'
primary_key = {'topics': 'urlkey'}
foreign_key = 'id'
df_event_hosts = create_mapping_df_from_parsed_json(fetch_target, primary_key, foreign_key)
pd.to_pickle(df_event_hosts, 'df_group_topics')
