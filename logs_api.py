#!/usr/bin/env python
# -*- coding: utf-8 -*-
import requests
import urllib
import json
import utils
import clickhouse
import datetime
import logging
import parsing_params


logger = logging.getLogger('logs_api')

HOST = 'https://api-metrika.yandex.ru'


def get_estimation(user_request):
    '''Returns estimation of Logs API (whether it's possible to load data and max period in days)'''
    url_params = urllib.urlencode(
        [
            ('date1', user_request.start_date_str),
            ('date2', user_request.end_date_str),
            ('source', user_request.source),
            ('fields', ','.join(user_request.fields)),
            ('oauth_token', user_request.token)
        ]
    )

    url = '{host}/management/v1/counter/{counter_id}/logrequests/evaluate?'\
        .format(host=HOST, counter_id=user_request.counter_id) + url_params

    r = requests.get(url)
    logger.debug(r.text)
    if r.status_code == 200:
        return json.loads(r.text)['log_request_evaluation']
    else:
        raise ValueError(r.text)


def get_api_requests(user_request):
    '''Returns list of API requests for UserRequest'''
    api_requests = []
    estimation = get_estimation(user_request)
    if estimation['possible']:
        api_request = utils.Structure(
            user_request=user_request,
            date1_str=user_request.start_date_str,
            date2_str=user_request.end_date_str,
            status='new'
        )
        api_requests.append(api_request)
    else:
        start_date = datetime.datetime.strptime(
            user_request.start_date_str,
            utils.DATE_FORMAT
        )

        end_date = datetime.datetime.strptime(
            user_request.end_date_str,
            utils.DATE_FORMAT
        )

        days = (end_date - start_date).days
        num_requests = int(days/estimation['max_possible_day_quantity']) + 1
        days_in_period = int(days/num_requests) + 1
        for i in range(num_requests):
            date1 = start_date + datetime.timedelta(i*days_in_period)
            date2 = min(
                end_date,
                start_date + datetime.timedelta((i+1)*days_in_period - 1)
            )

            api_request = utils.Structure(
                user_request=user_request,
                date1_str=date1.strftime(utils.DATE_FORMAT),
                date2_str=date2.strftime(utils.DATE_FORMAT),
                status='new'
            )
            api_requests.append(api_request)

    return api_requests


def create_task(api_request):
    '''Creates a Logs API task to generate data'''
    url_params = urllib.urlencode(
        [
            ('date1', api_request.date1_str),
            ('date2', api_request.date2_str),
            ('source', api_request.user_request.source),
            ('fields', ','.join(sorted(api_request.user_request.fields, key=lambda s: s.lower()))),
            ('oauth_token', api_request.user_request.token)
        ]
    )
    url = '{host}/management/v1/counter/{counter_id}/logrequests?'\
        .format(host=HOST,
                counter_id=api_request.user_request.counter_id) \
          + url_params

    r = requests.post(url)
    logger.debug(r.text)
    if r.status_code == 200:
        logger.debug(json.dumps(json.loads(r.text)['log_request'], indent=2))
        response = json.loads(r.text)['log_request']
        api_request.status = response['status']
        api_request.request_id = response['request_id']
        # api_request.size = response['size']
        return response
    else:
        raise ValueError(r.text)


def update_status(api_request):
    '''Returns current tasks\'s status'''
    url = '{host}/management/v1/counter/{counter_id}/logrequest/{request_id}?oauth_token={token}' \
        .format(request_id=api_request.request_id,
                counter_id=api_request.user_request.counter_id,
                token=api_request.user_request.token,
                host=HOST)

    r = requests.get(url)
    logger.debug(r.text)
    if r.status_code == 200:
        status = json.loads(r.text)['log_request']['status']
        api_request.status = status
        if status == 'processed':
            size = len(json.loads(r.text)['log_request']['parts'])
            api_request.size = size
        return api_request
    else:
        raise ValueError(r.text)


def save_data(api_request, part):
    '''Loads data chunk from Logs API and saves to ClickHouse'''
    url = '{host}/management/v1/counter/{counter_id}/logrequest/{request_id}/part/{part}/download?oauth_token={token}' \
        .format(
            host=HOST,
            counter_id=api_request.user_request.counter_id,
            request_id=api_request.request_id,
            part=part,
            token=api_request.user_request.token
        )

    r = requests.get(url)
    if r.status_code != 200:
        logger.debug(r.text)
        raise ValueError(r.text)



    splitted_text = r.text.split('\n')
    logger.info('### DATA SAMPLE')
    logger.info('\n'.join(splitted_text[:5]))

    headers_num = len(splitted_text[0].split('\t'))
    splitted_text_filtered = filter(lambda x: len(x.split('\t')) == headers_num, r.text.split('\n'))
    num_filtered = len(splitted_text) - len(splitted_text_filtered)
    if num_filtered != 0:
        logger.warning('%d rows were filtered out' % num_filtered)

    #Get additional fields for clickHouse
    ch_fields_config = utils.get_ch_fields_config()
    ch_fields = ch_fields_config['{source}_fields'.format(source=api_request.user_request.source)]
    prefix = 'ym:pv:'
    if api_request.user_request.source == 'visits':
        prefix = 'ym:s:'

    #adds additional fields to the end
    if len(ch_fields) > 0:
        splitted_text_filtered[0] += '\t' + '\t'.join(ch_fields)

    headers = splitted_text[0].split('\t')

    if prefix + 'params' in headers and prefix + 'URL' in headers:
        params_index = headers.index(prefix + 'params')

        url_index = headers.index(prefix + 'URL')

        #parse the params
        i = 1
        while i < len(splitted_text_filtered):
            value = splitted_text_filtered[i].split('\t')

            for field in ch_fields:
                splitted_text_filtered[i] += "\t"
                params_json = clear_json(value[params_index])
                url = clear_json(value[url_index])
                if not is_json(params_json):
                    continue
                params = json.loads(params_json)
                if len(params) > 0:
                    if type(params) is list:
                        params = params[0]
                    data = parsing_params.get_data_from_params(prefix, params, field, url)
                    splitted_text_filtered[i] += unicode(data)
            i += 1

    output_data = '\n'.join(splitted_text_filtered).encode('utf-8')
    output_data = output_data.replace(r"\'", "'") # to correct escapes in params

    clickhouse.save_data(api_request.user_request.source,
                         api_request.user_request.fields,
                         output_data)

    api_request.status = 'saved'

#Removes duplicate characters
def clear_json(str):
    return str.encode('utf8') \
        .replace("'", "") \
        .replace('""', '"') \
        .replace('"{', "{") \
        .replace('}"', '}') \
        .replace("\ ", "") \
        .replace('\\"', "\"") \
        .replace('"[', "[") \
        .replace(']"', ']')

#checks whether the JSON string is
def is_json(myjson):
    try:
        json.loads(myjson)
    except ValueError, e:
        return False
    return True

def clean_data(api_request):
    '''Cleans generated data on server'''
    url = '{host}/management/v1/counter/{counter_id}/logrequest/{request_id}/clean?oauth_token={token}' \
        .format(host=HOST,
                counter_id=api_request.user_request.counter_id,
                token=api_request.user_request.token,
                request_id=api_request.request_id)
    r = requests.post(url)
    logger.debug(r.text)
    if r.status_code != 200:
        raise ValueError(r.text)

    api_request.status = json.loads(r.text)['log_request']['status']
    return json.loads(r.text)['log_request']
