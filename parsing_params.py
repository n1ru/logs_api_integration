#!/usr/bin/env python
# -*- coding: utf-8 -*-
from urlparse import urlparse
import logging

def get_data_from_params(prefix, params, field, url):
    logger = logging.getLogger('logs_api')
    try:
        action = params.keys()[0]

        if field == prefix + 'hitType':
            return action

        if field == prefix + 'app' and  params[action].has_key('app'):
            return params[action]['app']

        if field == prefix + 'offerID':
            return get_offerId_from_url(url)

        if params[action].has_key('region'):

            if type(params[action]['region']) is dict:
                region = params[action]['region'].keys()[0]
                city = params[action]['region'][region].keys()[0]
            else:
                region = params[action]['region']
                city = None

            if field == prefix + 'region':
                return region

            if city != None:
                if field == prefix + 'cityName':
                    return city
                if field == prefix + 'microdistrict' and params[action]['region'][region][city].has_key(u'Микрорайон'):
                    return params[action]['region'][region][city][u'Микрорайон']
                if field == prefix + 'district'and params[action]['region'][region][city].has_key(u'Район'):
                    return params[action]['region'][region][city][u'Район']
                if field == prefix + 'street' and params[action]['region'][region][city].has_key(u'Улица'):
                    return params[action]['region'][region][city][u'Улица']

        if params[action].has_key('from'):
            from_p = params[action]['from'].keys()[0] if type(params[action]['from']) is dict else params[action]['from']

            if field == prefix + 'from':
                return from_p
            if field == prefix + 'fromBlock' and type(params[action]['from']) is dict:
                return params[action]['from'][from_p]

        if field == prefix + 'page' and params[action].has_key('page'):
            return params[action]['page']

        if params[action].has_key('rubric'):
            rubric = params[action]['rubric'].keys()[0] if type(params[action]['rubric']) is dict else params[action]['rubric']

            if field == prefix + 'rubric':
                return rubric
            if field == prefix + 'realtyType' and rubric == 'flats':
                return params[action]['rubric'][rubric]

        if field == prefix + 'dealType' and params[action].has_key('dealType'):
            return params[action]['dealType']

        if field == prefix + 'price' and params[action].has_key('price') and params[action]['price'] != 0:
            return params[action]['price']

        if field == prefix + 'numberRooms' and params[action].has_key('numberRooms'):
            return params[action]['numberRooms']

        if field == prefix + 'ownerOffer' and params[action].has_key('ownerOffer'):
            return params[action]['ownerOffer']

        offer_services = params[action]['offerServices'] if params[action].has_key('offerServices') else {}

        if field == prefix + 'special':
            return 'true' if offer_services.has_key('special') else 'false'

        if field == prefix + 'premium':
            return 'true' if offer_services.has_key('premium') else 'false'

        if field == prefix + 'highlight':
            return 'true' if offer_services.has_key('highlight') else 'false'

        if field == prefix + 'autoraiseX2':
            return 'true' if offer_services.has_key('autoraiseX2') else 'false'

        if field == prefix + 'autoraiseX4':
            return 'true' if offer_services.has_key('autoraiseX4') else 'false'

        if field == prefix + 'autoraiseX8':
            return 'true' if offer_services.has_key('autoraiseX8') else 'false'

        if field == prefix + 'top':
            return 'true' if offer_services.has_key('top') else 'false'

        if field == prefix + 'phone' and params[action].has_key('phone'):
            return params[action]['phone']

    except TypeError, e:
        logger.warning('%s the nesting structure does not match' % field)

    return ''

def get_offerId_from_url(url):
    url_params = urlparse(url)
    path = url_params.path.split('/')
    if len(path) > 1 and path[1] == 'view':
        return path[2]
    return ''
