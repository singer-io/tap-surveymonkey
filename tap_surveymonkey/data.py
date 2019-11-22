import requests
import time
import singer
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

LOGGER = singer.get_logger()


class SurveyMonkey(object):
    def __init__(self, access_token):
        self.access_token = access_token

    def make_request(self, endpoint, method='GET', state={}, **request_kwargs):
        headers = {
            'Authorization': 'bearer %s' % self.access_token,
            'Content-Type': 'application/json'
        }
        url = 'https://api.surveymonkey.com/v3/%s' % endpoint
        LOGGER.info('URL={}'.format(endpoint))
        resp = requests.request(method, url, headers=headers, **request_kwargs)

        # check rate limit
        day_remaining = int(resp.headers['X-Ratelimit-App-Global-Day-Remaining'])
        if day_remaining == 0:
            day_reset = int(resp.headers['X-Ratelimit-App-Global-Day-Reset'])
            day_reset += 2
            LOGGER.info(
                'Sleeping for %s seconds due to SurveyMonkey API rate limit... printing state' % day_reset)
            singer.write_state(state)
            time.sleep(day_reset)
            resp = requests.request(method, url, headers=headers, **request_kwargs)

        minute_remaining = int(resp.headers['X-Ratelimit-App-Global-Minute-Remaining'])
        if minute_remaining == 0:
            minute_reset = int(resp.headers['X-Ratelimit-App-Global-Minute-Reset'])
            minute_reset += 2
            LOGGER.info(
                'Sleeping for %s seconds due to SurveyMonkey API rate limit... printing state' % minute_reset)
            singer.write_state(state)
            time.sleep(minute_reset)
            resp = requests.request(method, url, headers=headers, **request_kwargs)

        return resp.json()
