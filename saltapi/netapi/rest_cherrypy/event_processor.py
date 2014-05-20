import json
import logging

logger = logging.getLogger(__name__)

jobs = {}


class SaltInfo:
    '''
    Class to  handle processing and publishing of "real time" Salt upates.
    '''

    def __init__(self, handler):
        '''
        handler is expected to be the server side end of a websocket
        connection.
        '''
        self.handler = handler

    def publish(self, key, data):
        '''
        Publishes the data to the event stream.
        '''
        publish_data = {key: data}
        self.handler.send(json.dumps(publish_data), False)

    def process_ret_job_event(self, event_data):
        tag = event_data['tag']
        event_info = event_data['data']

        _, _, jid, _, mid = tag.split('/')

        job = jobs[jid]

        minion = job['minions'][mid]
        minion.update({'return': event_info['return']})
        minion.update({'retcode': event_info['retcode']})
        minion.update({'success': event_info['success']})
        self.publish('jobs', jobs)

    def process_new_job_event(self, event_data):
        '''
        Creates a new job with properties from the event data
        like jid, function, args, timestamp.

        Also sets the initial state to started.

        Minions that are participating in this job are also noted.

        '''
        job = None
        tag = event_data['tag']
        event_info = event_data['data']
        minions = {}
        logger.info('event data is {}'.format(event_data))
        logger.info('event info is {}'.format(event_info))
        for mid in event_info['minions']:
            minions[mid] = {}

        job = {
            'jid': event_info['jid'],
            'start_time': event_info['_stamp'],
            'minions': minions,  # is a dictionary keyed my mids
            'fun': event_info['fun'],
            'tgt': event_info['tgt'],
            'tgt_type': event_info['tgt_type'],
            'state': 'running',
        }
        jobs[event_info['jid']] = job
        self.publish('jobs', jobs)

    def process(self, salt_data):
        '''
        Process events and publish data
        '''
        parts = salt_data['tag'].split('/')
        if parts[1] == 'job':
            if parts[3] == 'new':
                self.process_new_job_event(salt_data)
            elif parts[3] == 'ret':
                self.process_ret_job_event(salt_data)
