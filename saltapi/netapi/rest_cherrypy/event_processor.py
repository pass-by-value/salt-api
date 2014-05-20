import json
import logging

logger = logging.getLogger(__name__)


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

        '''
        These represent a "real time" view into Salt's jobs.
        '''
        self.jobs = {}

        '''
        This represents a "real time" view of minions connected to
        Salt.
        '''
        self.minions = {}

    def publish(self, key, data):
        '''
        Publishes the data to the event stream.
        '''
        publish_data = {key: data}
        self.handler.send(json.dumps(publish_data), False)

    def process_minion_update(self, event_data):
        '''
        Associate grains data with a minion and publish minion update
        '''
        tag = event_data['tag']
        event_info = event_data['data']

        _, _, _, _, mid = tag.split('/')

        if not self.minions.get(mid, None):
            self.minions[mid] = {}

        minion = self.minions[mid]

        minion.update({'grains': event_info['return']})

        self.publish('minions', self.minions)

    def process_ret_job_event(self, event_data):
        '''
        Process a /ret event returned by Salt for a particular minion.
        These events contain the returned results from a particular execution.
        '''
        tag = event_data['tag']
        event_info = event_data['data']

        _, _, jid, _, mid = tag.split('/')
        job = self.jobs[jid]

        minion = job['minions'][mid]
        minion.update({'return': event_info['return']})
        minion.update({'retcode': event_info['retcode']})
        minion.update({'success': event_info['success']})

        job_complete = all([minion['success'] for mid, minion
                            in job['minions'].iteritems()])

        if job_complete:
            job['state'] = 'complete'

        self.publish('jobs', self.jobs)

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
        for mid in event_info['minions']:
            minions[mid] = {'success': False}

        job = {
            'jid': event_info['jid'],
            'start_time': event_info['_stamp'],
            'minions': minions,  # is a dictionary keyed my mids
            'fun': event_info['fun'],
            'tgt': event_info['tgt'],
            'tgt_type': event_info['tgt_type'],
            'state': 'running',
        }
        self.jobs[event_info['jid']] = job
        self.publish('jobs', self.jobs)

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
                if salt_data['data']['fun'] == 'grains.items':
                    self.process_minion_update(salt_data)
