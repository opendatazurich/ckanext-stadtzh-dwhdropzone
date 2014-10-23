# coding: utf-8

from pylons import config
from ckan.model import Session
from ckanext.stadtzhharvest.harvester import StadtzhHarvester

import logging
log = logging.getLogger(__name__)


class StadtzhdwhdropzoneHarvester(StadtzhHarvester):
    '''
    The harvester for the Stadt ZH DWH Dropzone
    '''

    DATA_PATH = '/usr/lib/ckan/DWH'
    METADATA_PATH = config.get('metadata.metadatapath', '/usr/lib/ckan/diffs/dwh-metadata')

    def info(self):
        '''
        Return some general info about this harvester
        '''
        return {
            'name': 'stadtzhdwhdropzone',
            'title': 'Stadtzhdwhdropzone',
            'description': 'Harvests the Stadtzhdwhdropzone data',
            'form_config_interface': 'Text'
        }

    def gather_stage(self, harvest_job):
        log.debug('In StadtzhdwhdropzoneHarvester gather_stage')
        return self._gather_datasets(harvest_job)

    def fetch_stage(self, harvest_object):
        log.debug('In StadtzhdwhdropzoneHarvester fetch_stage')
        return self._fetch_datasets(harvest_object)

    def import_stage(self, harvest_object):
        log.debug('In StadtzhdwhdropzoneHarvester import_stage')
        return self._import_datasets(harvest_object)
