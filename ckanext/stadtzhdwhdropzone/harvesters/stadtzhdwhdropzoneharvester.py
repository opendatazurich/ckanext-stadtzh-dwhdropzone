#coding: utf-8

import os
import time
import datetime
import difflib
from lxml import etree

from ofs import get_impl
from pylons import config
from ckan.lib.base import c
from ckan import model
from ckan.model import Session, Package
from ckan.logic import ValidationError, NotFound, get_action, action
from ckan.lib.helpers import json
from ckan.lib.munge import munge_title_to_name

from ckanext.harvest.model import HarvestJob, HarvestObject, HarvestGatherError, \
                                    HarvestObjectError
from ckanext.harvest.harvesters import HarvesterBase

from pylons import config

import logging
log = logging.getLogger(__name__)

class StadtzhdwhdropzoneHarvester(HarvesterBase):
    '''
    The harvester for the Stadt ZH DWH Dropzone
    '''

    ORGANIZATION = {
        'de': u'Stadt Zürich',
        'fr': u'fr_Stadt Zürich',
        'it': u'it_Stadt Zürich',
        'en': u'en_Stadt Zürich',
    }
    LANG_CODES = ['de', 'fr', 'it', 'en']
    BUCKET = config.get('ckan.storage.bucket', 'default')
    CKAN_SITE_URL = config.get('ckan.site_url', 'http://stadtzh.lo')

    config = {
        'user': u'harvest'
    }

    DROPZONE_PATH = '/usr/lib/ckan/DWH'
    METADATA_PATH = config.get('metadata.metadatapath', '/usr/lib/ckan/diffs/dwh-metadata')
    DIFF_PATH = config.get('metadata.diffpath', '/usr/lib/ckan/diffs')

    # ---
    # COPIED FROM THE CKAN STORAGE CONTROLLER
    # ---

    def create_pairtree_marker(self, folder):
        """ Creates the pairtree marker for tests if it doesn't exist """
        if not folder[:-1] == '/':
            folder = folder + '/'

        directory = os.path.dirname(folder)
        if not os.path.exists(directory):
            os.makedirs(directory)

        target = os.path.join(directory, 'pairtree_version0_1')
        if os.path.exists(target):
            return

        open(target, 'wb').close()


    def get_ofs(self):
        """Return a configured instance of the appropriate OFS driver.
        """
        storage_backend = config['ofs.impl']
        kw = {}
        for k, v in config.items():
            if not k.startswith('ofs.') or k == 'ofs.impl':
                continue
            kw[k[4:]] = v

        # Make sure we have created the marker file to avoid pairtree issues
        if storage_backend == 'pairtree' and 'storage_dir' in kw:
            self.create_pairtree_marker(kw['storage_dir'])

        ofs = get_impl(storage_backend)(**kw)
        return ofs

    # ---
    # END COPY
    # ---

    def _remove_hidden_files(self, file_list):
        '''
        Removes dotfiles from a list of files
        '''
        cleaned_file_list = []
        for file in file_list:
            if not file.startswith('.'):
                cleaned_file_list.append(file)
        return cleaned_file_list


    def _generate_tags(self, dataset_node):
        '''
        Given a dataset node it extracts the tags and returns them in an array
        '''
        if dataset_node.find('keywords') is not None:
            return dataset_node.find('keywords').text.split(', ')
        else:
            return []


    def _generate_resources_dict_array(self, dataset):
        '''
        Given a dataset folder, it'll return an array of resource metadata
        '''
        resources = []
        resource_files = self._remove_hidden_files((f for f in os.listdir(os.path.join(self.DROPZONE_PATH, dataset))
            if os.path.isfile(os.path.join(self.DROPZONE_PATH, dataset, f))))
        log.debug(resource_files)

        # for resource_file in resource_files:
        for resource_file in (x for x in resource_files if x != 'meta.xml'):
            resources.append({
                # 'url': '', # will be filled in the import stage
                'name': resource_file,
                'format': resource_file.split('.')[-1],
                'resource_type': 'file'
            })

        return resources


    def _generate_attribute_notes(self, attributlist_node):
        '''
        Compose the attribute notes for all the given attributes
        '''
        response = u'##Attribute  \n'
        for attribut in attributlist_node:
            response += u'**' + attribut.find('sprechenderfeldname').text + u'**  \n'
            if attribut.find('feldbeschreibung').text != None:
                response += attribut.find('feldbeschreibung').text + u'  \n'
        return response

    def _node_exists_and_is_nonempty(self, dataset_node, element_name):
        element = dataset_node.find(element_name)
        if element == None:
            log.debug('TODO: send a message to SSZ, telling them Georg has to fix the meta.xml (OGDZH-29)')
            return None
        elif element.text == None:
            return None
        else:
            return element.text

    def _get(self, node, name):
        element = self._node_exists_and_is_nonempty(node, name)
        if element:
            return element
        else:
            return ''

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

        ids = []

        # list directories in dwhdropzone folder
        datasets = self._remove_hidden_files(os.listdir(self.DROPZONE_PATH))
        for dataset in datasets:
            meta_xml_file_path = os.path.join(self.DROPZONE_PATH, dataset, 'meta.xml')
            metadata = {}

            # check if a meta.xml exists
            if os.path.exists(meta_xml_file_path):
                with open(meta_xml_file_path, 'r') as meta_xml:
                    parser = etree.XMLParser(encoding='utf-8')
                    dataset_node = etree.fromstring(meta_xml.read(), parser=parser).find('datensatz')

                    metadata = {
                        'datasetID': dataset,
                        'title': dataset_node.find('titel').text,
                        'url': dataset_node.find('lieferant').text, 
                        'notes': dataset_node.find('beschreibung').text,
                        'author': dataset_node.find('quelle').text,
                        'maintainer': 'Open Data Zürich',
                        'maintainer_email': 'opendata@zuerich.ch',
                        'license_id': 'cc-zero',
                        'license_url': 'http://opendefinition.org/licenses/cc-zero/',
                        'tags': self._generate_tags(dataset_node),
                        'groups': dataset_node.find('kategorie').text,
                        'resources': self._generate_resources_dict_array(dataset),
                        'extras': [
                            ('spatialRelationship', self._get(dataset_node, 'raeumliche_beziehung')),
                            ('dateFirstPublished', self._get(dataset_node, 'erstmalige_veroeffentlichung')),
                            ('dateLastUpdated', self._get(dataset_node, 'aktualisierungsdatum')),
                            ('updateInterval', self._get(dataset_node, 'aktualisierungsintervall').replace(u'ä', u'ae').replace(u'ö', u'oe').replace(u'ü', u'ue')),
                            ('dataType', self._get(dataset_node, 'datentyp')),
                            ('legalInformation', self._get(dataset_node, 'rechtsgrundlage')),
                            ('version', self._get(dataset_node, 'aktuelle_version')),
                            ('timeRange', self._get(dataset_node, 'zeitraum')),
                            ('comments', self._get(dataset_node, 'bemerkungen')),
                            ('attributes', self._json_encode_attributes(self._get_attributes(dataset_node))),
                            ('dataQuality', self._get(dataset_node, 'datenqualitaet'))
                        ],
                        'related': self._get_related(dataset_node)
                    }

                    for extra in metadata['extras']                        :
                        if extra[0] == 'updateInterval' or extra[0] == 'dataType':
                            if not extra[1]:
                                metadata['extras'].append((extra[0], '   '))
                                metadata['extras'].remove(extra)
                                log.debug('No value in meta.xml for %s' % extra[0])

            else:
                metadata = {
                    'datasetID': dataset,
                    'title': dataset,
                    'url': None
                }

            # Get group IDs from group titles
            user = model.User.get(self.config['user'])
            context = {
                'model': model,
                'session': Session,
                'user': self.config['user']
            }

            groups = []
            group_titles = metadata['groups'].split(', ')
            for title in group_titles:
                if title == u'Bauen und Wohnen':
                    name = u'bauen-wohnen'
                else:
                    name = title.lower().replace(u'ö', u'oe').replace(u'ä', u'ae')
                try:
                    data_dict = {'id': name}
                    group_id = get_action('group_show')(context, data_dict)['id']
                    groups.append(group_id)
                    log.debug('Added group %s' % name)
                except:
                    log.debug('Couldn\'t get group id for title %s.' % title)
            metadata['groups'] = groups

            obj = HarvestObject(
                guid = metadata['datasetID'],
                job = harvest_job,
                content = json.dumps(metadata)
            )
            obj.save()
            log.debug('adding ' + metadata['datasetID'] + ' to the queue')
            ids.append(obj.id)

            if not os.path.isdir(os.path.join(self.METADATA_PATH, dataset)):
                os.makedirs(os.path.join(self.METADATA_PATH, dataset))

            with open(os.path.join(self.METADATA_PATH, dataset, 'metadata-' + str(datetime.date.today())), 'w') as meta_json:
                meta_json.write(json.dumps(metadata, sort_keys=True, indent=4, separators=(',', ': ')))
                log.debug('Metadata JSON created')

        return ids


    def fetch_stage(self, harvest_object):
        log.debug('In StadtzhdwhdropzoneHarvester fetch_stage')

        # Get the URL
        datasetID = json.loads(harvest_object.content)['datasetID']
        log.debug(harvest_object.content)

        # Get contents
        try:
            harvest_object.save()
            log.debug('successfully processed ' + datasetID)
            return True
        except Exception, e:
            log.exception(e)



    def import_stage(self, harvest_object):
        log.debug('In StadtzhdwhdropzoneHarvester import_stage')

        if not harvest_object:
            log.error('No harvest object received')
            return False


        try:
            package_dict = json.loads(harvest_object.content)
            package_dict['id'] = harvest_object.guid
            package_dict['name'] = munge_title_to_name(package_dict[u'datasetID'])

            user = model.User.get(self.config['user'])
            context = {
                'model': model,
                'session': Session,
                'user': self.config['user']
            }

            # Find or create the organization the dataset should get assigned to.
            try:
                data_dict = {
                    'permission': 'edit_group',
                    'id': munge_title_to_name(self.ORGANIZATION['de']),
                    'name': munge_title_to_name(self.ORGANIZATION['de']),
                    'title': self.ORGANIZATION['de']
                }
                package_dict['owner_org'] = get_action('organization_show')(context, data_dict)['id']
            except:
                organization = get_action('organization_create')(context, data_dict)
                package_dict['owner_org'] = organization['id']

            # Insert or update the package
            package = model.Package.get(package_dict['id'])
            if package: # package has already been imported.
                # create a diff between this new metadata set and the one from yesterday.
                # send the diff to SSZ

                today = datetime.date.today()
                new_metadata_path = os.path.join(self.METADATA_PATH, package_dict['id'], 'metadata-' + str(today))
                prev_metadata_path = os.path.join(self.METADATA_PATH, package_dict['id'], 'metadata-previous')
                diff_path = os.path.join(self.DIFF_PATH, str(today) + '-' + package_dict['id'] + '.html')

                if not os.path.isdir(self.DIFF_PATH):
                    os.makedirs(self.DIFF_PATH)

                if os.path.isfile(new_metadata_path):
                    if os.path.isfile(prev_metadata_path):
                        with open(prev_metadata_path) as prev_metadata:
                            with open(new_metadata_path) as new_metadata:
                                if prev_metadata.read() != new_metadata.read():
                                    with open(prev_metadata_path) as prev_metadata:
                                        with open(new_metadata_path) as new_metadata:
                                            with open(diff_path, 'w') as diff:
                                                diff.write(
                                                    "<!DOCTYPE html>\n<html>\n<body>\n<h2>Metadata diff for the dataset <a href=\""
                                                    + "https://ogd-integ.global.szh.loc/dataset/" + package_dict['id'] + "\">"
                                                    + package_dict['id'] + "</a></h2></body></html>\n"
                                                )
                                                d = difflib.HtmlDiff(wrapcolumn=60)
                                                umlauts = {
                                                    "\\u00e4": "ä",
                                                    "\\u00f6": "ö",
                                                    "\\u00fc": "ü",
                                                    "\\u00c4": "Ä",
                                                    "\\u00d6": "Ö",
                                                    "\\u00dc": "Ü",
                                                    "ISO-8859-1": "UTF-8"
                                                }
                                                html = d.make_file(prev_metadata, new_metadata, context=True, numlines=1)
                                                for code in umlauts.keys():
                                                    html = html.replace(code, umlauts[code])
                                                diff.write(html)
                                                log.debug('Metadata diff generated for the dataset: ' + package_dict['id'])
                                else:
                                    log.debug('No change in metadata for the dataset: ' + package_dict['id'])
                        os.remove(prev_metadata_path)
                        log.debug('Deleted previous day\'s metadata file.')
                    else:
                        log.debug('No earlier metadata JSON')

                    os.rename(new_metadata_path, prev_metadata_path)

                else:
                    log.debug('Metadata JSON missing for the dataset: ' + package_dict['id'])
            else: # package does not exist, therefore create it
                pkg_role = model.PackageRole(package=package, user=user, role=model.Role.ADMIN)

            # Move file around and make sure it's in the file-store
            for r in package_dict['resources']:
                if r['resource_type'] == 'file':
                    label = package_dict['datasetID'] + '/' + r['name']
                    file_contents = ''
                    with open(os.path.join(self.DROPZONE_PATH, package_dict['datasetID'], r['name'])) as contents:
                        file_contents = contents.read()
                    params = {
                        'filename-original': 'the original file name',
                        'uploaded-by': self.config['user']
                    }
                    r['url'] = self.CKAN_SITE_URL + '/storage/f/' + label
                    self.get_ofs().put_stream(self.BUCKET, label, file_contents, params)

            if not package:
                result = self._create_or_update_package(package_dict, harvest_object)
                if 'related' in package_dict:
                    self._related_create_or_update(package_dict['name'], package_dict['related'])
                Session.commit()

        except Exception, e:
            log.exception(e)

        return True

    def _json_encode_attributes(self, properties):
        attributes = []
        for key, value in properties:
            if value:
                attributes.append((key, value))

        return json.dumps(attributes)

    def _get_attributes(self, node):
        attribut_list = node.find('attributliste')
        attributes = []
        for attribut in attribut_list:
            tech_name = attribut.get('technischerfeldname')
            speak_name = attribut.find('sprechenderfeldname').text
            attributes.append(('%s (technisch: %s)' % (speak_name, tech_name), attribut.find('feldbeschreibung').text))
        return attributes

    def _get_related(self, xpath):
        related = []
        app_list = xpath.find('anwendungen')
        for app in app_list:
            related.append({
                'title': self._get(app, 'beschreibung'),
                'type': 'Applikation',
                'url': self._get(app, 'url')
            })
        pub_list = xpath.find('publikationen')
        for pub in pub_list:
            related.append({
                'title': self._get(pub, 'beschreibung'),
                'type': 'Publikation',
                'url': self._get(pub, 'url')
            })
        return related

    def _related_create_or_update(self, dataset_id, data):
        context = {
            'model': model,
            'session': Session,
            'user': self.config['user']
        }

        related_items = {}
        data_dict = {
            'id': dataset_id
        }
        for related in action.get.related_list(context, data_dict):
            related_items[related['url']] = related

        for entry in data:
            entry['dataset_id'] = dataset_id
            if entry['url'] in related_items.keys():
                entry = dict(related_items[entry['url']].items() + entry.items())
                log.debug('Updating related %s' % entry)
                action.update.related_update(context, entry)
            else:
                try:
                    log.debug('Creating related %s' % entry)
                    action.create.related_create(context, entry)
                except Exception, e:
                    log.exception(e)
