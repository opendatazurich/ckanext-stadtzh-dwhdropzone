from setuptools import setup, find_packages
import sys, os

version = '0.0'

setup(
    name='ckanext-stadtzh-dwhdropzone',
    version=version,
    description="CKAN extension for the City of Zurich for the DWH Dropzone",
    long_description="""\
    """,
    classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
    keywords='',
    author='Liip AG',
    author_email='ogd@liip.ch',
    url='http://www.liip.ch',
    license='GPL',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    namespace_packages=['ckanext', 'ckanext.stadtzhdwhdropzone'],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        # -*- Extra requirements: -*-
    ],
    entry_points=\
    """
    [ckan.plugins]
    stadtzhdwhdropzone=ckanext.stadtzhdwhdropzone.plugins:StadtzhdwhdropzoneHarvest
    stadtzhdwhdropzone_harvester=ckanext.stadtzhdwhdropzone.harvesters:StadtzhdwhdropzoneHarvester
    [paste.paster_command]
    harvester=ckanext.stadtzhdwhdropzone.commands.harvester:HarvesterCommand
    """,
)
