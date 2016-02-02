from distutils.core import setup

setup(
    name='gpudb',
    version='4.0',
    description='Python client for GPUdb',
    packages=['gpudb',],
    package_data={'gpudb' : ['obj_defs/*.json']},
    url='http://gpudb.com',
)

