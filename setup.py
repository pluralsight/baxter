"""A setuptools based setup module.
See:
https://packaging.python.org/en/latest/distributing.html
https://github.com/pypa/sampleproject
"""

# Always prefer setuptools over distutils
from setuptools import setup, find_packages

# requirements = None
# with open('requirements.txt', 'r') as f:
#     requirements = [
#         line.split('==')[0]
#         for line in f.read().split('\n')
#     ]

# Read the version number
with open("baxter/_version.py") as f:
    exec(f.read())

setup(name='baxter',
      version=__version__, # use the same version that's in _version.py
      packages=['baxter'],
      license='LICENSE.txt',
      description='libraries for data engineering, created by Pluralsight data team',
      long_description=open('README.rst').read(),
      install_requires=[
                        'httplib2',
                        'google-api-python-client',
                        'urllib3',
                        'oauth2client',
                        'pyodbc',
                        'requests',
                        'psycopg2'
                       ]
                        #,
                        #'impyla',
                        #'pexpect',
                        #'MySQL-python'
)
