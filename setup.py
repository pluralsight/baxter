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

# setup(name='pyLoveLamp',
#       version='0.1',
#       description='Classy data processing functions to make python etl easy',
#       author='Pluralsight Data Engineering',
#       author_email='data-engineering@pluralsight.com',
#       packages=find_packages(exclude=['contrib', 'docs', 'tests*']),
#       #packages=['googlecloud','hadoop','files','relationaldb','toobox'],
#       install_requires=['httplib2'],
# )

      #,
      #install_requires=requirements

setup(name='baxter',
      version='0.1',
      packages=['baxter'],
      install_requires=['httplib2', 'google-api-python-client', 'urllib3', 'oauth2client', 'pyodbc', 'impyla', 'pexpect']
)