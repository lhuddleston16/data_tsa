from setuptools import setup
import sys

def forbid_publish():
    argv = sys.argv
    blacklist = ['register', 'upload']

    for command in blacklist:
        if command in argv:
            values = {'command': command}
            print('Command "%(command)s" has been blacklisted, exiting...' %
                  values)
            sys.exit(2)

forbid_publish()

setup(name='data_tsa',
      version='0.0.1',
      description='A data profiling utility.',
      author='Slalom',
      packages=['data_tsa']
      )