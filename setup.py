import os
import sys
from setuptools import setup
from setuptools.command.test import test as TestCommand


def read(fname):
    """Utility function to read the README file into the long_description."""
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


install_requires_list = ['pandas>=0.18.1',
                         'matplotlib>=1.5',
                         'numpy>=1.11.0',
                         'frozendict>=0.6',
                         'simplejson>=3.8.2',
                         'requests[security]>=2.10.0',
                         'psycopg2==2.6.1',
                         'langdetect>=1.0.6',
                         'scipy>=0.17.1',
                         'spacy>=0.100']


class PyTest(TestCommand):
    user_options = [('pytest-args=', 'a', "Arguments to pass to py.test")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = []

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import pytest
        errno = pytest.main(self.pytest_args)
        sys.exit(errno)


packages_list = [root for root, dirs, files in os.walk('arcs')]


setup(
    include_package_data=True,
    name="arcs",
    version="0.0.1",
    author="The Discovery Team",
    author_email="discovery-l@socrata.com",
    description=("A library for assessing relevance of Socrata's catalog "
                 "search"),
    license = "TBD",
    keywords = "search relevance",
    url = "http://www.socrata.com",
    packages=packages_list,
    long_description=read('README.md'),
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Socrata",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    install_requires=install_requires_list,
    setup_requires=['pytest-runner'],
    tests_require=["pytest==2.6.4"],
    cmdclass={'test': PyTest})
