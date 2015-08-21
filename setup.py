import os
import sys
from setuptools import setup
from setuptools.command.test import test as TestCommand


def read(fname):
    """Utility function to read the README file into the long_description."""
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


install_requires_list = ['pandas==0.16.2',
                         'matplotlib==1.4.3',
                         'numpy==1.9.1',
                         'SQLAlchemy==1.0.6',
                         'frozendict==0.4',
                         'simplejson==3.7.3',
                         'requests[security]==2.7.0',
                         'psycopg2==2.6.1',
                         'langdetect==1.0.5',
                         'crowdflower==0.1.3',
                         'scipy==0.16.0']


tests_require = ["pytest==2.6.4"]


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
    author="The AniML pack at Socrata",
    author_email="animl@socrata.com",
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
    tests_require=tests_require,
    cmdclass={'test': PyTest})
