import os
from setuptools import setup


def read(fname):
    """Utility function to read the README file into the long_description."""
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


install_requires_list = ['pandas==0.16.2',
                         'matplotlib==1.4.3',
                         'numpy==1.9.1',
                         'pytest==2.6.4',
                         'SQLAlchemy==1.0.6',
                         'frozendict==0.4',
                         'simplejson==3.7.3']


packages_list = [root for root, dirs, files in os.walk('arcs')]


setup(
    include_package_data=True,
    name="arcs",
    version="0.0.1",
    author="The AniML pack at Socrata",
    author_email="animl@socrata.com",
    description=("A library for assessing relevance of Socrata's catalog " \
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
    install_requires=install_requires_list)
