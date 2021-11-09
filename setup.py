
from table import __version__

from setuptools import find_packages, setup


NAME = "table"
DESCRIPTION = "A 'datatype' that sits between a dictionary and database"
AUTHOR = "Kyle Emrick"
GITID = "kremrik"


setup(
    name=NAME,
    version=__version__,
    author=AUTHOR,
    url="https://github.com/{}/{}".format(GITID, NAME),
    description=DESCRIPTION,
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    packages=find_packages(exclude=["benchmark", "profiler", "tests"]),
    include_package_data=True,
)
