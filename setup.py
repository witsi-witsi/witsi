#!/usr/bin/env python3

import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name='witsi',
    version='0.1',
    author="Pewen",
    description="Some common function usen in the spiders",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/witsi-witsi/witsi",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)"
        "Operating System :: OS Independent",
    ],
)
