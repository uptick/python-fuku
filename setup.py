import os

from setuptools import find_packages, setup

version = '0.0.22'

setup(
    name='fuku',
    version=version,
    author='Luke Hodkinson',
    author_email='furious.luke@gmail.com',
    maintainer='Luke Hodkinson',
    maintainer_email='furious.luke@gmail.com',
    url='https://github.com/ABASystems/python-fuku',
    description='Easier Docker based deployments to AWS.',
    long_description=open(os.path.join(os.path.dirname(__file__), 'README.md')).read(),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5'
    ],
    license='BSD',
    packages=find_packages(),
    include_package_data=True,
    package_data={'': ['*.txt', '*.js', '*.html', '*.*']},
    install_requires=[
        'setuptools',
        'six',
        'boto3',
        'fabric3',
        'colorama',
        'pprint',
    ],
    scripts=[
        'fuku/scripts/fuku',
    ],
    zip_safe=False,
)
