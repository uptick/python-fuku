# fuku

Fuku is a (young) system to help manage web application infrastructure
using AWS.


## Requirements:

 * Python 3.6

 * ssh

 * ssh-agent

 * gpg

 * psql

 * awscli


## Installation

Standard pip install:

```bash
pip install fuku
```


## Quickstart

TODO


## Quickstart for existing configurations

 1. Configure your AWS credentials as usual (aws configure).

 2. `fuku profile ls` to see available profiles.

    `fuku profile sl <name>` to select your profile.

 3. `fuku profile bucket <bucket>` to set a workspace for Fuku.

 4. `fuku region ls` to see available regions.

    `fuku region sl <region>` to select a region.

 5. `fuku cluster ls` to see available clusters.

    `fuku cluster sl <cluster>` to select a cluster.

    Enter password to access cluster key file.

 6. `fuku app ls` to see available apps.

    `fuku app sl <app>` to select an app.

 7. `fuku pg ls` to see available DBs.

    `fuku pg sl <db>` to select a DB.

    Enter password to access PGPASS file.

At this point you have configured your session for a particular cluster, application,
and database. For convenience it's best to cache the session for easy retrieval:

 `fuku session sv <name>`

 `fuku session ld <name>`


To download the current database:

 `fuku pg dump <dumpfile>`

To overwrite a database with new content (CAUTION):

 `fuku pg restore <filename>`

To access one of the nodes in the cluster directly:

 `fuku node ssh <name>`
