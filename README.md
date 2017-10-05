# fuku

Fuku is a (young) system to help manage web application infrastructure
using AWS. It's aim is to replicate some of the simplicity of Heroku without
the cost.


## Requirements:

Please install the following requirements prior to installing Fuku:

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


## Quickstart for new configurations

 1. Configure your AWS credentials as usual (aws configure).

 2. `fuku profile ls` to see available profiles.

    `fuku profile mk <name>` to make the AWS groups etc required.

 3. `fuku profile bucket <bucket>` to set a workspace for Fuku.

 4. `fuku region ls` to see available regions.

    `fuku region sl <region>` to select a region.

 5. `fuku cluster mk <name>` to create a cluster.


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


## Downloading current DB

To download the current database:

 `fuku pg dump <dumpfile>`


## Upload DB

To overwrite a database with new content (CAUTION):

 `fuku pg restore <filename>`


## SSH into a node

To access one of the nodes in the cluster directly:

 `fuku node ssh <name>`


## Run an arbitrary command

To run a command:

 `fuku service run <task> <command>`

This attaches to a running container from the specified task, then
runs the provided command.


## Logging

To control logs printed use the flag `--log`, it uses the available logging levels (CRITICAL, WARNING, INFO, DEBUG)

 `fuku --log=DEBUG <command>`

By default the logs are set to WARNING.


## Running fuku in Sub-processes

The default behaviour is to assume that one user is on a single app and/or DB instance at all time.

However if you need to spawn multiple processes running commands on different app and/or DB instance,
you can use the `--app` or `--db` flags.

For example, we can run in parallel:

  `fuku --app=first_app service wait bg; fuku --app=first_app service run bg "./manage.py migrate";`

  `fuku --app=second_app service wait bg; fuku --app=second_app service run bg "./manage.py migrate";`
