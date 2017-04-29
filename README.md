Requirements:

 * Python 3.6

 * ssh

 * ssh-agent

 * gpg

 * psql

 * awscli


1. Configure your AWS credentials as usual (aws configure).

2. (a) `fuku profile ls` to see available profiles.
   (b) `fuku profile sl <name>` to select your profile.

3. `fuku profile bucket <bucket>` to set a workspace for Fuku.

4. (a) `fuku region ls` to see available regions.
   (b) `fuku region sl <region>` to select a region.

5. (a) `fuku cluster ls` to see available clusters.
   (b) `fuku cluster sl <cluster>` to select a cluster.
   (c) Enter password to access cluster key file.

6. (a) `fuku app ls` to see available apps.
   (b) `fuku app sl <app>` to select an app.

7. (a) `fuku pg ls` to see available DBs.
   (b) `fuku pg sl <db>` to select a DB.
   (c) Enter password to access PGPASS file.

At this point you have configured your session for a particular cluster, application,
and database. For convenience it's best to cache the session for easy retrieval:

 `fuku session save <name>`

 `fuku session load <name>`
