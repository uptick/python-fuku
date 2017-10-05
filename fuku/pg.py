import os
import re
import uuid
from datetime import datetime, timedelta
from pprint import pprint

import botocore

from .db import get_rc_path
from .module import Module
from .utils import gen_secret


class Pg(Module):
    dependencies = ['app']

    def __init__(self, **kwargs):
        super().__init__('pg', **kwargs)

    def add_arguments(self, parser):
        subp = parser.add_subparsers(help='pg help')

        p = subp.add_parser('ls', help='list postgres DBs')
        p.add_argument('name', metavar='NAME', nargs='?', help='DB name')
        p.set_defaults(pg_handler=self.handle_list)

        p = subp.add_parser('mk', help='add a postgres instance')
        p.add_argument('name', metavar='NAME', help='instance name')
        p.add_argument('--backup', '-b', default=7, type=int, help='number of days to retain backups')
        p.add_argument('--storage', '-s', default=5, type=int, help='allocated storage (GB)')
        p.set_defaults(pg_handler=self.handle_make)

        # p = subp.add_parser('cache', help='cache instance details')
        # p.add_argument('name', metavar='NAME', help='DB name')
        # p.add_argument('password', metavar='PASSWORD', help='DB password')
        # p.set_defaults(pg_handler=self.handle_cache)

        p = subp.add_parser('db', help='manage databases')
        ssp = p.add_subparsers()

        p = ssp.add_parser('ls')
        p.set_defaults(pg_handler=self.handle_db_list)

        p = ssp.add_parser('mk')
        p.add_argument('dbname', metavar='DBNAME', help='DB name')
        p.set_defaults(pg_handler=self.handle_db_make)

        p = subp.add_parser('connect', help='connect to a task')
        p.add_argument('dbname', metavar='DBNAME', help='DB name')
        p.add_argument('--task', '-t', help='target task name')
        p.set_defaults(pg_handler=self.handle_connect)

        p = subp.add_parser('sl', help='select a postgres DB')
        p.add_argument('name', metavar='NAME', nargs='?', help='instance to select')
        p.set_defaults(pg_handler=self.handle_select)

        p = subp.add_parser('psql')
        p.add_argument('--dbname', '-d', help='DB name')
        p.add_argument('--command', '-c', help='run SQL')
        p.set_defaults(pg_handler=self.handle_psql)

        p = subp.add_parser('dump', help='dump contents of database')
        p.add_argument('dbname', metavar='DBNAME', help='DB name')
        p.add_argument('output', metavar='OUTPUT', help='output filename')
        p.set_defaults(pg_handler=self.handle_dump)

        p = subp.add_parser('restore', help='restore a database')
        p.add_argument('dbname', metavar='DBNAME', help='DB name')
        p.add_argument('input', metavar='INPUT', help='database dump file')
        p.set_defaults(pg_handler=self.handle_restore)

        p = subp.add_parser('rollback', help='rollback a database')
        p.add_argument('dbname', metavar='DBNAME', help='DB name')
        p.add_argument('time', metavar='TIME', help='rollback time')
        p.set_defaults(pg_handler=self.handle_rollback)

        p = subp.add_parser('backup', help='backup a database to S3')
        p.add_argument('dbname', metavar='DBNAME', help='DB name')
        p.add_argument('--list', action='store_true', help='list backups')
        p.set_defaults(pg_handler=self.handle_backup)

        p = subp.add_parser('share', help='share a backed up database')
        p.add_argument('dbname', metavar='DBNAME', help='DB name')
        p.add_argument('key', metavar='KEY', help='backup key')
        p.set_defaults(pg_handler=self.handle_share)

        p = subp.add_parser('summary', help='summarize databases')
        p.set_defaults(pg_handler=self.handle_summary)

    def handle_list(self, args):
        self.list(args.name)

    def list(self, name):
        rds = self.get_boto_client('rds')
        if name:
            data = rds.describe_db_instances(
                Filters=[{
                    'Name': 'db-instance-id',
                    'Values': [self.get_instance_id(name)]
                }]
            )
            pprint(data['DBInstances'][0])
        else:
            for dbinst in self.iter_db_instances():
                print(dbinst)

    def iter_db_instances(self):
        rds = self.get_boto_client('rds')
        data = rds.describe_db_instances()
        ctx = self.get_context(use_context=False)
        pre = f'fuku-{ctx["cluster"]}-'
        for db in data['DBInstances']:
            name = db['DBInstanceIdentifier']
            if name.startswith(pre):
                yield name[len(pre):]

    def handle_make(self, args):
        self.make(args.name, args.backup, args.storage)

    def make(self, name, backup, storage=5):
        self.use_context = False
        ctx = self.get_context()
        password = gen_secret(16)
        inst_id = self.get_instance_id(name)
        db_id = 'postgres'
        sg_id = self.client.get_module('cluster').get_security_group_id()
        rds = self.get_boto_client('rds')
        rds.create_db_subnet_group(
            DBSubnetGroupName=inst_id,
            DBSubnetGroupDescription=f'Subnet group for {inst_id}',
            SubnetIds=[sn.id for sn in self.get_module('cluster').iter_public_subnets(ctx['cluster'])]
        )
        rds.create_db_instance(
            DBName=db_id,
            DBInstanceIdentifier=inst_id,
            DBInstanceClass='db.t2.micro',
            Engine='postgres',
            AllocatedStorage=storage,
            StorageType='gp2',
            MasterUsername=name,
            MasterUserPassword=password,
            BackupRetentionPeriod=backup,
            DBSubnetGroupName=inst_id,
            VpcSecurityGroupIds=[sg_id],
            PubliclyAccessible=True,
            Tags=[
                {
                    'Key': 'cluster',
                    'Value': ctx['cluster']
                }
            ]
        )
        waiter = rds.get_waiter('db_instance_available')
        waiter.wait(
            DBInstanceIdentifier=inst_id
        )
        self.cache(name, db_id, password)
        self.select(name)

    def handle_db_list(self, args):
        self.db_list()

    def db_list(self):
        for db in self.iter_dbs():
            print(db)

    def iter_dbs(self, inst_name=None):
        if inst_name is None:
            ctx = self.get_context()
            inst_name = ctx['dbinstance']
        else:
            ctx = self.get_context(use_context=False)
        path = os.path.join(ctx['cluster'], f'{inst_name}.pgpass')
        path = self.get_secure_file(path)
        endpoint = self.get_endpoint(inst_name)
        cmd = 'psql -h {} -p {} -U {} -d {}'.format(
            endpoint['Address'],
            endpoint['Port'],
            inst_name,
            'postgres'
        )
        sql = 'SELECT datname FROM pg_database WHERE datistemplate = false;'
        cmd = f'{cmd} -c "{sql}"'
        output = self.run(
            cmd,
            capture=True,
            env={'PGPASSFILE': path}
        )
        for db in output.split()[4:-2]:
            i = db.find('_')
            yield db[i + 1:]

    def handle_db_make(self, args):
        self.db_make(args.dbname)

    def db_make(self, name):
        password = gen_secret(16)
        ctx = self.get_context()
        inst_name = ctx['dbinstance']
        db_id = self.get_db_id(name)
        self.psql(command=f'CREATE DATABASE {db_id} OWNER {inst_name}')
        self.psql(command=f'CREATE ROLE {db_id} NOSUPERUSER NOCREATEDB NOCREATEROLE LOGIN ENCRYPTED PASSWORD \'{password}\'')
        self.psql(command=f'GRANT ALL ON DATABASE {db_id} TO {db_id}')
        self.psql(command=f'GRANT rds_superuser TO {db_id}')
        data = self.get_endpoint(inst_name)
        path = os.path.join(self.get_rc_path(), ctx['app'], inst_name, f'{name}.pgpass')
        try:
            os.makedirs(os.path.dirname(path))
        except OSError:
            pass
        with open(path, 'w') as outf:
            outf.write('{}:{}:{}:{}:{}'.format(
                data['Address'],
                data['Port'],
                db_id,
                db_id,
                password
            ))
        os.chmod(path, 0o600)
        self.encrypt_file(path, purpose='the database credentials')
        s3 = self.get_boto_client('s3')
        s3.upload_file(f'{path}.gpg', ctx['bucket'], f'fuku/{ctx["cluster"]}/{ctx["app"]}/{inst_name}/{name}.pgpass.gpg')

    # def handle_cache(self, args):
    #     self.cache(args.name, args.password)

    def cache(self, inst_name, db_name, password):
        ctx = self.get_context()
        data = self.get_endpoint(inst_name)
        path = os.path.join(self.get_rc_path(), f'{inst_name}.pgpass')
        try:
            os.makedirs(os.path.dirname(path))
        except OSError:
            pass
        with open(path, 'w') as outf:
            outf.write('{}:{}:{}:{}:{}'.format(
                data['Address'],
                data['Port'],
                db_name,
                inst_name,
                password
            ))
        os.chmod(path, 0o600)
        self.encrypt_file(path, purpose='the database credentials')
        s3 = self.get_boto_client('s3')
        s3.upload_file(f'{path}.gpg', ctx['bucket'], f'fuku/{ctx["cluster"]}/{inst_name}.pgpass.gpg')

    def handle_connect(self, args):
        self.connect(args.dbname, args.task)

    def connect(self, db_name, task_name):
        task_mod = self.client.get_module('task')
        env = {
            'DATABASE_URL': self.get_url(db_name)
        }
        task_mod.env_set(task_name, env)

    def handle_select(self, args):
        self.select(args.name)

    def select(self, name):
        if name:
            self.use_context = False
            ctx = self.get_context()
            pgpass_path = f'{ctx["cluster"]}/{name}.pgpass'
            # self.exists(name)
            self.get_secure_file(pgpass_path)

        self.store_set('selected', name)
        self.clear_parent_selections()

    def handle_psql(self, args):
        self.psql(args.dbname, args.command)

    def psql(self, db_name=None, command=None):
        ctx = self.get_context()
        inst_name = ctx['dbinstance']
        if db_name:
            db_id, path = self.get_db_creds(db_name)
        else:
            db_id = None
            path = os.path.join(self.get_rc_path(), f'{inst_name}.pgpass')
        endpoint = self.get_endpoint(inst_name)
        cmd = 'psql -h {} -p {} -U {} -d {}'.format(
            endpoint['Address'],
            endpoint['Port'],
            db_id or ctx['dbinstance'],
            db_id or 'postgres'
        )
        if command:
            cmd = f'{cmd} -c "{self.escape(command)}"'
        self.run(
            cmd,
            capture=False,
            env={'PGPASSFILE': path}
        )

    def handle_dump(self, args):
        self.dump(args.dbname, args.output)

    def dump(self, db_name, output):
        ctx = self.get_context()
        db_id, path = self.get_db_creds(db_name)
        endpoint = self.get_endpoint(ctx['dbinstance'])
        self.run(
            'pg_dump -Fc -x -O -h {} -p {} -U {} -d {} -f {}'.format(
                endpoint['Address'],
                endpoint['Port'],
                db_id,
                db_id,
                output
            ),
            capture=False,
            env={'PGPASSFILE': path}
        )

    def handle_restore(self, args):
        self.restore(args.dbname, args.input)

    def restore(self, db_name, input):
        ctx = self.get_context()
        db_id, path = self.get_db_creds(db_name)
        # self.psql(command=f'DROP DATABASE {db_id}')
        # self.psql(command=f'CREATE DATABASE {db_id} OWNER {inst_name}')
        # self.psql(command=f'GRANT ALL ON DATABASE {db_id} TO {db_id}')
        endpoint = self.get_endpoint(ctx['dbinstance'])
        # self.run(
        #     f'psql -h {endpoint["Address"]} -p {endpoint["Port"]} -U {db_name} {db_name} -c \'DROP SCHEMA public CASCADE; CREATE SCHEMA public;\'',
        #     env={'PGPASSFILE': path}
        # )
        self.run(
            f'pg_restore -x -O -c -h {endpoint["Address"]} -p {endpoint["Port"]} -U {db_id} -d {db_id} {input}',
            capture=False,
            env={'PGPASSFILE': path}
        )

    def handle_rollback(self, args):
        self.rollback(args.dbname, args.time)

    def rollback(self, db_name, time_str):
        # ctx = self.get_context()
        db_id, path = self.get_db_creds(db_name)
        # endpoint = self.get_endpoint(ctx['dbinstance'])
        rds_cli = self.get_client('rds')
        m = re.match(r'(\d+):(\d+):(\d+)', time_str)
        if not m:
            self.error('invalid time-ago string')
        time = datetime.utcnow() - timedelta(days=m.groups[1], minutes=m.groups[2], seconds=m.groups[3])
        rds_cli.restore_db_instance_to_point_in_time(
            SourceDBInstanceIdentifier=db_id,
            TargetDBInstanceIdentifier=f'{db_id}_rollback',
            RestoreTime=time
        )

    def handle_backup(self, args):
        self.backup(args.dbname, args.list)

    def backup(self, db_name, list):
        ctx = self.get_context()
        if list:
            pre = 'backups/{}/{}/'.format(
                ctx['dbinstance'],
                db_name,
            )
            for obj in self.iters3(pre):
                key = obj.key[obj.key.rfind('/') + 1:obj.key.rfind('.')]
                print(f'{key}  ({obj.last_modified})')
            return
        db_id, path = self.get_db_creds(db_name)
        endpoint = self.get_endpoint(ctx['dbinstance'])
        cmd = 'pg_dump -Fc -x -O -h {} -p {} -U {} -d {}'.format(
            endpoint['Address'],
            endpoint['Port'],
            db_id,
            db_id
        )
        # key = str(datetime.now()).replace(' ', '-')
        while 1:
            key = str(uuid.uuid4()).replace('-', '')[:8]
            s3 = self.get_boto_resource('s3')
            bucket_key = 'backups/{}/{}/{}.dump'.format(
                ctx['dbinstance'],
                db_name,
                key
            )
            try:
                s3.Object(ctx['bucket'], bucket_key)
                break
            except botocore.exceptions.ClientError as e:
                pass
        cmd += ' | aws s3 cp - s3://{}/backups/{}/{}/{}.dump --profile {}'.format(
            ctx['bucket'],
            ctx['dbinstance'],
            db_name,
            key,
            ctx['profile']
        )
        self.run(
            cmd,
            capture=False,
            env={'PGPASSFILE': path}
        )
        print(f'backed up as "{key}"')

    def handle_share(self, args):
        self.share(args.dbname, args.key)

    def share(self, db_name, key):
        ctx = self.get_context()
        db_id, path = self.get_db_creds(db_name)
        # endpoint = self.get_endpoint(ctx['dbinstance'])
        cmd = 'aws s3 presign s3://{}/backups/{}/{}/{}.dump --profile {}'.format(
            ctx['bucket'],
            ctx['dbinstance'],
            db_name,
            key,
            ctx['profile']
        )
        r = self.run(
            cmd,
            capture=True,
            env={'PGPASSFILE': path}
        )
        print(r)

    def handle_summary(self, args):
        self.summary()

    def summary(self):
        print('')
        for dbinst in self.iter_db_instances():
            print(dbinst)
            for db in self.iter_dbs(dbinst):
                print(f'  {db}')
            print('')

    def get_instance_id(self, instance):
        ctx = self.get_context(use_context=False)
        return f'fuku-{ctx["cluster"]}-{instance}'

    def get_db_id(self, name):
        ctx = self.get_context()
        return f'{ctx["app"]}_{name}'

    def get_rc_path(self):
        ctx = self.get_context(use_context=False)
        return os.path.join(get_rc_path(), ctx['cluster'])

    def get_endpoint(self, name):
        inst_id = self.get_instance_id(name)
        rds = self.get_boto_client('rds')
        try:
            return rds.describe_db_instances(
                DBInstanceIdentifier=inst_id
            )['DBInstances'][0]['Endpoint']
        except:
            self.error(f'no database "{name}"')

    def get_instance(self, inst_name):
        rds = self.get_boto_client('rds')
        return rds.describe_db_instances(
            Filters=[{
                'Name': 'db-instance-id',
                'Values': [self.get_instance_id(inst_name)]
            }]
        )

    def get_url(self, db_name):
        ctx = self.get_context()
        path = os.path.join(self.get_rc_path(), ctx['app'], ctx['dbinstance'], f'{db_name}.pgpass')
        try:
            with open(path, 'r') as inf:
                data = inf.read()
        except:
            self.error(f'no cached information for "{db_name}"')
        host, port, db, user, pw = data.split(':')
        return 'postgres://{}:{}@{}:{}/{}'.format(user, pw, host, port, db)

    # def get_pgpass_file(self, name):
    #     ctx = self.get_context()
    #     path = os.path.join(self.get_rc_path(), '%s.pgpass' % name)
    #     if not os.path.exists(path):
    #         s3 = self.get_boto_client('s3')
    #         s3.download_file(ctx['bucket'], f'fuku/{ctx["cluster"]}/{ctx["app"]}/{inst_name}/{name}.pgpass.gpg', path)
    #         self.run(
    #             'gpg -o {} -d {}.gpg'.format(path, path)
    #         )
    #         os.chmod(path, 0o600)

    def get_db_creds(self, db_name):
        ctx = self.get_context()
        db_id = self.get_db_id(db_name)
        app = ctx['app']
        inst_name = ctx['dbinstance']
        path = f'{ctx["cluster"]}/{app}/{inst_name}/{db_name}.pgpass'
        full_path = self.get_secure_file(path)
        return db_id, full_path

    def get_selected(self, fail=True):
        sel = self.store.get('selected', None)
        if not sel and fail:
            self.error('no postgres DB selected')
        return sel

    def get_my_context(self):
        if self.client.args.pg:
            ctx = {'dbinstance': self.client.args.pg}
        else:
            sel = self.store_get('selected')
            if not sel:
                self.error('no DB currently selected')
            ctx = {'dbinstance': sel}

        self.get_logger().debug(f'PG Context: {ctx}')
        return ctx
