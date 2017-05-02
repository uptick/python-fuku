import os
import json

from .module import Module
from .db import get_rc_path
from .utils import gen_secret, gen_name


class Pg(Module):
    dependencies = ['app']

    def __init__(self, **kwargs):
        super().__init__('pg', **kwargs)

    def add_arguments(self, parser):
        subp = parser.add_subparsers(help='pg help')

        p = subp.add_parser('ls', help='list DBs')
        p.add_argument('name', metavar='NAME', nargs='?', help='DB name')
        p.set_defaults(pg_handler=self.handle_list)

        p = subp.add_parser('mk', help='add a postgres database')
        p.add_argument('name', metavar='NAME', help='DB name')
        p.add_argument('--backup', '-b', default=7, type=int, help='number of days to retain backups')
        p.set_defaults(pg_handler=self.handle_make)

        p = subp.add_parser('cache', help='cache DB details')
        p.add_argument('name', metavar='NAME', help='DB name')
        p.add_argument('password', metavar='PASSWORD', help='DB password')
        p.set_defaults(pg_handler=self.handle_cache)

        p = subp.add_parser('connect', help='connect to a task')
        p.add_argument('name', metavar='NAME', help='DB name')
        p.add_argument('target', metavar='TARGET', nargs='?', help='target task name')
        p.set_defaults(pg_handler=self.handle_connect)

        p = subp.add_parser('sl', help='select a postgres database')
        p.add_argument('name', metavar='NAME', nargs='?', help='database to select')
        p.add_argument('--show', '-s', action='store_true', help='show currently db')
        p.set_defaults(pg_handler=self.handle_select)

        p = subp.add_parser('psql')
        # p.add_argument('name', metavar='NAME', nargs='', help='DB name')
        p.add_argument('--command', '-c',  help='run SQL')
        p.set_defaults(pg_handler=self.handle_psql)

        p = subp.add_parser('dump', help='dump contents of database')
        p.add_argument('output', metavar='OUTPUT', help='output filename')
        p.set_defaults(pg_handler=self.handle_dump)

        p = subp.add_parser('restore', help='restore a database')
        p.add_argument('input', metavar='INPUT', help='database dump file')
        p.set_defaults(pg_handler=self.handle_restore)

    def handle_list(self, args):
        self.list(args.name)

    def list(self, name):
        self.use_context = False
        ctx = self.get_context()
        app = ctx['app']
        rds = self.get_boto_client('rds')
        if name:
            data = rds.describe_db_instances(
                Filter=[{
                    'Key': 'Name',
                    'Values': [self.get_id(name)]
                }]
            )
            print(json.dumps(data, indent=2))
        else:
            data = rds.describe_db_instances()
            pre = self.get_id('')
            for db in data['DBInstances']:
                name = db['DBInstanceIdentifier']
                if name.startswith(pre):
                    print(name[len(pre):])

    def handle_make(self, args):
        self.make(args.name, args.backup)

    def make(self, name, backup):
        self.use_context = False
        ctx = self.get_context()
        app = ctx['app']
        password = gen_secret(16)
        inst_id = self.get_id(name)
        sg_id = self.client.get_module('cluster').get_security_group_id()
        rds = self.get_boto_client('rds')
        rds.create_db_subnet_group(
            DBSubnetGroupName=inst_id,
            DBSubnetGroupDescription=f'Subnet group for {inst_id}',
            SubnetIds=[sn.id for sn in self.get_module('cluster').iter_public_subnets(ctx['cluster'])]
        )
        data = rds.create_db_instance(
            DBName=name,
            DBInstanceIdentifier=inst_id,
            DBInstanceClass='db.t2.micro',
            Engine='postgres',
            AllocatedStorage=5,
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
                },
                {
                    'Key': 'app',
                    'Value': app
                }
            ]
        )
        waiter = rds.get_waiter('db_instance_available')
        waiter.wait(
            DBInstanceIdentifier=inst_id
        )
        self.cache(name, password)
        self.select(name)

    def handle_cache(self, args):
        self.cache(args.name, args.password)

    def cache(self, name, password):
        ctx = self.get_context()
        data = self.get_endpoint(name)
        path = os.path.join(self.get_rc_path(), f'{name}.pgpass')
        try:
            os.makedirs(os.path.dirname(path))
        except OSError:
            pass
        with open(path, 'w') as outf:
            outf.write('{}:{}:{}:{}:{}'.format(
                data['Address'],
                data['Port'],
                name,
                name,
                password
            ))
        os.chmod(path, 0o600)
        self.encrypt_file(path, purpose='the database credentials')
        s3 = self.get_boto_client('s3')
        s3.upload_file(f'{path}.gpg', ctx['bucket'], f'fuku/{ctx["cluster"]}/{ctx["app"]}/{name}.pgpass.gpg')

    def handle_connect(self, args):
        self.connect(args.name, args.target)

    def connect(self, db_name, task_name):
        self.use_context = False
        task_mod = self.client.get_module('task')
        env = {
            'DATABASE_URL': self.get_url(db_name)
        }
        task_mod.env_set(task_name, env)

    def handle_select(self, args):
        self.select(args.name, args.show)

    def select(self, name, show=False):
        if show:
            sel = self.get_selected(fail=False)
            if sel:
                print(sel)
        else:
            if name:
                self.use_context = False
                ctx = self.get_context()
                pgpass_path = f'{ctx["cluster"]}/{ctx["app"]}/{name}.pgpass'
                # self.exists(name)
                self.get_secure_file(pgpass_path)
                self.get_pgpass_file(name)
                self.store['selected'] = name
            else:
                sel = self.store.get('selected', None)
                if sel:
                    self.use_context = False
                    ctx = self.get_context()
                    pgpass_path = f'{ctx["cluster"]}/{ctx["app"]}/{sel}.pgpass'
                    self.clear_secure_file(pgpass_path)
                try:
                    del self.store['selected']
                except KeyError:
                    pass
            self.clear_parent_selections()

    def handle_psql(self, args):
        self.psql(args.command)

    def psql(self, command=None):
        ctx = self.get_context()
        name = ctx['db']
        path = os.path.join(self.get_rc_path(), f'{name}.pgpass')
        endpoint = self.get_endpoint(name)
        cmd = 'psql -h {} -p {} -U {} -d {}'.format(
            endpoint['Address'],
            endpoint['Port'],
            name,
            name
        )
        if command:
            cmd = f'{cmd} -c "{self.escape(command)}"'
        self.run(
            cmd,
            capture=False,
            env={'PGPASSFILE': path}
        )

    def handle_dump(self, args):
        self.dump(args.output)

    def dump(self, output):
        ctx = self.get_context()
        app = self.client.get_selected('app')
        name = self.get_selected()
        path = os.path.join(self.get_rc_path(), '%s.pgpass' % name)
        endpoint = self.get_endpoint(name)
        self.run(
            'pg_dump -Fc --no-acl --no-owner -h {} -p {} -U {} -d {} -f {}'.format(
                endpoint['Address'],
                endpoint['Port'],
                name,
                name,
                output
            ),
            capture=False,
            env={'PGPASSFILE': path}
        )

    def handle_restore(self, args):
        ctx = self.get_context()
        name = self.get_selected()
        path = os.path.join(self.get_rc_path(), '%s.pgpass' % name)
        endpoint = self.get_endpoint(name)
        self.run(
            f'psql -h {endpoint["Address"]} -p {endpoint["Port"]} -U {name} {name} -c \'DROP SCHEMA public CASCADE; CREATE SCHEMA public;\'',
            env={'PGPASSFILE': path}
        )
        self.run(
            f'pg_restore --no-acl --no-owner -h {endpoint["Address"]} -p {endpoint["Port"]} -U {name} -d {name} {args.input}',
            capture=False,
            env={'PGPASSFILE': path}
        )

    def get_id(self, name):
        ctx = self.get_context()
        return f'fuku-{ctx["cluster"]}-{ctx["app"]}-{name}'

    def get_rc_path(self):
        ctx = self.get_context()
        return os.path.join(get_rc_path(), ctx['cluster'], ctx['app'])

    def get_endpoint(self, name):
        ctx = self.get_context()
        inst_id = self.get_id(name)
        rds = self.get_boto_client('rds')
        try:
            return rds.describe_db_instances(
                DBInstanceIdentifier=inst_id
            )['DBInstances'][0]['Endpoint']
        except:
            self.error(f'no database "{name}"')

    def get_url(self, name):
        ctx = self.get_context()
        path = os.path.join(self.get_rc_path(), f'{name}.pgpass')
        try:
            with open(path, 'r') as inf:
                data = inf.read()
        except:
            self.error(f'no cached information for "{name}"')
        host, port, db, user, pw = data.split(':')
        return 'postgres://{}:{}@{}:{}/{}'.format(user, pw, host, port, db)

    def get_pgpass_file(self, name):
        ctx = self.get_context()
        app = self.client.get_selected('app')
        path = os.path.join(self.get_rc_path(), '%s.pgpass' % name)
        if not os.path.exists(path):
            self.run(
                '$aws s3 cp s3://$bucket/fuku/{}/{}/{}.pgpass.gpg {}.gpg'.format(app, ctx['cluster'], name, path)
            )
            self.run(
                'gpg -o {} -d {}.gpg'.format(path, path)
            )
            os.chmod(path, 0o600)

    def get_selected(self, fail=True):
        sel = self.store.get('selected', None)
        if not sel and fail:
            self.error('no postgres DB selected')
        return sel

    def get_my_context(self):
        sel = self.store_get('selected')
        if not sel:
            self.error('no DB currently selected')
        return {
            'db': sel
        }
