import os
import json

from .module import Module
from .db import get_rc_path
from .utils import gen_secret


class Postgres(Module):
    dependencies = ['app']

    def __init__(self, **kwargs):
        super().__init__('postgres', **kwargs)

    def add_arguments(self, parser):
        subp = parser.add_subparsers(help='postgres help')

        p = subp.add_parser('list', help='list DBs')
        p.add_argument('name', nargs='?')
        p.set_defaults(postgres_handler=self.handle_list)

        p = subp.add_parser('add', help='add a postgres database')
        p.add_argument('name')
        p.set_defaults(postgres_handler=self.handle_add)

        p = subp.add_parser('connect', help='connect to a task')
        p.add_argument('target')
        p.set_defaults(postgres_handler=self.handle_connect)

        p = subp.add_parser('select', help='select a postgres database')
        p.add_argument('name', nargs='?')
        p.add_argument('--show', '-s', action='store_true', help='show currently db')
        p.set_defaults(postgres_handler=self.handle_select)

        p = subp.add_parser('psql')
        p.set_defaults(postgres_handler=self.handle_psql)

    def handle_list(self, args):
        app = self.client.get_selected('app')
        if args.name:
            data = self.run(
                '$aws rds describe-db-instances'
                ' --filter Name=db-instance-id,Values={}-{}'
                ' --query DBInstances[0]'
                .format(
                    app,
                    args.name
                ),
                capture='json'
            )
            print(json.dumps(data, indent=2))
        else:
            data = self.run(
                '$aws rds describe-db-instances'
#                ' --filter Name=db-instance-id,Values={}-*'
                ' --query DBInstances[*].DBInstanceIdentifier'
                .format(
                    app,
                    args.name
                ),
                capture='json'
            )
            pre = '%s-' % app
            for name in data:
                if name.startswith(pre):
                    print(name[len(pre):])

    def handle_add(self, args):
        app = self.client.get_selected('app')
        password = gen_secret(16)
        inst_id = '%s-%s' % (app, args.name)
        self.run(
            '$aws rds create-db-instance'
            ' --db-name $name'
            ' --db-instance-identifier $inst_id'
            ' --db-instance-class db.t2.micro'
            ' --engine postgres'
            ' --allocated-storage 5'
            ' --master-username $name'
            ' --master-user-password $password'
            ' --backup-retention-period 0'
            ' --vpc-security-group-ids $security_group'
            ' --tags Key=app,Value=$app'
            ' --query DBInstance',
            {
                'name': args.name,
                'inst_id': inst_id,
                'password': password
            }
        )
        self.run(
            '$aws rds wait db-instance-available'
            ' --db-instance-identifier {}'.format(inst_id)
        )
        data = self.get_endpoint(args.name)
        path = os.path.join(get_rc_path(), app, '%s.pgpass' % args.name)
        try:
            os.makedirs(os.path.dirname(path))
        except OSError:
            pass
        with open(path, 'w') as outf:
            outf.write('{}:{}:{}:{}:{}'.format(
                data['Address'],
                data['Port'],
                args.name,
                args.name,
                password
            ))
        os.chmod(path, 0o600)
        self.run(
            'gpg -c {}'.format(path)
        )
        self.run(
            '$aws s3 cp {}.gpg s3://$bucket/fuku/{}/{}.pgpass.gpg'.format(path, app, args.name)
        )

    def handle_connect(self, args):
        task_mod = self.client.get_module('task')
        env = {
            'DATABASE_URL': self.get_url()
        }
        task_mod.env_set(args.target, env)

    def handle_select(self, args):
        if args.show:
            sel = self.get_selected(fail=False)
            if sel:
                print(sel)
        else:
            name = args.name
            if name:
                # self.exists(name)
                self.get_pgpass_file(name)
                self.store['selected'] = name
            else:
                try:
                    del self.store['selected']
                except KeyError:
                    pass
            self.clear_parent_selections()

    def handle_psql(self, args):
        app = self.client.get_selected('app')
        name = self.get_selected()
        path = os.path.join(get_rc_path(), app, '%s.pgpass' % name)
        endpoint = self.get_endpoint(name)
        self.run(
            'psql -h {} -p {} -U {} -d {}'.format(
                endpoint['Address'],
                endpoint['Port'],
                name,
                name
            ),
            capture=False,
            env={'PGPASSFILE': path}
        )

    def get_endpoint(self, name):
        app = self.client.get_selected('app')
        inst_id = '%s-%s' % (app, name)
        data = self.run(
            '$aws rds describe-db-instances'
            ' --db-instance-identifier {}'
            ' --query DBInstances[0].Endpoint'.format(inst_id),
            capture='json'
        )
        return data

    def get_url(self):
        app = self.client.get_selected('app')
        name = self.get_selected()
        path = os.path.join(get_rc_path(), app, '%s.pgpass' % name)
        with open(path, 'r') as inf:
            data = inf.read()
        host, port, db, user, pw = data.split(':')
        return 'postgres://{}:{}@{}:{}/{}'.format(user, pw, host, port, db)

    def get_pgpass_file(self, name):
        app = self.client.get_selected('app')
        path = os.path.join(get_rc_path(), app, '%s.pgpass' % name)
        if not os.path.exists(path):
            self.run(
                '$aws s3 cp s3://$bucket/fuku/{}/{}.pgpass.gpg {}.gpg'.format(app, name, path)
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
