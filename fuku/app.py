import os
import stat

from .module import Module
from .db import get_rc_path


class App(Module):
    dependencies = ['region']

    def __init__(self, **kwargs):
        super().__init__('app', **kwargs)

    def add_arguments(self, parser):
        subp = parser.add_subparsers(help='app help')

        addp = subp.add_parser('list', help='list applications')
        addp.set_defaults(app_handler=self.list)

        addp = subp.add_parser('add', help='add an app')
        addp.add_argument('name', help='app name')
        addp.set_defaults(app_handler=self.add)

        remp = subp.add_parser('remove', help='remove an app')
        remp.add_argument('name', help='app name')
        remp.set_defaults(app_handler=self.remove)

        selp = subp.add_parser('select', help='select an app')
        selp.add_argument('name', nargs='?', help='app name')
        selp.add_argument('--show', '-s', action='store_true', help='show currently selected')
        selp.set_defaults(app_handler=self.select)

    def create_app_group(self, name):
        self.run(
            '$aws iam create-group'
            ' --path /fuku/'
            ' --group-name fuku-$app',
            {'app': name}
        )

    def delete_app_group(self, name):
        self.run(
            '$aws iam delete-group'
            ' --group-name fuku-$app',
            {'app': name}
        )

    def list_app_groups(self):
        data = self.run(
            '$aws iam list-groups'
            ' --path-prefix /fuku/'
            ' --query \'Groups[*].GroupName\'',
            capture='json'
        )
        return [d[5:] for d in data]

    def create_security_group(self, name):
        sg_id = self.run(
            '$aws ec2 create-security-group'
            ' --group-name fuku-$app'
            ' --description "$app security group"'
            ' --query \'GroupId\'',
            {'app': name},
            capture='json'
        )
        self.run(
            '$aws ec2 authorize-security-group-ingress'
            ' --group-name fuku-$app'
            ' --protocol tcp'
            ' --port 22'
            ' --cidr 0.0.0.0/0',
            {'app': name}
        )
        self.run(
            '$aws ec2 authorize-security-group-ingress'
            ' --group-name fuku-$app'
            ' --protocol tcp'
            ' --port 80'
            ' --cidr 0.0.0.0/0',
            {'app': name}
        )
        self.run(
            '$aws ec2 authorize-security-group-ingress'
            ' --group-name fuku-$app'
            ' --protocol tcp'
            ' --port 443'
            ' --cidr 0.0.0.0/0',
            {'app': name}
        )
        self.run(
            '$aws ec2 authorize-security-group-ingress'
            ' --group-name fuku-$app'
            ' --protocol tcp'
            ' --port 6379'
            ' --cidr 0.0.0.0/0',
            {'app': name}
        )
        self.run(
            '$aws ec2 authorize-security-group-ingress'
            ' --group-name fuku-$app'
            ' --protocol tcp'
            ' --port 5432'
            ' --cidr 0.0.0.0/0',
            {'app': name}
        )
        return sg_id

    def delete_security_group(self, name):
        self.run(
            '$aws ec2 delete-security-group'
            ' --group-name fuku-$app',
            {'app': name}
        )

    def get_security_group_id(self, app):
        sg_id = self.run(
            '$aws ec2 describe-security-groups'
            ' --group-names fuku-$app'
            ' --query \'SecurityGroups[0].GroupId\'',
            {'app': app},
            capture='json'
        )
        if not sg_id:
            self.error('no security group')
        return sg_id

    def create_key_pair(self, name):
        data = self.run(
            '$aws ec2 create-key-pair'
            ' --key-name fuku-$app',
            {'app': name},
            capture='json'
        )
        path = os.path.join(get_rc_path(), name, 'key.pem')
        try:
            os.makedirs(os.path.dirname(path))
        except OSError:
            pass
        with open(path, 'w') as keyf:
            keyf.write(data['KeyMaterial'])
        os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
        self.run(
            'gpg -c {}'.format(path)
        )
        self.run(
            '$aws s3 cp {}.gpg s3://$bucket/fuku/{}/key.pem.gpg'.format(path, name)
        )

    def get_key_file(self, name):
        path = os.path.join(get_rc_path(), name, 'key.pem')
        if not os.path.exists(path):
            self.run(
                '$aws s3 cp s3://$bucket/fuku/{}/key.pem.gpg {}.gpg'.format(name, path)
            )
            self.run(
                'gpg -o {} -d {}.gpg'.format(path, path)
            )
            os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)

    def delete_key_pair(self, name):
        self.run(
            '$aws ec2 delete-key-pair'
            ' --key-name fuku-$app',
            {'app': name}
        )
        self.run(
            '$aws s3 rm s3://$bucket/fuku/{}/key.pem.gpg'.format(name),
            ignore_errors=True
        )

    def create_cluster(self, name):
        self.run(
            '$aws ecs create-cluster'
            ' --cluster-name {}'.format(name)
        )

    def delete_cluster(self, name):
        self.run(
            '$aws ecs delete-cluster'
            ' --cluster {}'.format(name)
        )

    def config(self):
        cfg = {}
        app = self.get_selected()
        cfg['app'] = app
        cfg['pem'] = os.path.join(get_rc_path(), app, 'key.pem')
        cfg.update(self.store['apps'][app])
        return cfg

    def list(self, args):
        apps = self.list_app_groups()
        for name in apps:
            print(name)

    def add(self, args):
        name = args.name
        self.create_app_group(name)
        self.create_key_pair(name)
        self.create_security_group(name)
        # self.create_cluster(name)

    def remove(self, args):
        name = args.name
        app = self.store.get('selected', None)
        if app == name:
            del self.store['selected']
        try:
            del self.store.get('apps', {})[name]
        except KeyError:
            pass
        self.delete_security_group(name)
        self.delete_app_group(name)
        self.delete_key_pair(name)
        # self.delete_cluster(name)

    def select(self, args):
        if args.show:
            sel = self.get_selected(fail=False)
            if sel:
                print(sel)
        else:
            name = args.name
            if name:
                self.exists(name)
                data = self.store.setdefault('apps', {}).setdefault(name, {})
                data['security_group'] = self.get_security_group_id(name)
                self.get_key_file(name)
                self.store['selected'] = name
            else:
                try:
                    del self.store['selected']
                except KeyError:
                    pass
            self.clear_parent_selections()

    def exists(self, name):
        apps = self.list_app_groups()
        if name not in apps:
            self.error('app does not exist')

    def get_selected(self, fail=True):
        sel = self.store.get('selected', None)
        if not sel and fail:
            self.error('no app selected')
        return sel
