import os
import re
import json
import stat

import boto3

from .module import Module
from .utils import entity_already_exists, EntityAlreadyExists
from .db import get_rc_path


ARN_PROG = re.compile(r'[^/]*/fuku-(.+)')


class Cluster(Module):
    dependencies = ['region']

    def __init__(self, **kwargs):
        super().__init__('cluster', **kwargs)

    def add_arguments(self, parser):
        subp = parser.add_subparsers(help='cluster help')

        p = subp.add_parser('ls', help='list clusters')
        p.add_argument('name', metavar='NAME', nargs='?', help='cluster name')
        p.set_defaults(cluster_handler=self.handle_list)

        p = subp.add_parser('mk', help='make a cluster')
        p.add_argument('name', metavar='NAME', help='cluster name')
        p.set_defaults(cluster_handler=self.handle_make)

        p = subp.add_parser('sl', help='select a cluster')
        p.add_argument('name', metavar='NAME', help='cluster name')
        p.set_defaults(cluster_handler=self.handle_select)

        p = subp.add_parser('up', help='update a cluster')
        p.add_argument('name', metavar='NAME', help='cluster name')
        p.add_argument('--pem', '-p', help='PEM file')
        p.set_defaults(cluster_handler=self.handle_update)

    def handle_list(self, args):
        self.list(args.name)

    def list(self, name):
        for cl in self.iter_clusters():
            print(cl)

    def handle_make(self, args):
        self.make(args.name)

    def make(self, name):
        self.validate(name)
        ecs = self.get_boto_client('ecs')
        res = ecs.create_cluster(
            clusterName=f'fuku-{name}'
        )
        ec2 = self.get_boto_client('ec2')
        self.create_key_pair(name, ec2=ec2)
        self.create_security_group(name, ec2=ec2)
        self.create_log_group(name)

    def handle_update(self, args):
        self.update(args.name, args.pem)

    def update(self, name, pem):
        if pem:
            self.add_pem(name, pem_fn=pem)

    def handle_select(self, args):
        self.select(args.name)

    def select(self, name):
        if name and name not in list(self.iter_clusters()):
            self.error(f'no cluster "{name}"')
        self.store_set('selected', name)
        self.clear_parent_selections()

    def iter_clusters(self):
        ecs = self.get_boto_client('ecs')
        for cl in ecs.list_clusters()['clusterArns']:
            m = ARN_PROG.match(cl)
            if not m:
                continue
            yield m.group(1)

    def create_key_pair(self, name, ec2=None):
        if ec2 is None:
            ec2 = self.get_boto_client('ec2')
        try:
            with entity_already_exists(hide=False):
                key = ec2.create_key_pair(
                    KeyName=f'fuku-{name}'
                )['KeyMaterial']
        except EntityAlreadyExists:
            print('key-pair already exists, add PEM file manually')
            return
        self.add_pem(name, pem_key=key)

    def add_pem(self, name, pem_key=None, pem_fn=None):
        ctx = self.get_context()
        path = os.path.join(get_rc_path(), name, 'key.pem')
        try:
            os.makedirs(os.path.dirname(path))
        except OSError:
            pass
        if pem_fn:
            with open(pem_fn, 'r') as pem_f:
                pem_key = pem_f.read()
        with open(path, 'w') as keyf:
            keyf.write(pem_key)
        os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
        self.run(
            'gpg -c {}'.format(path)
        )
        s3 = self.get_boto_client('s3')
        s3.upload_file(f'{path}.gpg', ctx['bucket'], f'fuku/{ctx["cluster"]}/key.pem')

    def create_security_group(self, name, ec2=None):
        if ec2 is None:
            ec2 = self.get_boto_client('ec2')
        user_id = self.client.get_module('profile').get_user_id()
        sg_id = None
        with entity_already_exists():
            sg_id = ec2.create_security_group(
                GroupName=f'fuku-{name}',
                Description=f'{name} security group'
            )['GroupId']
        if sg_id is None:
            sg_id = ec2.describe_security_groups(
                GroupNames=[f'fuku-{name}']
            )['SecurityGroups'][0]['GroupId']
        with entity_already_exists():
            ec2.authorize_security_group_ingress(
                GroupName=f'fuku-{name}',
                IpProtocol='tcp',
                FromPort=22,
                ToPort=22,
                CidrIp='0.0.0.0/0'
            )
        with entity_already_exists():
            ec2.authorize_security_group_ingress(
                GroupName=f'fuku-{name}',
                IpProtocol='tcp',
                FromPort=80,
                ToPort=80,
                CidrIp='0.0.0.0/0'
            )
        with entity_already_exists():
            ec2.authorize_security_group_ingress(
                GroupName=f'fuku-{name}',
                IpProtocol='tcp',
                FromPort=443,
                ToPort=443,
                CidrIp='0.0.0.0/0'
            )
        with entity_already_exists():
            ec2.authorize_security_group_ingress(
                GroupName=f'fuku-{name}',
                IpProtocol='tcp',
                FromPort=5432,
                ToPort=5432,
                CidrIp='0.0.0.0/0'
            )
        with entity_already_exists():
            ec2.authorize_security_group_ingress(
                GroupName=f'fuku-{name}',
                IpPermissions=[{
                    'IpProtocol': 'tcp',
                    'FromPort': 0,
                    'ToPort': 65535,
                    'UserIdGroupPairs': [{
                        'UserId': user_id,
                        'GroupId': sg_id
                    }]
                }]
            )
        return id

    def get_security_group_id(self, name=None):
        name = name or self.store_get('selected')
        ec2 = self.get_boto_client('ec2')
        try:
            return ec2.describe_security_groups(
                GroupNames=[f'fuku-{name}']
            )['SecurityGroups'][0]['GroupId']
        except (KeyError, IndexError):
            self.error(f'security group for "{name}" does not exist')

    def create_log_group(self, name):
        logs = self.get_boto_client('logs')
        with entity_already_exists():
            logs.create_log_group(
                logGroupName=f'/{name}'
            )

    def get_my_context(self):
        ctx = {}
        ctx['cluster'] = self.get_selected()
        ctx['pem'] = os.path.join(get_rc_path(), ctx['cluster'], 'key.pem')
        return ctx

    def get_selected(self):
        sel = self.store_get('selected')
        if not sel:
            self.error('no cluster currently selected')
        return sel
