import os
from configparser import ConfigParser

from .module import Module


class Profile(Module):
    def __init__(self, **kwargs):
        super().__init__('profile', **kwargs)
        self.aws_path = os.path.expanduser('~/.aws/credentials')

    def add_arguments(self, parser):
        subp = parser.add_subparsers(help='profile help')

        p = subp.add_parser('configure', help='configuration')
        p.add_argument('--bucket', '-b', help='app cache bucket')
        p.set_defaults(profile_handler=self.configure)

        addp = subp.add_parser('list', help='list profiles')
        addp.set_defaults(profile_handler=self.list)

        addp = subp.add_parser('add', help='add a profile')
        addp.add_argument('name', help='profile name')
        addp.set_defaults(profile_handler=self.add)

        remp = subp.add_parser('remove', help='remove a profile')
        remp.add_argument('name', help='profile name')
        remp.set_defaults(profile_handler=self.remove)

        selp = subp.add_parser('select', help='select a profile')
        selp.add_argument('name', help='profile name')
        selp.set_defaults(profile_handler=self.select)

    def config(self):
        cfg = {}
        sel = self.get_selected()
        if 'bucket' not in self.store:
            self.error('bucket not set')
        cfg['profile'] = sel
        cfg['aws'] = '$aws --profile $profile'
        cfg['bucket'] = self.store['bucket']
        return cfg

    def create_role(self, user, name, policies=[]):
        self.run(
            '$aws iam create-role'
            ' --profile {user}'
            ' --role-name {name}'
            ' --assume-role-policy-document'
            ' file://{file}'.format(
                user=user,
                name=name,
                file=self.data_path('ecs-assume-role.json')
            ),
        )
        for policy in policies:
            ctx = self.merged_config({'name': name})
            with self.template_file('%s.json' % policy, ctx) as policy_file:
                self.run(
                    '$aws iam put-role-policy '
                    ' --profile {user}'
                    ' --role-name {name}'
                    ' --policy-name {policy}'
                    ' --policy-document file://{policy_file}'.format(
                        user=user,
                        name=name,
                        policy=policy,
                        policy_file=policy_file
                    )
                )

    def delete_role(self, user, role):
        self.run(
            '$aws iam delete-role'
            ' --profile {user}'
            ' --role-name {role}'.format(
                user=user,
                role=role
            )
        )

    def create_ec2_role(self, user):
        role_name = 'ec2-role'
        inst_name = 'ec2-profile'
        self.create_role(user, role_name, ['ec2-policy'])
        self.run(
            '$aws iam create-instance-profile '
            ' --profile {user}'
            ' --instance-profile-name {inst_name}'.format(
                user=user,
                inst_name=inst_name
            )
        )
        self.run(
            '$aws iam add-role-to-instance-profile'
            ' --profile {user}'
            ' --instance-profile-name {inst_name}'
            ' --role-name {role_name}'.format(
                user=user,
                inst_name=inst_name,
                role_name=role_name
            ),
            use_self=True
        )

    def delete_ec2_role(self, user):
        role_name = 'ec2-role'
        inst_name = 'ec2-profile'
        self.run(
            '$aws iam delete-instance-profile'
            ' --profile {user}'
            ' --instance-profile-name {inst_name}'.format(
                user=user,
                inst_name=inst_name
            )
        )
        self.delete_role(user, role_name)

    def configure(self, args):
        if args.bucket:
            bucket = args.bucket
            self.store['bucket'] = bucket

    def list(self, args):
        cfg = ConfigParser()
        cfg.read(self.aws_path)
        print(cfg.sections())

    def add(self, args):
        name = args.name
        # self.run('aws configure --profile {}'.format(name))
        self.create_ec2_role(name)

    def remove(self, args):
        name = args.name
        # TODO: Remove profile
        self.delete_ec2_role(name)

    def select(self, args):
        name = args.name
        cfg = ConfigParser()
        cfg.read(self.aws_path)
        if name not in cfg.sections():
            self.error('no profile named "{}"'.format(name))
        self.store['selected'] = name
        self.clear_parent_selections()

    def get_selected(self, fail=True):
        try:
            sel = self.store['selected']
        except KeyError:
            sel = None
        if not sel and fail:
            self.error('no profile currently selected')
        return sel
