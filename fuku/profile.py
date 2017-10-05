import os
from configparser import ConfigParser

import boto3

from .module import Module
from .utils import entity_already_exists, limit_exceeded


class Profile(Module):
    def __init__(self, **kwargs):
        super().__init__('profile', **kwargs)
        self.aws_path = os.path.expanduser('~/.aws/credentials')

    def add_arguments(self, parser):
        subp = parser.add_subparsers(help='profile help')

        p = subp.add_parser('ls', help='list profiles')
        p.add_argument('name', metavar='NAME', nargs='?', help='profile name')
        p.set_defaults(profile_handler=self.handle_list)

        # p = subp.add_parser('mk', help='make a new profile')
        # p.add_argument('name', metavar='NAME', help='profile name')
        # p.set_defaults(profile_handler=self.handle_make)

        # p = subp.add_parser('rm', help='remove a profile')
        # p.add_argument('name', metavar='NAME', help='profile name')
        # p.set_defaults(profile_handler=self.handle_remove)

        p = subp.add_parser('bucket', help='set FUKU bucket')
        p.add_argument('name', metavar='NAME', help='bucket name')
        p.set_defaults(profile_handler=self.handle_bucket)

        p = subp.add_parser('sl', help='select a profile')
        p.add_argument('name', metavar='NAME', help='profile name')
        p.set_defaults(profile_handler=self.handle_select)

        p = subp.add_parser('sh', help='show selected profile')
        p.set_defaults(profile_handler=self.handle_show)

    def handle_list(self, args):
        self.list(args.name)

    def list(self, name):
        for prof in self.list_local_profiles():
            if not name or name == prof:
                print(prof)

    def handle_make(self, args):
        self.make(args.name)

    def make(self, name):
        self.use_context = False
        if name not in self.list_local_profiles():
            self.error(f'please create a "{name}" profile using AWS CLI then rerun')
        self.create_ec2_role(name)

    def handle_select(self, args):
        self.select(args.name)

    def select(self, name):
        if name and name not in self.list_local_profiles():
            self.error(f'no profile named "{name}"')
        self.store_set('selected', name)
        self.clear_parent_selections()

    def handle_show(self, args):
        self.show()

    def show(self):
        sel = self.store_get('selected')
        if sel:
            print(sel)

    def handle_bucket(self, args):
        self.bucket(args.name)

    def bucket(self, name):
        s3 = self.get_boto_resource('s3', {'profile': self.get_selected()})
        bucket = s3.Bucket(name)
        try:
            bucket.load()
        except:
            bucket.create()
        self.store_set('bucket', name)

    def list_local_profiles(self):
        cfg = ConfigParser()
        cfg.read(self.aws_path)
        return cfg.sections()

    def create_ec2_role(self, user):
        role_name = 'ec2-role'
        inst_name = 'ec2-profile'
        iam = boto3.Session(profile_name=user).client('iam')
        self.create_role(user, role_name, ['ec2-policy'], iam=iam)
        with entity_already_exists():
            iam.create_instance_profile(
                InstanceProfileName=inst_name
            )
        # TODO: Add AmazonEC2ContainerServiceforEC2Role policy to role
        with limit_exceeded():
            iam.add_role_to_instance_profile(
                InstanceProfileName=inst_name,
                RoleName=role_name
            )

    def create_role(self, user, name, policies=[], iam=None):
        if iam is None:
            iam = boto3.Session(profile_name=user).client('iam')
        with entity_already_exists():
            iam.create_role(
                RoleName=name,
                AssumeRolePolicyDocument=f'file://{self.data_path("ecs-assume-role.json")}'
            )
        for policy in policies:
            ctx = self.get_context({
                'name': name,
            })
            with self.template_file(f'{policy}.json', ctx) as policy_file:
                iam.put_role_policy(
                    RoleName=name,
                    PolicyName=policy,
                    PolicyDocument=policy_file
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

    def get_user_id(self):
        # profile = self.get_selected()
        sts = self.get_boto_client('sts')
        try:
            return sts.get_caller_identity()['Account']
        except:
            return None

    def get_my_context(self):
        ctx = {}
        sel = self.get_selected()
        if sel:
            ctx['profile'] = sel
        if 'bucket' not in self.store:
            self.error('bucket not set')
        ctx['bucket'] = self.store['bucket']
        return ctx

    def get_selected(self):
        sel = self.store_get('selected')
        if not sel:
            self.error('no profile currently selected')
        return sel
