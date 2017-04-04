import re
import json

from .module import Module


class Node(Module):
    dependencies = ['cluster']
    ami_map = {
      # 'ap-southeast-2': 'ami-862211e5',  # docker enabled amazon
        'ap-southeast-2': 'ami-e7878484',  # arch linux
    }

    def __init__(self, **kwargs):
        super().__init__('node', **kwargs)

    def add_arguments(self, parser):
        subp = parser.add_subparsers(help='node help')

        p = subp.add_parser('ls', help='list nodes')
        p.add_argument('name', metavar='NAME', nargs='?', help='node name')
        p.set_defaults(node_handler=self.handle_list)

        p = subp.add_parser('mk', help='make a node')
        p.add_argument('name', metavar='NAME', help='node name')
        p.add_argument('--manager', '-m', action='store_true', help='manager node')
        p.set_defaults(node_handler=self.handle_make)

        p = subp.add_parser('ssh', help='SSH to a node')
        p.add_argument('name', metavar='NAME', nargs='?', help='node name')
        p.set_defaults(node_handler=self.handle_ssh)

    def handle_list(self, args):
        self.list(args.name)

    def list(self, name):
        for inst in self.iter_instances():
            import pdb; pdb.set_trace()
            print('h')

    def handle_make(self, args):
        self.make(args.name, args.manager)

    def make(self, name, manager=False):
        self.validate(name)
        existing = [i['name'] for i in self.iter_instances()]
        if name in existing:
            self.error(f'node "{name}" already exists')
        if not len(existing) and not manager:
            self.error(f'must have at least one manager before workers')
        ctx = self.get_context({'node': name})
        ec2 = self.get_boto_client('ec2')
        image = self.ami_map[ctx['region']]
        sg_id = self.client.get_module('cluster').get_security_group_id()
        with self.template_file('arch-user-data.sh', ctx) as user_data:
            inst = ec2.run_instances(
                ImageId=image,
                KeyName=f'fuku-{ctx["cluster"]}',
                SecurityGroupIds=[sg_id],
                UserData=user_data,
                InstanceType='t2.micro',
                IamInstanceProfile={
                    'Name': 'ec2-profile'
                },
                MinCount=1,
                MaxCount=1
            )
        inst_id = inst['Instances'][0]['InstanceId']
        self.tag_instance(inst_id, name, manager, ec2=ec2, ctx=ctx)
        # self.init_manager

    def handle_ssh(self, args):
        self.ssh_run('', args.name)

    def tag_instance(self, inst_id, name, manager=False, ec2=None, ctx=None):
        if ctx is None:
            ctx = self.get_context()
        if ec2 is None:
            ec2 = self.get_boto_resource('ec2')
        ec2.create_tags(
            Resources=[inst_id],
            Tags=[
                {'Key': 'Name', 'Value': f'{ctx["cluster"]}-{name}'},
                {'Key': 'name', 'Value': name},
                {'Key': 'cluster', 'Value': ctx['cluster']},
                {'Key': 'node', 'Value': 'manager' if manager else 'worker'}
            ]
        )

    def iter_instances(self):
        ec2 = self.get_boto_resource('ec2')
        cluster = self.client.get_selected('cluster')
        filters = [
            {
                'Name': 'tag:app',
                'Values': [cluster]
            },
            {
                'Name': 'instance-state-name',
                'Values': ['pending', 'running', 'shutting-down', 'stopping', 'stopped']
            }
        ]
        for inst in ec2.instances.filter(Filters=filters):
            import pdb; pdb.set_trace()
            yield inst

    def get_instance(self, name):
        ec2 = self.get_boto_resource('ec2')
        cluster = self.client.get_selected('cluster')
        filters = [
            {
                'Name': 'tag:cluster',
                'Values': [cluster]
            },
            {
                'Name': 'tag:name',
                'Values': [name]
            },
            {
                'Name': 'instance-state-name',
                'Values': ['pending', 'running', 'shutting-down', 'stopping', 'stopped']
            }
        ]
        insts = list(ec2.instances.filter(Filters=filters))
        if not len(insts):
            self.error(f'no node in cluster "{cluster}" with name "{name}"')
        return insts[0]

    def ssh_run(self, cmd, name=None, tty=False, capture=None):
        ctx = self.get_context()
        name = name or ctx['node']
        inst = self.get_instance(name)
        ip = inst.public_ip_address
        full_cmd = f'ssh{" -t" if tty else ""} -o "StrictHostKeyChecking no" -i "{ctx["pem"]}" root@{ip} {cmd}'
        return self.run(full_cmd, capture=capture)

    def get_my_context(self):
        ctx = {}
        node = self.store_get('selected')
        if node is not None:
            ctx['node'] = node
        return ctx
