import base64
import json
import re

from .module import Module
from .utils import StoreKeyValuePair, json_serial


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
        p.add_argument('--manager', '-m', action='store_true', help='manager node (Swarm)')
        p.add_argument('--availability-zone', '-a', choices=['a', 'b'], help='availability zone')
        p.add_argument('--type', '-t', default='t2.micro', help='instance type')
        p.set_defaults(node_handler=self.handle_make)

        p = subp.add_parser('putattr', help='add attribute')
        p.add_argument('name', metavar='NAME', help='node name')
        p.add_argument('values', metavar='VALUES', action=StoreKeyValuePair, nargs='*', help='attributes')
        p.set_defaults(node_handler=self.handle_put_attributes)

        p = subp.add_parser('rm', help='remove a node')
        p.add_argument('name', metavar='NAME', help='node name')
        p.set_defaults(node_handler=self.handle_remove)

        p = subp.add_parser('bastion', help='make a bastion node')
        p.set_defaults(node_handler=self.handle_bastion)

        p = subp.add_parser('ssh', help='SSH to a node')
        p.add_argument('name', metavar='NAME', nargs='?', help='node name')
        p.set_defaults(node_handler=self.handle_ssh)

        p = subp.add_parser('init', help='initialise swarm')
        p.add_argument('name', metavar='NAME', help='node name')
        p.set_defaults(node_handler=self.handle_init_swarm)

        p = subp.add_parser('join', help='join swarm')
        p.add_argument('name', metavar='NAME', help='node name')
        p.set_defaults(node_handler=self.handle_join_swarm)

        p = subp.add_parser('wait', help='wait for OK status')
        p.add_argument('name', metavar='NAME', help='node name')
        p.set_defaults(node_handler=self.handle_wait)

        p = subp.add_parser('reboot', help='reboot a node')
        p.add_argument('name', metavar='NAME', help='node name')
        p.set_defaults(node_handler=self.handle_reboot)

    def handle_list(self, args):
        self.list(args.name)

    def list(self, name):
        if name:
            ec2_cli = self.get_boto_client('ec2')
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
            inst = ec2_cli.describe_instances(Filters=filters)['Reservations'][0]['Instances'][0]
            print(json.dumps(inst, default=json_serial, indent=2))
        else:
            for name in self.get_instance_names():
                print(name)

    def handle_make(self, args):
        self.make(args.name, args.manager, args.type)

    def make(self, name, manager, type):
        self.validate(name)
        existing = self.get_instance_names()
        if name in existing:
            self.error(f'node "{name}" already exists')
        if not len(existing) and not manager:
            self.error(f'must have at least one manager before workers')
        ctx = self.get_context({'node': name})
        ec2 = self.get_boto_client('ec2')
        image = self.ami_map[ctx['region']]
        sg_id = self.get_module('cluster').get_security_group_id()
        sn = self.get_module('cluster').get_private_subnet()
        opts = {
            'ImageId': image,
            'SubnetId': sn.id,
            'KeyName': f'fuku-{ctx["cluster"]}',
            'SecurityGroupIds': [sg_id],
            'InstanceType': type,
            'IamInstanceProfile': {
                'Name': 'ec2-profile'
            },
            'MinCount': 1,
            'MaxCount': 1
        }
        with self.template_file('arch-user-data.sh', ctx) as user_data:
            opts['UserData'] = user_data
            inst = ec2.run_instances(**opts)
        inst_id = inst['Instances'][0]['InstanceId']
        self.tag_instance(inst_id, name, manager, ec2=ec2, ctx=ctx)
        self.wait(name)
        if manager:
            self.init_swarm(name)

    def handle_put_attributes(self, args):
        self.put_attribute(args.name, args.values)

    def put_attribute(self, name, values):
        ctx = self.get_context()
        ecs = self.get_boto_client('ecs')
        arn = self.get_instance_arn(name)
        attrs = []
        for k, v in values.items():
            attrs.append({
                'name': k,
                'value': v,
                'targetType': 'container-instance',
                'targetId': arn
            })
        ecs.put_attributes(
            cluster=f'fuku-{ctx["cluster"]}',
            attributes=attrs
        )

    def handle_remove(self, args):
        self.remove(args.name)

    def remove(self, name):
        inst = self.get_instance(name)
        inst.terminate()

    def handle_init_swarm(self, args):
        self.init_swarm(args.name)

    def init_swarm(self, name):
        inst = self.get_instance(name)
        ip = inst.private_ip_address
        resp = self.ssh_run(
            f'docker swarm init --advertise-addr {ip}',
            name,
            capture='text'
        )
        try:
            token = re.search(r'token\s+(.*)\s+\\', resp).group(1)
            port = re.search(r':(\d\d\d\d)', resp).group(1)
        except AttributeError:
            self.error('swarm initialisation failed')
        # s3 = self.get_boto_client('s3')
        # try:
        #     data = s3.get_object(
        #         Bucket=ctx['bucket'],
        #         Key=f'fuku/{ctx["cluster"]}/swarm.json',
        #     )['Body'].read().decode()
        #     data = json.loads(data)
        # except:
        #     data = {
        #         'managers': []
        #     }
        # data['managers'].append({
        #     'workertoken': token,
        #     'ip': ip,
        #     'port': port
        # })
        # s3.put_object(
        #     Bucket=ctx['bucket'],
        #     Key=f'fuku/{ctx["cluster"]}/swarm.json',
        #     Body=json.dumps(data)
        # )
        ec2 = self.get_boto_client('ec2')
        ec2.create_tags(
            Resources=[inst.id],
            Tags=[
                {'Key': 'swarmtoken', 'Value': token},
                {'Key': 'swarmport', 'Value': port}
            ]
        )
        self.ssh_run(
            'docker network create --driver overlay all',
            name=name,
            capture='discard'
        )

    def handle_join_swarm(self, args):
        self.join_swarm(args.name)

    def join_swarm(self, name):
        for inst in self.iter_managers():
            try:
                token, port = None, None
                for tag in inst.tags:
                    if tag['Key'] == 'swarmtoken':
                        token = tag['Value']
                    if tag['Key'] == 'swarmport':
                        port = tag['Value']
                if token is None:
                    raise Exception
                resp = self.ssh_run(
                    f'docker swarm join --token {token} {inst.private_ip_address}:{port}',
                    name=name,
                    capture='text'
                )
                if resp != 'This node joined a swarm as a worker.':
                    self.error('failed to join swarm')
            except:
                pass
            else:
                break

    def handle_wait(self, args):
        self.wait(args.name)

    def wait(self, name):
        inst = self.get_instance(name)
        ec2 = self.get_boto_client('ec2')
        waiter = ec2.get_waiter('instance_status_ok')
        waiter.wait(InstanceIds=[inst.id])

    def handle_ssh(self, args):
        self.ssh_run('', args.name, tty=True)

    def handle_reboot(self, args):
        self.reboot(args.name)

    def reboot(self, name):
        ctx = self.get_context()
        try:
            name = name or ctx['node']
        except KeyError:
            self.error('unknown node')
        inst = self.get_instance(name)
        inst.reboot()

    def tag_instance(self, inst_id, name, manager=False, ec2=None, ctx=None):
        if ctx is None:
            ctx = self.get_context()
        if ec2 is None:
            ec2 = self.get_boto_resource('ec2')
        ec2.create_tags(
            Resources=[inst_id],
            Tags=[
                {'Key': 'Name', 'Value': f'fuku-{ctx["cluster"]}-{name}'},
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
                'Name': 'tag:cluster',
                'Values': [cluster]
            },
            {
                'Name': 'instance-state-name',
                'Values': ['pending', 'running', 'stopping', 'stopped']
            }
        ]
        for inst in ec2.instances.filter(Filters=filters):
            yield inst

    def get_instance_names(self):
        names = []
        for inst in self.iter_instances():
            for tag in inst.tags:
                if tag['Key'] == 'name':
                    names.append(tag['Value'])
                    break
        return names

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

    def get_instance_arn(self, name):
        ctx = self.get_context()
        ecs = self.get_boto_client('ecs')
        paginator = ecs.get_paginator('list_container_instances')
        ec2inst = self.get_instance(name)
        instances = paginator.paginate(
            cluster=f'fuku-{ctx["cluster"]}',
        )
        for inst in instances:
            for arn in inst['containerInstanceArns']:
                res = ecs.describe_container_instances(
                    cluster=f'fuku-{ctx["cluster"]}',
                    containerInstances=[arn]
                )
                for ci in res['containerInstances']:
                    if ci['ec2InstanceId'] == ec2inst.id:
                        return arn
        raise Exception

    def get_bastion(self):
        return self.get_instance('bastion')

    def iter_managers(self):
        ctx = self.get_context()
        ec2 = self.get_boto_resource('ec2')
        filters = [
            {
                'Name': 'tag:cluster',
                'Values': [ctx['cluster']]
            },
            {
                'Name': 'tag:node',
                'Values': ['manager']
            },
            {
                'Name': 'instance-state-name',
                'Values': ['running']
            }
        ]
        for inst in ec2.instances.filter(Filters=filters):
            yield inst

    def iter_nodes(self):
        ctx = self.get_context()
        ec2 = self.get_boto_resource('ec2')
        filters = [
            {
                'Name': 'tag:cluster',
                'Values': [ctx['cluster']]
            },
            {
                'Name': 'instance-state-name',
                'Values': ['running']
            }
        ]
        for inst in ec2.instances.filter(Filters=filters):
            yield inst

    def ssh_run(self, cmd, name=None, inst=None, tty=False, capture=None):
        ctx = self.get_context()
        if inst is None:
            name = name or ctx['node']
            inst = self.get_instance(name)
        bastion = self.get_bastion()
        ip = bastion.public_ip_address
        priv_ip = inst.private_ip_address
        b64cmd = base64.b64encode(cmd.encode()).decode()
        full_cmd = f'ssh-add {ctx["pem"]} 2> /dev/null && ssh{" -t" if tty else ""} -o StrictHostKeyChecking=no -o LogLevel=QUIET  -A ec2-user@{ip} ssh{" -t" if tty else ""} -o StrictHostKeyChecking=no -o LogLevel=QUIET {priv_ip} "\`echo {b64cmd} | base64 -di\`"'
        return self.run(full_cmd, capture=capture)

    def handle_bastion(self, args):
        self.bastion()

    def bastion(self):
        name = 'bastion'
        ctx = self.get_context()
        ec2 = self.get_boto_client('ec2')
        image = self.ami_map[ctx['region']]
        sg_id = self.get_module('cluster').get_security_group_id()
        sn = self.get_module('cluster').get_public_subnet()
        opts = {
            'ImageId': image,
            'NetworkInterfaces': [
                {
                    'DeviceIndex': 0,
                    'SubnetId': sn.id,
                    'Groups': [sg_id],
                    'AssociatePublicIpAddress': True
                }
            ],
            'KeyName': f'fuku-{ctx["cluster"]}',
            # 'SecurityGroupIds': [sg_id],
            'InstanceType': 't2.nano',
            'IamInstanceProfile': {
                'Name': 'ec2-profile'
            },
            'MinCount': 1,
            'MaxCount': 1,
            # 'BlockDeviceMappings': [
            #     {
            #         'DeviceName': '/dev/xvdcz',
            #         'Ebs': {
            #             'VolumeSize': 10
            #         }
            #     }
            # ]
        }
        inst = ec2.run_instances(**opts)
        inst_id = inst['Instances'][0]['InstanceId']
        ec2.create_tags(
            Resources=[inst_id],
            Tags=[
                {'Key': 'Name', 'Value': f'fuku-{ctx["cluster"]}-{name}'},
                {'Key': 'cluster', 'Value': f'{ctx["cluster"]}'},
                {'Key': 'name', 'Value': f'{name}'},
                {'Key': 'bastion', 'Value': 'true'}
            ]
        )
        self.wait(name)
        return inst_id

    def mgr_run(self, cmd, tty=False, capture=None):
        try:
            mgr = list(self.iter_managers())[0]
        except IndexError:
            self.error('no managers available')
        return self.ssh_run(cmd, inst=mgr, tty=tty, capture=capture)

    def all_run(self, cmd):
        for node in self.iter_nodes():
            self.ssh_run(cmd, inst=node, capture=False)

    def get_my_context(self):
        ctx = {}
        node = self.store_get('selected')
        if node is not None:
            ctx['node'] = node
        return ctx


class EcsNode(Node):
    ami_map = {
        'ap-southeast-2': 'ami-c1a6bda2'
    }

    def add_arguments(self, parser):
        subp = parser.add_subparsers(help='node help')

        p = subp.add_parser('ls', help='list nodes')
        p.add_argument('name', metavar='NAME', nargs='?', help='node name')
        p.set_defaults(node_handler=self.handle_list)

        p = subp.add_parser('mk', help='make a node')
        p.add_argument('name', metavar='NAME', help='node name')
        p.add_argument('--availability-zone', '-a', choices=['a', 'b'], help='availability zone')
        p.add_argument('--type', '-t', help='instance type')
        p.set_defaults(node_handler=self.handle_make)

        p = subp.add_parser('putattr', help='add attribute')
        p.add_argument('name', metavar='NAME', help='node name')
        p.add_argument('values', metavar='VALUES', action=StoreKeyValuePair, nargs='*', help='attributes')
        p.set_defaults(node_handler=self.handle_put_attributes)

        p = subp.add_parser('rm', help='remove a node')
        p.add_argument('name', metavar='NAME', help='node name')
        p.set_defaults(node_handler=self.handle_remove)

        p = subp.add_parser('bastion', help='make a bastion node')
        p.set_defaults(node_handler=self.handle_bastion)

        p = subp.add_parser('ssh', help='SSH to a node')
        p.add_argument('name', metavar='NAME', nargs='?', help='node name')
        p.set_defaults(node_handler=self.handle_ssh)

        p = subp.add_parser('wait', help='wait for OK status')
        p.add_argument('name', metavar='NAME', help='node name')
        p.set_defaults(node_handler=self.handle_wait)

        p = subp.add_parser('reboot', help='reboot a node')
        p.add_argument('name', metavar='NAME', help='node name')
        p.set_defaults(node_handler=self.handle_reboot)

    def handle_make(self, args):
        self.make(args.name, args.availability_zone, args.type)

    def make(self, name, zone, type):
        self.validate(name)
        existing = self.get_instance_names()
        if name in existing:
            self.error(f'node "{name}" already exists')
        ctx = self.get_context({'node': name})
        ec2 = self.get_boto_client('ec2')
        image = self.ami_map[ctx['region']]
        sg_id = self.get_module('cluster').get_security_group_id()
        sn = self.get_module('cluster').get_private_subnet(zone=zone)
        opts = {
            'ImageId': image,
            'SubnetId': sn.id,
            'KeyName': f'fuku-{ctx["cluster"]}',
            'SecurityGroupIds': [sg_id],
            'InstanceType': type,
            'IamInstanceProfile': {
                'Name': 'ec2-profile'
            },
            'MinCount': 1,
            'MaxCount': 1,
            'UserData': f'#!/bin/bash\necho ECS_CLUSTER=fuku-{ctx["cluster"]} >> /etc/ecs/ecs.config',
            # 'BlockDeviceMappings': [
            #     {
            #         'DeviceName': '/dev/xvdcz',
            #         'VirtualName': 'ephemeral0'
            #     }
            # ]
        }
        inst = ec2.run_instances(**opts)
        inst_id = inst['Instances'][0]['InstanceId']
        self.tag_instance(inst_id, name, False, ec2=ec2, ctx=ctx)
        self.wait(name)
