import os
import random
import re
import stat

from .db import get_rc_path
from .module import Module
from .utils import EntityAlreadyExists, entity_already_exists

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
        p.add_argument('--type', '-t', choices=['swarm', 'ecs'], default='ecs', help='cluster type')
        p.set_defaults(cluster_handler=self.handle_make)

        p = subp.add_parser('sl', help='select a cluster')
        p.add_argument('name', metavar='NAME', nargs='?', help='cluster name')
        p.set_defaults(cluster_handler=self.handle_select)

        p = subp.add_parser('up', help='update a cluster')
        p.add_argument('name', metavar='NAME', help='cluster name')
        p.add_argument('--pem', '-p', help='PEM file')
        p.set_defaults(cluster_handler=self.handle_update)

        p = subp.add_parser('summary', help='summarize cluster contents')
        p.set_defaults(cluster_handler=self.handle_summary)

    def handle_list(self, args):
        self.list(args.name)

    def list(self, name):
        for cl in self.iter_clusters():
            print(cl)

    def handle_make(self, args):
        self.make(args.name, args.type)

    def make(self, name, type):
        self.validate(name)
        vpc = self.create_vpc(name, type)
        self.create_subnets(name, vpc)
        self.create_igw(name)
        nat_id = self.create_nat(name)
        self.create_route_tables(name, nat_id)
        ecs = self.get_boto_client('ecs')
        ecs.create_cluster(
            clusterName=f'fuku-{name}'
        )
        ec2 = self.get_boto_client('ec2')
        self.create_key_pair(name, ec2=ec2)
        sg_id = self.create_security_group(name, vpc, ec2=ec2)
        self.create_log_group(name)
        self.create_alb(name, vpc.id, sg_id)
        self.select(name)

    def handle_update(self, args):
        self.update(args.name, args.pem)

    def update(self, name, pem):
        if pem:
            self.add_pem(name, pem_fn=pem)

    def handle_select(self, args):
        self.select(args.name)

    def select(self, name):
        self.get_logger().debug(f'Selecting: {name}')

        self.use_context = False
        if name and name not in list(self.iter_clusters()):
            self.error(f'no cluster "{name}"')

        if name:
            path = self.get_secure_file(f'{name}/key.pem')
            self.run(f'ssh-add {path}')
        else:
            sel = self.store_get('selected')
            if sel:
                self.clear_secure_file(f'{sel}/key.pem')

        self.store_set('selected', name)
        self.clear_parent_selections()

        # path = os.path.join(get_rc_path(), name, 'key.pem')
        # if not os.path.exists(path):
        #     key = self.gets3(f'{name}/key.pem.gpg')
        #     if key is None:
        #         self.error(f'no key file found for cluster')
        #     try:
        #         os.makedirs(os.path.dirname(path))
        #     except OSError:
        #         pass
        #     with open(f'{path}.gpg', 'wb') as file:
        #         file.write(key)
        #     self.run(f'gpg -d {path}.gpg > {path}')
        #     os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)

    def handle_summary(self, args):
        self.summary()

    def summary(self):
        app_mod = self.get_module('app')
        svc_mod = self.get_module('service')
        for app in app_mod.iter_apps():
            all_svcs = []
            for svc in svc_mod.iter_services(app_name=app):
                svc_data = svc_mod.describe_service(svc, app_name=app)
                all_svcs.append((svc, svc_data))
            print(f'{app}')
            all_svcs = sorted(all_svcs, key=lambda x: x[0])
            try:
                max_w = max(len(s[0]) for s in all_svcs)
            except ValueError:
                max_w = None
            for svc, data in all_svcs:
                if max_w:
                    w = max_w - len(svc)
                else:
                    w = 0
                try:
                    des_cnt = data['deployments'][0]['desiredCount']
                    run_cnt = data['deployments'][0]['runningCount']
                    print(f'  {svc}{w * " "}  {des_cnt}  {run_cnt}')
                except:
                    pass

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
        s3.upload_file(f'{path}.gpg', ctx['bucket'], f'fuku/{ctx["cluster"]}/key.pem.gpg')

    def create_vpc(self, name, type):
        ec2 = self.get_boto_resource('ec2')
        vpc = self.get_vpc(name, ec2)
        if not vpc:
            ec2_cli = self.get_boto_client('ec2')
            id = ec2_cli.create_vpc(
                CidrBlock='10.0.0.0/16'
            )['Vpc']['VpcId']
            vpc = ec2.Vpc(id)
            vpc.modify_attribute(
                EnableDnsSupport={'Value': True},
                EnableDnsHostnames={'Value': True}
            )
            vpc.create_tags(
                Tags=[
                    {
                        'Key': 'Name',
                        'Value': f'fuku-{name}',
                    },
                    {
                        'Key': 'cluster',
                        'Value': name,
                    },
                    {
                        'Key': 'type',
                        'Value': type
                    }
                ]
            )
        return vpc

    def create_subnets(self, name, vpc=None):
        if not vpc:
            vpc = self.get_vpc(name)
        region_mod = self.get_module('region')
        public_subnet_a = vpc.create_subnet(
            CidrBlock='10.0.4.0/23',
            AvailabilityZone=region_mod.get_availability_zone('a')
        )
        public_subnet_a.create_tags(
            Tags=[
                {
                    'Key': 'Name',
                    'Value': f'fuku-{name}-public-a',
                },
                {
                    'Key': 'cluster',
                    'Value': name,
                }
            ]
        )
        public_subnet_b = vpc.create_subnet(
            CidrBlock='10.0.6.0/23',
            AvailabilityZone=region_mod.get_availability_zone('b')
        )
        public_subnet_b.create_tags(
            Tags=[
                {
                    'Key': 'Name',
                    'Value': f'fuku-{name}-public-b',
                },
                {
                    'Key': 'cluster',
                    'Value': name,
                }
            ]
        )
        private_subnet_a = vpc.create_subnet(
            CidrBlock='10.0.0.0/23',
            AvailabilityZone=region_mod.get_availability_zone('a')
        )
        private_subnet_a.create_tags(
            Tags=[
                {
                    'Key': 'Name',
                    'Value': f'fuku-{name}-private-a',
                },
                {
                    'Key': 'cluster',
                    'Value': name,
                }
            ]
        )
        private_subnet_b = vpc.create_subnet(
            CidrBlock='10.0.2.0/23',
            AvailabilityZone=region_mod.get_availability_zone('b')
        )
        private_subnet_b.create_tags(
            Tags=[
                {
                    'Key': 'Name',
                    'Value': f'fuku-{name}-private-b',
                },
                {
                    'Key': 'cluster',
                    'Value': name,
                }
            ]
        )

    def create_nat(self, name, eip=None):
        if not eip:
            eip = self.create_eip()
        subnet = self.get_public_subnet(name)
        ec2_cli = self.get_boto_client('ec2')
        nat_id = ec2_cli.create_nat_gateway(
            SubnetId=subnet.id,
            AllocationId=eip
        )['NatGateway']['NatGatewayId']
        waiter = ec2_cli.get_waiter('nat_gateway_available')
        waiter.wait(NatGatewayIds=[nat_id])
        return nat_id

    def create_igw(self, name):
        igw = self.get_igw(name)
        if igw is not None:
            return igw
        vpc = self.get_vpc(name)
        ec2_cli = self.get_boto_client('ec2')
        id = ec2_cli.create_internet_gateway()['InternetGateway']['InternetGatewayId']
        ec2 = self.get_boto_resource('ec2')
        igw = ec2.InternetGateway(id)
        igw.create_tags(
            Tags=[
                {
                    'Key': 'Name',
                    'Value': f'fuku-{name}'
                },
                {
                    'Key': 'cluster',
                    'Value': name,
                }
            ]
        )
        igw.attach_to_vpc(VpcId=vpc.id)
        return igw

    def create_route_tables(self, name, nat_id):
        ec2_cli = self.get_boto_client('ec2')
        ec2 = self.get_boto_resource('ec2')
        vpc = self.get_vpc(name)
        igw = self.get_igw(name)
        id = ec2_cli.create_route_table(VpcId=vpc.id)['RouteTable']['RouteTableId']
        rt = ec2.RouteTable(id)
        rt.create_tags(
            Tags=[
                {
                    'Key': 'Name',
                    'Value': f'fuku-{name}-private'
                },
                {
                    'Key': 'cluster',
                    'Value': name,
                }
            ]
        )
        ec2_cli.create_route(
            RouteTableId=rt.id,
            DestinationCidrBlock='0.0.0.0/0',
            NatGatewayId=nat_id
        )
        for sn in self.iter_private_subnets(name):
            rt.associate_with_subnet(SubnetId=sn.id)
        id = ec2_cli.create_route_table(VpcId=vpc.id)['RouteTable']['RouteTableId']
        rt = ec2.RouteTable(id)
        rt.create_tags(
            Tags=[
                {
                    'Key': 'Name',
                    'Value': f'fuku-{name}-public'
                },
                {
                    'Key': 'cluster',
                    'Value': name,
                }
            ]
        )
        ec2_cli.create_route(
            RouteTableId=rt.id,
            DestinationCidrBlock='0.0.0.0/0',
            NatGatewayId=igw.id
        )
        for sn in self.iter_public_subnets(name):
            rt.associate_with_subnet(SubnetId=sn.id)

    def create_eip(self):
        ec2_cli = self.get_boto_client('ec2')
        return ec2_cli.allocate_address(
            Domain='vpc'
        )['AllocationId']

    def create_alb(self, name, vpc_id, sg_id):
        alb_cli = self.get_boto_client('elbv2')
        alb_name = f'fuku-{name}-0'
        subnets = [sn.id for sn in self.iter_public_subnets(name)]
        alb_arn = alb_cli.create_load_balancer(
            Name=alb_name,
            Subnets=subnets,
            SecurityGroups=[
                sg_id
            ],
            Scheme='internet-facing',
            IpAddressType='ipv4',
            Tags=[
                {
                    'Key': 'Name',
                    'Value': name
                },
                {
                    'Key': 'cluster',
                    'Value': name
                },
                {
                    'Key': 'index',
                    'Value': '0'
                }
            ]
        )['LoadBalancers'][0]['LoadBalancerArn']
        tg_arn = alb_cli.create_target_group(
            Name=f'fuku-{name}-default',
            Protocol='HTTP',
            Port=80,
            VpcId=vpc_id,
            Matcher={
                'HttpCode': '200,301'
            }
        )['TargetGroups'][0]['TargetGroupArn']
        alb_cli.create_listener(
            LoadBalancerArn=alb_arn,
            Protocol='HTTP',
            Port=80,
            DefaultActions=[
                {
                    'Type': 'forward',
                    'TargetGroupArn': tg_arn
                }
            ]
        )

    def get_alb_arn(self, name, index=0, alb_cli=None):
        if alb_cli is None:
            alb_cli = self.get_boto_client('elbv2')
        return alb_cli.describe_load_balancers(
            Names=[f'fuku-{name}-{index}']
        )['LoadBalancers'][0]['LoadBalancerArn']

    def iter_listeners(self, name=None, index=0, alb_cli=None):
        if not name:
            name = self.get_context()['cluster']
        if alb_cli is None:
            alb_cli = self.get_boto_client('elbv2')
        alb_arn = self.get_alb_arn(name, index=index)
        listeners = alb_cli.describe_listeners(
            LoadBalancerArn=alb_arn,
        )['Listeners']
        for lsnr in listeners:
            yield lsnr

    def get_igw(self, name):
        ec2 = self.get_boto_resource('ec2')
        igws = ec2.internet_gateways.filter(Filters=[{'Name': 'tag:cluster', 'Values': [name]}])
        for i in igws:
            return i
        return None

    def get_public_subnet(self, name=None, zone=None):
        if name is None:
            ctx = self.get_context()
            name = ctx['cluster']
        zone = zone if zone else random.choice(['a', 'b'])
        ec2 = self.get_boto_resource('ec2')
        subnets = ec2.subnets.filter(Filters=[{'Name': 'tag:Name', 'Values': [f'fuku-{name}-public-{zone}']}])
        for s in subnets:
            return s
        self.error('no private subnets')

    def iter_public_subnets(self, name, ec2=None):
        ec2 = self.get_boto_resource('ec2')
        subnets = ec2.subnets.filter(Filters=[{'Name': 'tag:Name', 'Values': [f'fuku-{name}-public-a']}])
        for s in subnets:
            yield s
        subnets = ec2.subnets.filter(Filters=[{'Name': 'tag:Name', 'Values': [f'fuku-{name}-public-b']}])
        for s in subnets:
            yield s

    def get_private_subnet(self, name=None, zone=None):
        if name is None:
            ctx = self.get_context()
            name = ctx['cluster']
        zone = zone if zone else random.choice(['a', 'b'])
        ec2 = self.get_boto_resource('ec2')
        subnets = ec2.subnets.filter(Filters=[{'Name': 'tag:Name', 'Values': [f'fuku-{name}-private-{zone}']}])
        for s in subnets:
            return s
        self.error('no private subnets')

    def iter_private_subnets(self, name, ec2=None):
        ec2 = self.get_boto_resource('ec2')
        subnets = ec2.subnets.filter(Filters=[{'Name': 'tag:Name', 'Values': [f'fuku-{name}-private-a']}])
        for s in subnets:
            yield s
        subnets = ec2.subnets.filter(Filters=[{'Name': 'tag:Name', 'Values': [f'fuku-{name}-private-b']}])
        for s in subnets:
            yield s

    def get_vpc(self, name=None, ec2=None):
        if name is None:
            name = self.get_context()['cluser']
        if ec2 is None:
            ec2 = self.get_boto_resource('ec2')
        vpcs = ec2.vpcs.filter(Filters=[{'Name': 'tag:cluster', 'Values': [name]}])
        for vpc in vpcs:
            return vpc
        return None

    def create_security_group(self, name, vpc=None, ec2=None):
        if vpc is None:
            vpc = self.get_vpc(name)
        if ec2 is None:
            ec2 = self.get_boto_client('ec2')
        user_id = self.client.get_module('profile').get_user_id()
        sg_id = None
        with entity_already_exists():
            sg_id = ec2.create_security_group(
                GroupName=f'fuku-{name}',
                Description=f'{name} security group',
                VpcId=vpc.id
            )['GroupId']
        if sg_id is None:
            sg_id = self.get_security_group_id(name)
        with entity_already_exists():
            ec2.authorize_security_group_ingress(
                GroupId=sg_id,
                IpProtocol='tcp',
                FromPort=22,
                ToPort=22,
                CidrIp='0.0.0.0/0'
            )
        with entity_already_exists():
            ec2.authorize_security_group_ingress(
                GroupId=sg_id,
                IpProtocol='tcp',
                FromPort=80,
                ToPort=80,
                CidrIp='0.0.0.0/0'
            )
        with entity_already_exists():
            ec2.authorize_security_group_ingress(
                GroupId=sg_id,
                IpProtocol='tcp',
                FromPort=443,
                ToPort=443,
                CidrIp='0.0.0.0/0'
            )
        with entity_already_exists():
            ec2.authorize_security_group_ingress(
                GroupId=sg_id,
                IpProtocol='tcp',
                FromPort=5432,
                ToPort=5432,
                CidrIp='0.0.0.0/0'
            )
        with entity_already_exists():
            ec2.authorize_security_group_ingress(
                GroupId=sg_id,
                IpPermissions=[{
                    'IpProtocol': '-1',
                    'UserIdGroupPairs': [{
                        'UserId': user_id,
                        'GroupId': sg_id
                    }]
                }]
            )
        return sg_id

    def get_security_group_id(self, name=None):
        name = name or self.store_get('selected')
        ec2 = self.get_boto_client('ec2')
        vpc = self.get_vpc(name)
        all_groups = ec2.describe_security_groups(
            Filters=[{'Name': 'vpc-id', 'Values': [vpc.id]}]
        )['SecurityGroups']
        for sg in all_groups:
            if sg['GroupName'] == f'fuku-{name}':
                return sg['GroupId']
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
