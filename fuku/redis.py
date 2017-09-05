from .module import Module


class Redis(Module):
    dependencies = ['task']

    def __init__(self, **kwargs):
        super().__init__('redis', **kwargs)

    def add_arguments(self, parser):
        subp = parser.add_subparsers(help='redis help')

        p = subp.add_parser('mk', help='make a redis task')
        p.set_defaults(redis_handler=self.handle_make)

        p = subp.add_parser('connect')
        p.add_argument('target', metavar='TARGET', help='target task name')
        p.set_defaults(redis_handler=self.handle_connect)

    def handle_make(self, args):
        self.make()

    def make(self):
        task_mod = self.client.get_module('task')
        task_mod.make('redis', '!redis:alpine', memory=64)

    def handle_connect(self, args):
        self.connect(args.target)

    def connect(self, target):
        task_mod = self.client.get_module('task')
        env = {
            'REDIS_URL': self.get_url()
        }
        task_mod.env_set(target, env)

    def get_url(self):
        ctx = self.get_context()
        return f'redis://{ctx["app"]}-redis:6379'


class EcsRedis(Redis):
    def add_arguments(self, parser):
        subp = parser.add_subparsers(help='redis help')

        p = subp.add_parser('ls')
        p.set_defaults(redis_handler=self.handle_list)

        p = subp.add_parser('mk', help='make a redis instance')
        p.add_argument('name', metavar='NAME', help='instance name')
        p.add_argument('--group', metavar='GROUP', help='subnet group name')
        p.set_defaults(redis_handler=self.handle_make)

        p = subp.add_parser('connect')
        p.add_argument('name', metavar='NAME', help='instance name')
        p.add_argument('target', metavar='TARGET', nargs='?', help='target task name')
        p.set_defaults(redis_handler=self.handle_connect)

    def handle_make(self, args):
        self.make(args.name, args.group)

    def make(self, name, group):
        ctx = self.get_context()
        inst_id = self.get_id(name, group)
        sg_id = self.client.get_module('cluster').get_security_group_id()
        ec_cli = self.get_boto_client('elasticache')

        try:
            ec_cli.describe_cache_subnet_groups(CacheSubnetGroupName=inst_id)
        except:
            ec_cli.create_cache_subnet_group(
                CacheSubnetGroupName=inst_id,
                CacheSubnetGroupDescription=f'Subnet group for {inst_id}',
                SubnetIds=[
                    sn.id for sn in
                    self.get_module('cluster').iter_private_subnets(ctx['cluster'])
                ]
            )

        ec_cli.create_cache_cluster(
            CacheClusterId=name,
            NumCacheNodes=1,
            CacheNodeType='cache.t2.micro',
            Engine='redis',
            CacheSubnetGroupName=inst_id,
            SecurityGroupIds=[sg_id],
            Tags=[
                {
                    'Key': 'cluster',
                    'Value': ctx['cluster']
                },
                {
                    'Key': 'app',
                    'Value': ctx['app']
                }
            ]
        )
        waiter = ec_cli.get_waiter('cache_cluster_available')
        waiter.wait(
            CacheClusterId=name
        )

    def handle_connect(self, args):
        self.connect(args.name, args.target)

    def connect(self, name, target):
        task_mod = self.client.get_module('task')
        env = {
            'REDIS_URL': self.get_url(name)
        }
        task_mod.env_set(target, env)

    def get_id(self, name, group):
        ctx = self.get_context()
        if group:
            return f'fuku-{ctx["cluster"]}-{group}'
        return f'fuku-{ctx["cluster"]}-{ctx["app"]}-{name}'

    def get_url(self, name):
        ec_cli = self.get_boto_client('elasticache')
        ep = ec_cli.describe_cache_clusters(
            CacheClusterId=name,
            ShowCacheNodeInfo=True
        )['CacheClusters'][0]['CacheNodes'][0]['Endpoint']
        return f'redis://{ep["Address"]}:{ep["Port"]}'

    def handle_list(self, args):
        ec_cli = self.get_boto_client('elasticache')
        data = ec_cli.describe_cache_clusters()
        for cache in data['CacheClusters']:
            name = cache['CacheClusterId']
            print(name)
