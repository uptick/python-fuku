from .module import Module
from .utils import entity_already_exists


class App(Module):
    dependencies = ['cluster']

    def __init__(self, **kwargs):
        super().__init__('app', **kwargs)

    def add_arguments(self, parser):
        subp = parser.add_subparsers(help='app help')

        p = subp.add_parser('ls', help='list applications')
        p.set_defaults(app_handler=self.handle_list)

        p = subp.add_parser('mk', help='add an app')
        p.add_argument('name', metavar='NAME', help='app name')
        p.set_defaults(app_handler=self.handle_make)

        # remp = subp.add_parser('remove', help='remove an app')
        # remp.add_argument('name', help='app name')
        # remp.set_defaults(app_handler=self.remove)

        p = subp.add_parser('sl', help='select an app')
        p.add_argument('name', metavar='NAME', help='app name')
        p.set_defaults(app_handler=self.handle_select)

        p = subp.add_parser('run', help='run a command')
        p.add_argument('image', metavar='IMAGE', help='image name')
        p.add_argument('command', metavar='CMD', nargs='+', help='command to run')
        p.set_defaults(app_handler=self.handle_run)

        p = subp.add_parser('expose', help='expose to internet')
        p.add_argument('domain', metavar='DOMAIN', help='domain name')
        p.add_argument('load_balancer_index', metavar='LOAD_BALANCER_INDEX', help='load balancer index')
        p.set_defaults(app_handler=self.handle_expose)

    def handle_list(self, args):
        self.list()

    def list(self):
        for app in self.iter_apps():
            print(app)

    def iter_apps(self):
        for gr in self.iter_groups():
            yield gr.group_name[5:]

    def handle_make(self, args):
        self.make(args.name)

    def make(self, name):
        self.use_context = False
        self.validate(name)
        self.create_group(name)
        self.make_task(name)
        self.select(name)

    def handle_select(self, args):
        self.select(args.name)

    def select(self, name):
        if name is not None:
            for gr in self.iter_groups():
                if name and gr.name.endswith('-' + name):
                    self.store_set('selected', name)
                    self.clear_parent_selections()
                    return
            self.error(f'no app "{name}"')
        else:
            self.clear_parent_selections()

    def handle_run(self, args):
        self.run(args.image, args.command)

    def run(self, img, cmd):
        img = self.client.get_module('image').get_uri(img)
        cmd = ' '.join(cmd or [])
        full_cmd = f'docker run --rm -it {img} {cmd}'
        node_mod = self.client.get_module('node')
        node_mod.mgr_run(full_cmd, tty=True)

    def handle_expose(self, args):
        self.expose(args.domain, args.load_balancer_index)

    def expose(self, domain, load_balancer_index):
        cluster_mod = self.get_module('cluster')
        alb_cli = self.get_boto_client('elbv2')
        tg_arn = self.get_target_group_arn()
        for listener in cluster_mod.iter_listeners(index=load_balancer_index):
            rules = alb_cli.describe_rules(
                ListenerArn=listener['ListenerArn']
            )['Rules']

            rule_priorities = [int(r['Priority']) for r in rules if r['Priority'] != 'default']
            priority = (max(rule_priorities) + 1) if rule_priorities else 1
            alb_cli.create_rule(
                ListenerArn=listener['ListenerArn'],
                Conditions=[
                    {
                        'Field': 'host-header',
                        'Values': [domain]
                    }
                ],
                Priority=priority,
                Actions=[
                    {
                        'Type': 'forward',
                        'TargetGroupArn': tg_arn
                    }
                ]
            )

    def iter_groups(self):
        self.use_context = False
        ctx = self.get_context()
        iam = self.get_boto_resource('iam')
        for gr in iam.groups.filter(PathPrefix=f'/fuku/{ctx["cluster"]}/'):
            yield gr

    def create_group(self, name):
        ctx = self.get_context()
        cluster = ctx['cluster']
        iam = self.get_boto_client('iam')
        with entity_already_exists():
            iam.create_group(
                Path=f'/fuku/{cluster}/{name}/',
                GroupName=f'fuku-{cluster}-{name}'
            )

    def delete_app_group(self, name):
        # ctx = self.get_context()
        #  cluster = ctx['cluster']
        assert 0, 'todo'

    def make_task(self, name):
        ctx = self.get_context()
        ecr_cli = self.get_boto_client('ecr')
        with entity_already_exists():
            ecr_cli.create_repository(
                repositoryName='fuku'
            )
        img_uri = self.client.get_module('image').image_name_to_uri('/fuku')
        task = {
            'family': f'fuku-{ctx["cluster"]}-{name}',
            'containerDefinitions': []
        }
        ctr_def = {
            'name': name,
            'image': img_uri,
            'memoryReservation': 1
        }
        task['containerDefinitions'].append(ctr_def)
        ecs = self.get_boto_client('ecs')
        skip = set(['taskDefinitionArn', 'revision', 'status', 'requiresAttributes'])
        ecs.register_task_definition(**dict([
            (k, v) for k, v in task.items() if k not in skip
        ]))

    def get_my_context(self):
        if self.client.args.app:
            ctx = {'app': self.client.args.app}
        else:
            sel = self.store_get('selected')
            if not sel:
                self.error('no app currently selected')
            ctx = {'app': sel}
        self.get_logger().debug(f'APP Context: {ctx}')
        return ctx


class EcsApp(App):
    def make(self, name):
        super().make(name)
        self.make_target_group(name)

    def make_target_group(self, name):
        ctx = self.get_context()
        vpc_id = self.get_module('cluster').get_vpc(ctx['cluster']).id
        alb_cli = self.get_boto_client('elbv2')
        alb_cli.create_target_group(
            Name=f'fuku-{ctx["cluster"]}-{name}',
            Protocol='HTTP',
            Port=80,
            VpcId=vpc_id,
            Matcher={
                'HttpCode': '200,301'
            }
        )['TargetGroups'][0]['TargetGroupArn']

    def get_target_group_arn(self):
        ctx = self.get_context()
        alb_cli = self.get_boto_client('elbv2')
        tg_arn = alb_cli.describe_target_groups(
            Names=[f'fuku-{ctx["cluster"]}-{ctx["app"]}']
        )['TargetGroups'][0]['TargetGroupArn']
        return tg_arn
