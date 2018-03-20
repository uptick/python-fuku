from .module import Module
from .task import IGNORED_TASK_KWARGS
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

        remp = subp.add_parser('rm', help='remove an app')
        remp.add_argument('name', help='app name')
        remp.set_defaults(app_handler=self.handle_remove)

        p = subp.add_parser('sl', help='select an app')
        p.add_argument('name', metavar='NAME', help='app name')
        p.set_defaults(app_handler=self.handle_select)

        p = subp.add_parser('run', help='run a command')
        p.add_argument('image', metavar='IMAGE', help='image name')
        p.add_argument('command', metavar='CMD', nargs='+', help='command to run')
        p.set_defaults(app_handler=self.handle_run)

        p = subp.add_parser('expose', help='expose to internet')
        p.add_argument('name', metavar='NAME', help='app name')
        p.add_argument('domain', metavar='DOMAIN', help='domain name')
        p.set_defaults(app_handler=self.handle_expose)

        p = subp.add_parser('hide', help='hide from internet')
        p.add_argument('name', metavar='NAME', help='app name')
        p.set_defaults(app_handler=self.handle_hide)

    def handle_list(self, args):
        self.list()

    def list(self):
        for app in self.iter_apps():
            print(app)

    def iter_apps(self):
        for gr in self.iter_target_groups():
            yield gr['TargetGroupName'].rsplit('-', 1)[1]

    def handle_make(self, args):
        self.make(args.name)

    def make(self, name):
        self.use_context = False

        self.validate(name)

        if self.get_target_group(name):
            self.error(f'App "{name}" already exists')
            return

        self.make_target_group(name)
        self.make_task(name)
        self.select(name)

    def handle_remove(self, args):
        self.remove(args.name)

    def remove(self, name):
        self.use_context = False

        if not self.get_target_group(name):
            self.error(f'No such app "{name}"')
            return

        self.remove_target_group(name)

    def handle_select(self, args):
        self.select(args.name)

    def select(self, name):
        if name is not None:
            for gr in self.iter_target_groups():
                if name and gr['TargetGroupName'].endswith('-' + name):
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
        self.expose(args.name, args.domain)

    def expose(self, name, domain):
        alb_cli = self.get_boto_client('elbv2')
        target_group_arn = self.get_target_group()['TargetGroupArn']

        added = False
        paginate = self.get_boto_paginator('elbv2', 'describe_load_balancers').paginate()
        for elb in paginate.search(
            'LoadBalancers[?starts_with(LoadBalancerName, `"fuku-uptick-"`)]'
        ):
            listeners = alb_cli.describe_listeners(LoadBalancerArn=elb['LoadBalancerArn'])
            for listener in listeners['Listeners']:
                rules = alb_cli.describe_rules(ListenerArn=listener['ListenerArn'])['Rules']

                # # check that rule does not already exist
                for rule in rules:
                    try:
                        if (
                            rule['Conditions'][0]['Values'][0] == domain and
                            rule['Actions'][0]['TargetGroupArn'] == target_group_arn
                        ):
                            self.error(f'App already exposed on {elb["LoadBalancerName"]}')
                    except IndexError:
                        pass

                # count how many rules there are to define next priority
                rule_priorities = [int(r['Priority']) for r in rules if r['Priority'] != 'default']
                priority = (max(rule_priorities) + 1) if rule_priorities else 1
                try:
                    # attempt to create the rule
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
                                'TargetGroupArn': target_group_arn
                            }
                        ]
                    )
                    added = True
                    print(f'{elb["LoadBalancerName"]} [added]')
                except alb_cli.exceptions.TooManyRulesException:
                    print(f'{elb["LoadBalancerName"]} [skipped - full]')
                    break

            if added:
                break

    def handle_hide(self, args):
        self.hide(args.name)

    def hide(self, name):
        alb_cli = self.get_boto_client('elbv2')

        target_group = self.get_target_group(name)
        if not target_group:
            self.error(f'App {name} is not exposed')

        listeners = alb_cli.describe_listeners(LoadBalancerArn=target_group['LoadBalancerArns'][0])['Listeners']
        for listener in listeners:
            rules = alb_cli.describe_rules(ListenerArn=listener['ListenerArn'])['Rules']
            for rule in rules:
                try:
                    if rule['Actions'][0]['TargetGroupArn'] == target_group['TargetGroupArn']:
                        alb_cli.delete_rule(RuleArn=rule['RuleArn'])
                except IndexError:
                    pass

    def make_task(self, name):
        ctx = self.get_context()
        ecr_cli = self.get_boto_client('ecr')
        with entity_already_exists():
            ecr_cli.create_repository(repositoryName='fuku')

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
        skip = set(IGNORED_TASK_KWARGS)
        ecs.register_task_definition(**{
            k: v for k, v in task.items() if k not in skip
        })

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
        )

    def remove_target_group(self, name):
        alb_cli = self.get_boto_client('elbv2')
        target_group = self.get_target_group(name)
        alb_cli.delete_target_group(TargetGroupArn=target_group['TargetGroupArn'])

    def get_target_group(self, app=None):
        ctx = self.get_context()
        alb_cli = self.get_boto_client('elbv2')

        cluster = ctx['cluster']
        if not app:
            app = ctx['app']

        try:
            return alb_cli.describe_target_groups(Names=[f'fuku-{cluster}-{app}', ])['TargetGroups'][0]
        except:
            return None

    def iter_target_groups(self):
        self.use_context = False
        ctx = self.get_context()
        paginate = self.get_boto_paginator('elbv2', 'describe_target_groups').paginate()
        for gr in paginate.search(
            'TargetGroups[?starts_with(TargetGroupName, `"fuku-{cluster}-"`)] '.format(**ctx) +
            '| sort_by(@, &TargetGroupName)'
        ):
            yield gr
