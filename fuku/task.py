import json

from .module import Module
from .utils import (
    StoreKeyValuePair, StorePortPair,
    env_to_dict, dict_to_env,
    ports_to_dict, dict_to_ports
)


class Task(Module):
    dependencies = ['app']

    def __init__(self, **kwargs):
        super().__init__('task', **kwargs)

    def add_arguments(self, parser):
        subp = parser.add_subparsers(help='container help')

        p = subp.add_parser('add', help='add/update a container defintion to a task')
        p.add_argument('task')
        p.add_argument('container')
        p.add_argument('image')
        p.add_argument('--update', '-u', action='store_true')
        p.set_defaults(task_handler=self.handle_add)

        p = subp.add_parser('remove', help='remove a container definition from a task')
        p.add_argument('task')
        p.add_argument('name', nargs='?')
        p.set_defaults(task_handler=self.handle_remove)

        p = subp.add_parser('list', help='list tasks')
        p.add_argument('name', nargs='?')
        p.add_argument('--running', '-r', action='store_true')
        p.add_argument('--ready', '-d', action='store_true')
        p.set_defaults(task_handler=self.handle_list)

        p = subp.add_parser('env', help='update environment')
        p.add_argument('task')
        p.add_argument('name')
        ssp = p.add_subparsers()
        p = ssp.add_parser('set')
        p.add_argument('values', action=StoreKeyValuePair, nargs='+', help='key value pairs')
        p.set_defaults(task_handler=self.handle_env_set)
        p = ssp.add_parser('unset')
        p.add_argument('values', nargs='+', help='keys')
        p.set_defaults(task_handler=self.handle_env_unset)

        p = subp.add_parser('ports', help='manage ports')
        p.add_argument('task')
        p.add_argument('name')
        ssp = p.add_subparsers()
        p = ssp.add_parser('set')
        p.add_argument('values', action=StorePortPair, nargs='+', help='port mappings')
        p.set_defaults(task_handler=self.handle_ports_set)
        p = ssp.add_parser('unset')
        p.add_argument('values', nargs='+', help='ports')
        p.set_defaults(task_handler=self.handle_ports_unset)

        p = subp.add_parser('link', help='manage links')
        p.add_argument('task')
        p.add_argument('name')
        p.add_argument('link')
        p.add_argument('--remove', '-r', action='store_true')
        p.set_defaults(task_handler=self.handle_link)

        p = subp.add_parser('run')
        p.add_argument('name', nargs='?')
        p.add_argument('task', nargs='?')
        p.add_argument('--restart', '-r', action='store_true')
        p.add_argument('--stop', '-s', action='store_true')
        p.add_argument('--remove', '-d', action='store_true')
        p.set_defaults(task_handler=self.handle_run)

        p = subp.add_parser('logs', help='get logs')
        p.add_argument('name', help='container name')
        p.set_defaults(task_handler=self.handle_logs)

    def handle_add(self, args):
        self.add(args.task, args.container, args.image, args.update)

    def add(self, task_name, ctr_name, image_name, update=False):
        app = self.client.get_selected('app')
        if image_name[0] != '/':
            img_mod = self.client.get_module('image')
            img = img_mod.get_uri(image_name)
        else:
            img = image_name[1:]
        try:
            task = self.get_task(task_name)
        except self.CommandError:
            task = None
        if update:
            if not task:
                self.error('no such task')
            ctr_def = self.get_container_definition(task, ctr_name)
            ctr_def['image'] = img
        else:
            if not task:
                task = {
                    'family': '%s-%s' % (app, task_name),
                    'containerDefinitions': []
                }
            for cd in task['containerDefinitions']:
                if cd['name'] == ctr_name:
                    self.error('container definition with that name already exists')
            ctr_def = {
                'name': ctr_name,
                'image': img,
                'memoryReservation': 1  # required
            }
            task['containerDefinitions'].append(ctr_def)
        self.register_task(task)

    def handle_remove(self, args):
        if args.name:
            task = self.get_task(args.task)
            task['containerDefinitions'] = list(filter(lambda x: x['name'] != args.name, task['containerDefinitions']))
            self.register_task(task)
        else:
            app = self.client.get_selected('app')
            full_task = '%s-%s' % (app, args.task)
            arns = self.run(
                '$aws ecs list-task-definitions'
                ' --family-prefix {}'
                ' --query=taskDefinitionArns'.format(full_task),
                capture='json'
            )
            for arn in arns:
                print('{}'.format(arn))
                self.run(
                    '$aws ecs deregister-task-definition'
                    ' --task-definition {}'.format(arn)
                )

    def handle_list(self, args):
        if args.running or args.ready:
            mach_mod = self.client.get_module('machine')
            mach = mach_mod.get_selected()
            cmd = 'fuku-agent list'
            if args.running:
                cmd += ' -r'
            data = mach_mod.ssh_run(
                cmd,
                name=mach,
                capture='json'
            )
            for ctr in data['result']:
                print(ctr)
        elif args.name:
            task = self.get_task(args.name, escape=False)
            print(json.dumps(task, indent=2))
        else:
            app = self.client.get_selected('app')
            data = self.run(
                '$aws ecs list-task-definition-families'
                ' --status ACTIVE'
                ' --query families',
                capture='json'
            )
            pre = '%s-' % app
            for d in data:
                if d.startswith(pre):
                    print(d[len(pre):])

    def handle_env_set(self, args):
        self.env_set(args.task, args.name, args.values)

    def env_set(self, task_name, ctr_name, values):
        task = self.get_task(task_name)
        ctr_def = self.get_container_definition(task, ctr_name)
        env = env_to_dict(ctr_def['environment'])
        env.update(values)
        env = dict_to_env(env)
        ctr_def['environment'] = env
        self.register_task(task)

    def handle_env_unset(self, args):
        self.env_unset(args.task, args.name, args.values)

    def env_unset(self, task_name, ctr_name, keys):
        task = self.get_task(task_name)
        ctr_def = self.get_container_definition(task, ctr_name)
        env = env_to_dict(ctr_def['environment'])
        for k in keys:
            try:
                del env[k]
            except KeyError:
                pass
        env = dict_to_env(env)
        ctr_def['environment'] = env
        self.register_task(task)

    def handle_ports_set(self, args):
        self.ports_set(args.task, args.name, args.values)

    def ports_set(self, task_name, ctr_name, values):
        task = self.get_task(task_name)
        ctr_def = self.get_container_definition(task, ctr_name)
        ports = ports_to_dict(ctr_def['portMappings'])
        for k, v in values.items():
            ports[int(k)] = int(v)
        ports = dict_to_ports(ports)
        ctr_def['portMappings'] = ports
        self.register_task(task)

    def handle_ports_unset(self, args):
        task = self.get_task(args.task)
        ctr_def = self.get_container_definition(task, args.name)
        ports = ports_to_dict(ctr_def['portMappings'])
        for p in args.values:
            try:
                del ports[int(p)]
            except KeyError:
                pass
        ports = dict_to_ports(ports)
        ctr_def['portMappings'] = ports
        self.register_task(task)

    def handle_link(self, args):
        self.link(args.task, args.name, args.link, args.remove)

    def link(self, task_name, ctr_name, link, remove=False):
        task = self.get_task(task_name)
        ctr_def = self.get_container_definition(task, ctr_name)
        links = ctr_def.get('links', [])
        if remove:
            ctr_def['links'] = list(set(links).difference(set([link])))
        else:
            ctr_def['links'] = list(set(links).union(set([link])))
        self.register_task(task)

    def handle_run(self, args):
        self._run(args.name, args.task, args.restart, args.remove, args.stop)

    def _run(self, name, task, restart=False, remove=False, stop=False):
        mach_mod = self.client.get_module('machine')
        mach = mach_mod.get_selected()
        cmd = 'fuku-agent'
        if stop or remove:
            cmd += ' remove'
            if remove:
                cmd += ' -d'
            if name:
                cmd += ' ' + name
        else:
            cmd += ' run'
            if name:
                cmd += ' -n {}'.format(name)
            if task:
                cmd += ' -t {}'.format(task)
            if restart:
                cmd += ' -r'
        data = mach_mod.ssh_run(
            cmd,
            name=mach,
            capture='json'
        )
        if data['status'] != 'ok':
            print(data['result'])

    def handle_logs(self, args):
        mach_mod = self.client.get_module('machine')
        mach = mach_mod.get_selected()
        cmd = 'docker logs {}'.format(args.name)
        mach_mod.ssh_run(
            cmd,
            name=mach,
            tty=True,
            capture=False
        )

    def get_task(self, name, escape=True):
        app = self.client.get_selected('app')
        name = '%s-%s' % (app, name)
        data = self.run(
            '$aws ecs describe-task-definition'
            ' --task-definition {}'
            ' --query taskDefinition'
            .format(
                name,
            ),
            capture='json'
        )
        if escape:
            tmp = json.dumps(data)
            tmp = tmp.replace('$', '${dollar}')
            data = json.loads(tmp)
        return data

    def get_container_definition(self, task, name):
        try:
            ctr_def = list(filter(lambda x: x['name'] == name, task['containerDefinitions']))[0]
        except KeyError:
            self.error('container definition with that name does not exist')
        return ctr_def

    def register_task(self, task):
        self.run(
            '$aws ecs register-task-definition'
            ' --family {}'
            ' --container-definitions \'{}\''
            .format(
                task['family'],
                json.dumps(task['containerDefinitions'])
            )
        )
