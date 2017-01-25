import json

from .module import Module
from .utils import (
    StoreKeyValuePair, StorePortPair,
    env_to_dict, dict_to_env,
    ports_to_dict, dict_to_ports,
)


class Task(Module):
    dependencies = ['app']

    def __init__(self, **kwargs):
        super().__init__('task', **kwargs)

    def add_arguments(self, parser):
        subp = parser.add_subparsers(help='container help')

        p = subp.add_parser('add', help='add/update a task')
        p.add_argument('name')
        p.add_argument('image')
        p.add_argument('--update', '-u', action='store_true')
        p.set_defaults(task_handler=self.handle_add)

        p = subp.add_parser('remove', help='remove a task')
        p.add_argument('name')
        p.set_defaults(task_handler=self.handle_remove)

        p = subp.add_parser('list', help='list tasks')
        p.add_argument('name', nargs='?')
        p.set_defaults(task_handler=self.handle_list)

        p = subp.add_parser('env', help='update environment')
        p.add_argument('name')
        ssp = p.add_subparsers()
        p = ssp.add_parser('set')
        p.add_argument('values', action=StoreKeyValuePair, nargs='+', help='key value pairs')
        p.set_defaults(task_handler=self.handle_env_set)
        p = ssp.add_parser('unset')
        p.add_argument('values', nargs='+', help='keys')
        p.set_defaults(task_handler=self.handle_env_unset)

        p = subp.add_parser('ports', help='manage ports')
        p.add_argument('name')
        ssp = p.add_subparsers()
        p = ssp.add_parser('set')
        p.add_argument('values', action=StorePortPair, nargs='+', help='port mappings')
        p.set_defaults(task_handler=self.handle_ports_set)
        p = ssp.add_parser('unset')
        p.add_argument('values', nargs='+', help='ports')
        p.set_defaults(task_handler=self.handle_ports_unset)

        p = subp.add_parser('command', help='set command')
        p.add_argument('name')
        p.add_argument('command', nargs='?')
        p.add_argument('--remove', '-r', action='store_true')
        p.set_defaults(task_handler=self.handle_command)

        # p = subp.add_parser('link', help='manage links')
        # p.add_argument('task')
        # p.add_argument('name')
        # p.add_argument('link')
        # p.add_argument('--remove', '-r', action='store_true')
        # p.set_defaults(task_handler=self.handle_link)

        # p = subp.add_parser('run')
        # p.add_argument('name', nargs='?')
        # p.add_argument('task', nargs='?')
        # p.add_argument('--restart', '-r', action='store_true')
        # p.add_argument('--stop', '-s', action='store_true')
        # p.add_argument('--remove', '-d', action='store_true')
        # p.set_defaults(task_handler=self.handle_run)

        # p = subp.add_parser('logs', help='get logs')
        # p.add_argument('name', help='container name')
        # p.set_defaults(task_handler=self.handle_logs)

    def handle_add(self, args):
        self.add(args.name, args.image, args.update)

    def add(self, name, image_name, update=False):
        task_name = self.get_task_name()
        img = self.client.get_module('image').get_image_name(image_name)
        try:
            task = self.get_task(task_name)
        except self.CommandError:
            task = None
        if update:
            if not task:
                self.error('no such task')
            ctr_def = self.get_container_definition(task, name)
            ctr_def['image'] = img
        else:
            if not task:
                task = {
                    'family': task_name,
                    'containerDefinitions': []
                }
            for cd in task['containerDefinitions']:
                if cd['name'] == name:
                    self.error('container definition with that name already exists')
            ctr_def = {
                'name': name,
                'image': img,
                'memoryReservation': 1  # required
            }
            task['containerDefinitions'].append(ctr_def)
        self.register_task(task)

    def handle_remove(self, args):
        task_name = self.get_task_name()
        if args.name:
            task = self.get_task(task_name)
            task['containerDefinitions'] = list(filter(lambda x: x['name'] != args.name, task['containerDefinitions']))
            self.register_task(task)
        else:
            arns = self.run(
                '$aws ecs list-task-definitions'
                ' --family-prefix {}'
                ' --query=taskDefinitionArns'.format(task_name),
                capture='json'
            )
            for arn in arns:
                print('{}'.format(arn))
                self.run(
                    '$aws ecs deregister-task-definition'
                    ' --task-definition {}'.format(arn)
                )

    def handle_list(self, args):
        task_name = self.get_task_name()
        try:
            task = self.get_task(task_name, escape=False)
        except self.CommandError:
            return
        if args.name:
            ctr_def = self.get_container_definition(task, args.name)
            print(json.dumps(ctr_def, indent=2))
        else:
            for d in task['containerDefinitions']:
                print(d['name'])

    def handle_env_set(self, args):
        self.env_set(args.name, args.values)

    def env_set(self, name, values):
        task_name = self.get_task_name()
        task = self.get_task(task_name)
        ctr_def = self.get_container_definition(task, name)
        env = env_to_dict(ctr_def['environment'])
        env.update(values)
        env = dict_to_env(env)
        ctr_def['environment'] = env
        self.register_task(task)

    def handle_env_unset(self, args):
        self.env_unset(args.name, args.values)

    def env_unset(self, name, keys):
        task_name = self.get_task_name()
        task = self.get_task(task_name)
        ctr_def = self.get_container_definition(task, name)
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
        self.ports_set(args.name, args.values)

    def ports_set(self, name, values):
        task_name = self.get_task_name()
        task = self.get_task(task_name)
        ctr_def = self.get_container_definition(task, name)
        ports = ports_to_dict(ctr_def['portMappings'])
        for k, v in values.items():
            ports[int(k)] = int(v)
        ports = dict_to_ports(ports)
        ctr_def['portMappings'] = ports
        self.register_task(task)

    def handle_ports_unset(self, args):
        task_name = self.get_task_name()
        task = self.get_task(task_name)
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

    def handle_command(self, args):
        task_name = self.get_task_name()
        task = self.get_task(task_name)
        ctr_def = self.get_container_definition(task, args.name)
        if args.remove:
            try:
                del ctr_def['command']
            except KeyError:
                pass
        else:
            ctr_def['command'] = [args.command]
        self.register_task(task)

    # def handle_link(self, args):
    #     self.link(args.task, args.name, args.link, args.remove)

    # def link(self, task_name, ctr_name, link, remove=False):
    #     task = self.get_task(task_name)
    #     ctr_def = self.get_container_definition(task, ctr_name)
    #     links = ctr_def.get('links', [])
    #     if remove:
    #         ctr_def['links'] = list(set(links).difference(set([link])))
    #     else:
    #         ctr_def['links'] = list(set(links).union(set([link])))
    #     self.register_task(task)

    # def handle_run(self, args):
    #     self._run(args.name, args.task, args.restart, args.remove, args.stop)

    # def _run(self, ctr_name, replicas=1, restart=False, remove=False, stop=False):
    #     task = self.get_task(ctr_name)
    #     ctr_def = self.get_container_definition(task, ctr_name)
    #     mach_mod = self.client.get_module('machine')
    #     mach = mach_mod.get_selected()
    #     cmd = 'docker service create --name {}'.format(ctr_def['name'])
    #     if replicas:
    #         cmd += ' --replicas {}'.format(replicas)
    #     cmd += env_to_string(ctr_def.get('environment', []))
    #     cmd += ports_to_string(ctr_def.get('ports', []))
    #     if stop or remove:
    #         cmd += ' remove'
    #         if remove:
    #             cmd += ' -d'
    #         if name:
    #             cmd += ' ' + name
    #     else:
    #         cmd += ' run'
    #         if name:
    #             cmd += ' -n {}'.format(name)
    #         if task:
    #             cmd += ' -t {}'.format(task)
    #         if restart:
    #             cmd += ' -r'
    #     data = mach_mod.ssh_run(
    #         cmd,
    #         name=mach,
    #         capture='json'
    #     )
    #     if data['status'] != 'ok':
    #         print(data['result'])

    # def handle_logs(self, args):
    #     mach_mod = self.client.get_module('machine')
    #     mach = mach_mod.get_selected()
    #     cmd = 'docker logs {}'.format(args.name)
    #     mach_mod.ssh_run(
    #         cmd,
    #         name=mach,
    #         tty=True,
    #         capture=False
        # )

    def get_task(self, name, escape=True):
        # app = self.client.get_selected('app')
        # name = '%s-%s' % (app, name)
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

    def get_task_name(self):
        app = self.client.get_selected('app')
        return app
