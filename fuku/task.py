import json

from .module import Module
from .utils import StoreKeyValuePair, env_to_dict, dict_to_env


class Task(Module):
    dependencies = ['app']

    def __init__(self, **kwargs):
        super().__init__('task', **kwargs)

    def add_arguments(self, parser):
        subp = parser.add_subparsers(help='container help')

        p = subp.add_parser('list', help='list tasks')
        p.add_argument('name', nargs='?')
        p.set_defaults(task_handler=self.handle_list)

        p = subp.add_parser('add', help='add a task')
        p.add_argument('name')
        p.add_argument('image')
        p.set_defaults(task_handler=self.handle_add)

        p = subp.add_parser('env', help='add environment')
        p.add_argument('name')
        p.add_argument('values', action=StoreKeyValuePair, nargs='+', help='key value pairs')
        p.set_defaults(task_handler=self.handle_env)

        # uregp = subp.add_parser('unregister', help='unregister a container')
        # uregp.add_argument('name', help='container name')
        # uregp.set_defaults(container_handler=self.unregister)

        # pushp = subp.add_parser('push', help='push a container')
        # pushp.add_argument('name', help='container name')
        # pushp.set_defaults(container_handler=self.push)

        # lnchp = subp.add_parser('launch', help='launch a container')
        # lnchp.add_argument('name', help='container name')
        # lnchp.set_defaults(container_handler=self.launch)

        # remp = subp.add_parser('remove', help='remove a profile')
        # remp.add_argument('name', help='profile name')
        # remp.set_defaults(profile_handler=self.remove)

        # selp = subp.add_parser('select', help='select a machine')
        # selp.add_argument('instance_id', help='machine instance ID')
        # selp.set_defaults(machine_handler=self.select)

    def handle_list(self, args):
        if args.name:
            task = self.get_task(args.name)
            print(json.dumps(task, indent=2))
        else:
            app = self.client.get_selected('app')
            data = self.run(
                '$aws ecs list-task-definition-families'
                ' --query families',
                capture='json'
            )
            pre = '%s-' % app
            for d in data:
                if d.startswith(pre):
                    print(d[len(pre):])

    def handle_add(self, args):
        app = self.client.get_selected('app')
        img_mod = self.client.get_module('image')
        img = img_mod.get_uri(args.image)
        name = '%s-%s' % (app, args.name)
        ctr_def = {
            'name': name,
            'image': img,
            'memoryReservation': 1  # required
        }
        self.register_task(ctr_def)

    def handle_env(self, args):
        task = self.get_task(args.name)
        ctr_def = task['containerDefinitions'][0]
        env = env_to_dict(ctr_def['environment'])
        env.update(args.values)
        env = dict_to_env(env)
        ctr_def['environment'] = env
        self.register_task(ctr_def)

    def get_task(self, name):
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
        return data

    def register_task(self, ctr_def):
        name = ctr_def['name']
        self.run(
            '$aws ecs register-task-definition'
            ' --family {}'
            ' --container-definitions \'{}\''
            .format(
                name,
                json.dumps([ctr_def])
            )
        )
