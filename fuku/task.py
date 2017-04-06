import json

from .module import Module
from .utils import (
    StoreKeyValuePair, StorePortPair,
    env_to_dict, dict_to_env,
    ports_to_dict, dict_to_ports,
    volumes_to_dict, dict_to_volumes,
    mounts_to_dict, dict_to_mounts,
    entity_already_exists
)


class Task(Module):
    dependencies = ['app']

    def __init__(self, **kwargs):
        super().__init__('task', **kwargs)

    def add_arguments(self, parser):
        subp = parser.add_subparsers(help='task help')

        p = subp.add_parser('ls', help='list tasks')
        p.add_argument('name', metavar='NAME', nargs='?', help='task name')
        p.set_defaults(task_handler=self.handle_list)

        p = subp.add_parser('mk', help='make a task')
        p.add_argument('name', metavar='NAME', help='task name')
        p.add_argument('image', metavar='IMAGE', help='image name')
        p.add_argument('--cpu', '-c', help='cpu reservation')
        p.add_argument('--memory', '-m', help='memory reservation')
        p.set_defaults(task_handler=self.handle_make)

        p = subp.add_parser('up', help='update a task')
        p.add_argument('name', metavar='NAME', help='task name')
        p.add_argument('image', metavar='IMAGE', help='image name')
        p.add_argument('--cpu', '-c', help='cpu reservation')
        p.add_argument('--memory', '-m', help='memory reservation')
        p.set_defaults(task_handler=self.handle_update)

        # p = subp.add_parser('remove', help='remove a task')
        # p.add_argument('name')
        # p.set_defaults(task_handler=self.handle_remove)

        p = subp.add_parser('env', help='manage environment')
        p.add_argument('--name', '-n', help='task name')
        ssp = p.add_subparsers()
        p = ssp.add_parser('ls')
        p.set_defaults(task_handler=self.handle_env_list)
        p = ssp.add_parser('set')
        p.add_argument('--file', '-f', help='load from file')
        p.add_argument('values', metavar='VALUES', action=StoreKeyValuePair, nargs='*', help='key value pairs')
        p.set_defaults(task_handler=self.handle_env_set)
        p = ssp.add_parser('unset')
        p.add_argument('values', metavar='VALUES', nargs='+', help='keys to remove')
        p.set_defaults(task_handler=self.handle_env_unset)

        p = subp.add_parser('ports', help='manage ports')
        p.add_argument('name', metavar='NAME', help='task name')
        ssp = p.add_subparsers()
        p = ssp.add_parser('ls')
        p.set_defaults(task_handler=self.handle_ports_list)
        p = ssp.add_parser('set')
        p.add_argument('values', metavar='VALUES', action=StorePortPair, nargs='+', help='port mappings')
        p.set_defaults(task_handler=self.handle_ports_set)
        p = ssp.add_parser('unset')
        p.add_argument('values', metavar='VALUES', nargs='+', help='ports')
        p.set_defaults(task_handler=self.handle_ports_unset)

        p = subp.add_parser('volume', help='manage volumes')
        p.add_argument('name', metavar='NAME', help='task name')
        ssp = p.add_subparsers()
        p = ssp.add_parser('add')
        p.add_argument('volume', metavar='VOLUME', help='volume name')
        p.add_argument('destination', metavar='DEST', help='container mount point')
        p.add_argument('--source', '-s', help='source path')
        p.set_defaults(task_handler=self.handle_volume_add)
        p = ssp.add_parser('remove')
        p.add_argument('volume', metavar='VOLUME', help='volume name')
        p.set_defaults(task_handler=self.handle_volume_remove)

        p = subp.add_parser('command', help='set command')
        p.add_argument('name', metavar='NAME', help='task name')
        p.add_argument('command', metavar='COMMAND', nargs='?', help='command to run')
        p.add_argument('--remove', '-r', action='store_true')
        p.set_defaults(task_handler=self.handle_command)

    def handle_list(self, args):
        self.list(args.name)

    def list(self, name):
        app_task = self.get_app_task()
        if not app_task:
            return
        if name:
            ctr_def = self.get_container_definition(app_task, name)
            print(json.dumps(ctr_def, indent=2))
        else:
            for cd in app_task['containerDefinitions']:
                if cd['name'] != '_':
                    print(cd['name'])

    def handle_make(self, args):
        self.make(args.name, args.image, args.cpu, args.memory)

    def make(self, name, image_name, cpu=None, memory=None):
        ctx = self.get_context()
        img = self.client.get_module('image').image_name_to_uri(image_name)
        app_task = self.get_app_task(ctx=ctx)
        if not app_task:
            app_task = {
                'family': ctx['app'],
                'containerDefinitions': []
            }
        for cd in app_task['containerDefinitions']:
            if cd['name'] == name:
                self.error('container definition with that name already exists')
        ctr_def = {
            'name': name,
            'image': img,
            'memoryReservation': int(memory or 1)
        }
        if cpu is not None:
            ctr_def['cpu'] = cpu
        app_task['containerDefinitions'].append(ctr_def)
        self.register_task(app_task)

    # def handle_remove(self, args):
    #     task_name = self.get_task_name()
    #     if args.name:
    #         task = self.get_task(task_name)
    #         task['containerDefinitions'] = list(filter(lambda x: x['name'] != args.name, task['containerDefinitions']))
    #         self.register_task(task)
    #     else:
    #         arns = self.run(
    #             '$aws ecs list-task-definitions'
    #             ' --family-prefix {}'
    #             ' --query=taskDefinitionArns'.format(task_name),
    #             capture='json'
    #         )
    #         for arn in arns:
    #             print('{}'.format(arn))
    #             self.run(
    #                 '$aws ecs deregister-task-definition'
    #                 ' --task-definition {}'.format(arn)
    #             )

    def handle_update(self, args):
        self.update(args.name, args.image, args.cpu, args.memory)

    def update(self, name, image_name, cpu=None, memory=None):
        ctx = self.get_context()
        img = self.client.get_module('image').image_name_to_uri(image_name)
        app_task = self.get_app_task(ctx=ctx)
        if not app_task:
            self.error('task not found')
        ctr_def = self.get_container_definition(app_task, name)
        ctr_def['image'] = img
        if cpu is not None:
            ctr_def['cpu'] = int(cpu)
        if memory is not None:
            ctr_def['memoryReservation'] = int(memory)
        self.register_task(app_task)

    def handle_env_list(self, args):
        self.env_list(args.name)

    def env_list(self, name):
        app_task = self.get_app_task()
        if not name:
            name = '_'
            try:
                ctr_def = self.get_container_definition(app_task, name)
            except IndexError:
                return
        else:
            ctr_def = self.get_container_definition(app_task, name)
        env = env_to_dict(ctr_def['environment'])
        for k, v in env.items():
            print('%s=%s' % (k, v))

    def handle_env_set(self, args):
        self.env_set(args.name, values=args.values, file=args.file)

    def env_set(self, name, values=None, file=None):
        to_set = {}
        if file is not None:
            with open(file, 'r') as inf:
                for line in inf:
                    line = line.strip()
                    ii = line.find('=')
                    if ii == -1:
                        continue
                    k = line[:ii]
                    v = line[ii + 1:]
                    to_set[k] = v
        to_set.update(values)
        app_task = self.get_app_task()
        if not name:
            name = '_'
            ecr = self.get_boto_client('ecr')
            with entity_already_exists():
                ecr.create_repository(
                    repositoryName='fuku'
                )
            ctr_def = self.get_container_definition(app_task, name, fail=False)
            if not ctr_def:
                img = self.client.get_module('image').get_uri('/fuku')
                ctr_def = {
                    'name': name,
                    'image': img,
                    'environment': [],
                    'memoryReservation': 1  # required,
                }
                app_task['containerDefinitions'].append(ctr_def)
        else:
            ctr_def = self.get_container_definition(app_task, name)
        env = env_to_dict(ctr_def['environment'])
        env.update(to_set)
        env = dict_to_env(env)
        ctr_def['environment'] = env
        self.register_task(app_task)

    def handle_env_unset(self, args):
        self.env_unset(args.name, args.values)

    def env_unset(self, name, keys):
        app_task = self.get_app_task()
        if not name:
            name = '_'
        ctr_def = self.get_container_definition(app_task, name)
        env = env_to_dict(ctr_def['environment'])
        for k in keys:
            try:
                del env[k]
            except KeyError:
                pass
        env = dict_to_env(env)
        ctr_def['environment'] = env
        self.register_task(app_task)

    def handle_ports_list(self, args):
        self.ports_list(args.name)

    def ports_list(self, name):
        app_task = self.get_app_task()
        ctr_def = self.get_container_definition(app_task, name)
        ports = ports_to_dict(ctr_def['portMappings'])
        for k, v in ports.items():
            print('%s:%s' % (k, v))

    def handle_ports_set(self, args):
        self.ports_set(args.name, args.values)

    def ports_set(self, name, values):
        app_task = self.get_app_task()
        ctr_def = self.get_container_definition(app_task, name)
        ports = ports_to_dict(ctr_def['portMappings'])
        for k, v in values.items():
            ports[int(k)] = int(v)
        ports = dict_to_ports(ports)
        ctr_def['portMappings'] = ports
        self.register_task(app_task)

    def handle_ports_unset(self, args):
        self.ports_unset(args.name, args.values)

    def ports_unset(self, name, values):
        app_task = self.get_app_task()
        ctr_def = self.get_container_definition(task, name)
        ports = ports_to_dict(ctr_def['portMappings'])
        for p in values:
            try:
                del ports[int(p)]
            except KeyError:
                pass
        ports = dict_to_ports(ports)
        ctr_def['portMappings'] = ports
        self.register_task(app_task)

    def handle_volume_add(self, args):
        self.volume_add(args.name, args.volume, args.destination, args.source)

    def volume_add(self, ctr_name, vol_name, dst, src=None):
        app_task = self.get_app_task()
        ctr_def = self.get_container_definition(app_task, ctr_name)
        volumes = volumes_to_dict(app_task['volumes'])
        volumes[vol_name] = src
        app_task['volumes'] = dict_to_volumes(volumes)
        mounts = mounts_to_dict(ctr_def['mountPoints'])
        mounts[vol_name] = dst
        ctr_def['mountPoints'] = dict_to_mounts(mounts)
        self.register_task(app_task)

    def handle_volume_remove(self, args):
        self.volume_remove(args.name, args.volume)

    def volume_remove(self, ctr_name, vol_name):
        app_task = self.get_app_task()
        ctr_def = self.get_container_definition(app_task, ctr_name)
        volumes = volumes_to_dict(app_task['volumes'])
        try:
            del volumes[vol_name]
        except KeyError:
            pass
        app_task['volumes'] = dict_to_volumes(volumes)
        mounts = mounts_to_dict(ctr_def['mountPoints'])
        try:
            del mounts[vol_name]
        except KeyError:
            pass
        ctr_def['mountPoints'] = dict_to_mounts(mounts)
        self.register_task(app_task)

    def handle_command(self, args):
        self.command(args.name, args.command, args.remove)

    def command(self, name, cmd, remove=False):
        app_task = self.get_app_task()
        ctr_def = self.get_container_definition(app_task, name)
        if remove:
            try:
                del ctr_def['command']
            except KeyError:
                pass
        else:
            ctr_def['command'] = [cmd]
        self.register_task(app_task)

    def get_app_task(self, ctx=None):
        if ctx is None:
            ctx = self.get_context()
        ecs = self.get_boto_client('ecs')
        try:
            return ecs.describe_task_definition(
                taskDefinition=ctx['app']
            )['taskDefinition']
        except:
            pass

    def register_task(self, task):
        ecs = self.get_boto_client('ecs')
        skip = set(['taskDefinitionArn', 'revision', 'status', 'requiresAttributes'])
        ecs.register_task_definition(**dict([
            (k, v) for k, v in task.items() if k not in skip
        ]))

    def get_container_definition(self, task, name, fail=True):
        try:
            return list(filter(lambda x: x['name'] == name, task['containerDefinitions']))[0]
        except (KeyError, IndexError):
            if fail:
                self.error(f'task "{name}" does not exist')

    def get_my_context(self):
        return {}
