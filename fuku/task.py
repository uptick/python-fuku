import json

from .module import Module
from .utils import (
    StoreKeyValuePair,
    StorePortPair,
    dict_to_env,
    dict_to_mounts,
    dict_to_ports,
    dict_to_volumes,
    env_to_dict,
    mounts_to_dict,
    ports_to_dict,
    volumes_to_dict,
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
        p.add_argument('--memory', '-m', help='memory reservation (MiB)')
        p.set_defaults(task_handler=self.handle_make)

        p = subp.add_parser('up', help='update a task')
        p.add_argument('name', metavar='NAME', help='task name')
        p.add_argument('--image', '-i', metavar='IMAGE', help='image name')
        p.add_argument('--cpu', '-c', help='cpu reservation')
        p.add_argument('--memory', '-m', help='memory reservation (MiB)')
        p.set_defaults(task_handler=self.handle_update)

        p = subp.add_parser('rm', help='remove a task')
        p.add_argument('name', metavar='NAME', help='task name')
        p.set_defaults(task_handler=self.handle_remove)

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

        p = subp.add_parser('logs', help='set log destination')
        p.add_argument('name', metavar='NAME', help='task name')
        p.add_argument('driver', metavar='DRIVER', default='awslogs', choices=['aws', 'syslog'], help='task name')
        p.add_argument('options', metavar='OPTIONS', action=StoreKeyValuePair, nargs='*', help='driver options')
        p.set_defaults(task_handler=self.handle_logs)

        p = subp.add_parser('prune', help='remove unused task definitions')
        p.set_defaults(task_handler=self.handle_prune)

    def handle_list(self, args):
        self.list(args.name)

    def list(self, name):
        if name:
            task = self.get_task(name)
            ctr_def = self.get_container_definition(task, name)
            print(json.dumps(ctr_def, indent=2))
        else:
            for task in self.iter_task_families(name):
                ii = task.rfind('-') + 1
                print(task[ii:])

    def handle_make(self, args):
        self.make(args.name, args.image, args.cpu, args.memory)

    def make(self, name, image_name, cpu=None, memory=None, logs=True):
        ctx = self.get_context()
        img_uri = self.client.get_module('image').image_name_to_uri(image_name)
        task = self.get_task(name, ctx=ctx, fail=False)
        if task:
            self.error(f'task "{name}" already exists')
        task = {
            'family': self.get_task_family(name),
            'containerDefinitions': []
        }
        ctr_def = {
            'name': name,
            'image': img_uri,
            'memory': int(memory or 4),
            'memoryReservation': int(memory or 4)
        }
        if logs:
            ctr_def['logConfiguration'] = {
                'logDriver': 'awslogs',
                'options': {
                    'awslogs-group': f'/{ctx["cluster"]}',
                    'awslogs-region': ctx['region'],
                    'awslogs-stream-prefix': ctx['app']
                }
            }
        if cpu is not None:
            ctr_def['cpu'] = cpu
        task['containerDefinitions'].append(ctr_def)
        self.register_task(task)

    def handle_remove(self, args):
        self.remove(args.name)

    def remove(self, name):
        self.confirm_remove(name)

        # # Deregister the task definitions.
        # ecs_cli = self.get_boto_client('ecs')
        # paginator = ecs_cli.get_paginator('list_task_definitions')
        # family = self.get_task_family(name)
        # task_defs = paginator.paginate(
        #     familyPrefix=family
        # )
        # for results in task_defs:
        #     for arn in results['taskDefinitionArns']:
        #         ecs_cli.deregister_task_definition(taskDefinition=arn)

        # # Also deregister the launch task definitions (prefixed with underbar).
        # task_defs = paginator.paginate(
        #     familyPrefix='_' + family
        # )
        # for results in task_defs:
        #     for arn in results['taskDefinitionArns']:
        #         ecs_cli.deregister_task_definition(taskDefinition=arn)

    def handle_update(self, args):
        self.update(args.name, args.image, args.cpu, args.memory)

    def update(self, name, image_name=None, cpu=None, memory=None):
        ctx = self.get_context()
        task = self.get_task(name, ctx=ctx)
        ctr_def = self.get_container_definition(task, name)
        if image_name:
            img_uri = self.client.get_module('image').image_name_to_uri(image_name)
            ctr_def['image'] = img_uri
        if cpu is not None:
            ctr_def['cpu'] = int(cpu)
        if memory is not None:
            ctr_def['memory'] = int(memory)
            ctr_def['memoryReservation'] = int(memory)
        self.register_task(task)

    def handle_env_list(self, args):
        self.env_list(args.name)

    def env_list(self, name):
        task = self.get_task(name)
        ctr_def = self.get_container_definition(task, name)
        env = env_to_dict(ctr_def['environment'])
        for k in sorted(env.keys()):
            print('%s=%s' % (k, env[k]))

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
        task = self.get_task(name)
        ctr_def = self.get_container_definition(task, name)
        env = env_to_dict(ctr_def['environment'])
        env.update(to_set)
        env = dict_to_env(env)
        ctr_def['environment'] = env
        self.register_task(task)

    def handle_env_unset(self, args):
        self.env_unset(args.name, args.values)

    def env_unset(self, name, keys):
        task = self.get_task(name)
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

    def handle_ports_list(self, args):
        self.ports_list(args.name)

    def ports_list(self, name):
        task = self.get_task(name)
        ctr_def = self.get_container_definition(task, name)
        ports = ports_to_dict(ctr_def['portMappings'])
        for k, v in ports.items():
            print('%s:%s' % (k, v))

    def handle_ports_set(self, args):
        self.ports_set(args.name, args.values)

    def ports_set(self, name, values):
        task = self.get_task(name)
        ctr_def = self.get_container_definition(task, name)
        ports = ports_to_dict(ctr_def['portMappings'])
        for k, v in values.items():
            ports[int(k)] = int(v)
        ports = dict_to_ports(ports)
        ctr_def['portMappings'] = ports
        self.register_task(task)

    def handle_ports_unset(self, args):
        self.ports_unset(args.name, args.values)

    def ports_unset(self, name, values):
        app_task = self.get_app_task()
        ctr_def = self.get_container_definition(app_task, name)
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

    def volume_add(self, task_name, vol_name, dst, src=None, read_only=False):
        task = self.get_task(task_name)
        ctr_def = self.get_container_definition(task, task_name)
        volumes = volumes_to_dict(task['volumes'])
        volumes[vol_name] = src
        task['volumes'] = dict_to_volumes(volumes)
        mounts = mounts_to_dict(ctr_def['mountPoints'])
        mounts[vol_name] = {
            'containerPath': dst,
            'readOnly': read_only
        }
        ctr_def['mountPoints'] = dict_to_mounts(mounts)
        self.register_task(task)

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
        task = self.get_task(name)
        ctr_def = self.get_container_definition(task, name)
        if remove:
            try:
                del ctr_def['command']
            except KeyError:
                pass
        else:
            ctr_def['command'] = cmd.split()
        self.register_task(task)

    def handle_logs(self, args):
        self.logs(args.name, args.driver, args.options)

    def logs(self, name, driver, options):
        ctx = self.get_context()
        task = self.get_task(name, ctx=ctx)
        ctr_def = self.get_container_definition(task, name)
        if driver == 'aws':
            ctr_def['logConfiguration'] = {
                'logDriver': 'awslogs',
                'options': {
                    'awslogs-group': f'/{ctx["cluster"]}',
                    'awslogs-region': ctx['region'],
                    'awslogs-stream-prefix': ctx['app']
                }
            }
        elif driver == 'syslog':
            opts = {
                'tag': '{{ (.ExtraAttributes nil).TASK_NAME }}/{{ .ID }}',
                'env': 'TASK_NAME'
            }
            opts.update(options)
            ctr_def['logConfiguration'] = {
                'logDriver': 'syslog',
                'options': opts
            }
        elif driver == 'none':
            try:
                del ctr_def['logConfiguration']
            except KeyError:
                pass
        self.register_task(task)

    def handle_prune(self, args):
        self.prune()

    def prune(self):
        ecs_cli = self.get_boto_client('ecs')
        for fam in self.iter_task_families():
            for prefix in ['', '_']:
                paginator = ecs_cli.get_paginator('list_task_definitions')
                response = paginator.paginate(
                    familyPrefix=prefix + fam,
                    sort='DESC'
                )
                first = True
                for results in response:
                    for arn in results['taskDefinitionArns']:
                        if not first:
                            ecs_cli.deregister_task_definition(
                                taskDefinition=arn
                            )
                        else:
                            first = False

    def get_task_family(self, name, ctx=None):
        if ctx is None:
            ctx = self.get_context()
        return '-'.join(['fuku', ctx['cluster'], ctx['app']] + ([name] if name else []))

    def get_task(self, name, ctx=None, fail=True):
        family = self.get_task_family(name, ctx)
        ecs = self.get_boto_client('ecs')
        try:
            return ecs.describe_task_definition(
                taskDefinition=family
            )['taskDefinition']
        except:
            if fail:
                self.error(f'no task "{name}"')

    def iter_task_families(self, name=None):
        ctx = self.get_context()
        ecs_cli = self.get_boto_client('ecs')
        paginator = ecs_cli.get_paginator('list_task_definition_families')
        prefix = f'fuku-{ctx["cluster"]}-{ctx["app"]}'
        tasks = paginator.paginate(
            familyPrefix=prefix,
            status='ACTIVE'
        )
        for t in tasks:
            for f in t['families']:
                if f != prefix:
                    yield f

    def register_task(self, task):
        ecs = self.get_boto_client('ecs')
        skip = set(['taskDefinitionArn', 'revision', 'status', 'requiresAttributes'])
        ecs.register_task_definition(**dict([
            (k, v) for k, v in task.items() if k not in skip
        ]))

    def get_container_definition(self, task, name, fail=True):
        if name is None:
            name = self.get_context()['app']
        try:
            return list(filter(lambda x: x['name'] == name, task['containerDefinitions']))[0]
        except (KeyError, IndexError):
            if fail:
                self.error(f'container definition "{name}" does not exist')

    def get_my_context(self):
        return {}
