from .module import Module
from .runner import CommandError
from .utils import (
    env_to_string, ports_to_string, env_to_dict, dict_to_env,
    mounts_to_string, volumes_to_dict
)


class Service(Module):
    dependencies = ['task']

    def __init__(self, **kwargs):
        super().__init__('service', **kwargs)

    def add_arguments(self, parser):
        subp = parser.add_subparsers(help='service help')

        p = subp.add_parser('ls', help='list services')
        p.add_argument('task', metavar='TASK', nargs='?', help='task name')
        p.set_defaults(service_handler=self.handle_list)

        p = subp.add_parser('mk', help='make a service')
        p.add_argument('task', metavar='TASK', help='task name')
        p.add_argument('--replicas', '-r', help='number of replicas')
        p.set_defaults(service_handler=self.handle_make)

        p = subp.add_parser('up', help='update a service')
        p.add_argument('task', metavar='TASK', help='task name')
        p.add_argument('--replicas', '-r', help='number of replicas')
        p.set_defaults(service_handler=self.handle_update)

        # p = subp.add_parser('remove')
        # p.add_argument('task')
        # p.add_argument('--volumes', '-v', action='store_true', help='remove volumes')
        # p.set_defaults(service_handler=self.handle_remove)

        # p = subp.add_parser('logs', help='get logs')
        # p.add_argument('name', help='container name')
        # p.set_defaults(service_handler=self.handle_logs)

    def handle_list(self, args):
        self.list(args.task)

    def list(self, task_name):
        ctx = self.get_context()
        node_mod = self.client.get_module('node')
        if task_name:
            svc_name = f'{ctx["app"]}-{task_name}'
            try:
                node_mod.mgr_run(
                    f'docker service inspect {svc_name}',
                    capture=False
                )
            except CommandError:
                pass
        else:
            node_mod.mgr_run(
                'docker service ls',
                capture=False
            )

    def handle_make(self, args):
        self.make(args.task, args.replicas)

    def make(self, task_name, replicas=None):
        ctx = self.get_context()
        task_mod = self.client.get_module('task')
        app_task = task_mod.get_app_task()
        try:
            env = env_to_dict(task_mod.get_container_definition(app_task, '_', fail=False)['environment'])
        except TypeError:
            env = {}
        ctr_def = task_mod.get_container_definition(app_task, task_name)
        svc_name = f'{ctx["app"]}-{ctr_def["name"]}'
        cmd = f'$(aws --region {ctx["region"]} ecr get-login);'
        cmd += f' docker pull {ctr_def["image"]};'
        cmd += f' docker service create'
        cmd += f' --name {svc_name}'
        if replicas:
            cmd += f' --replicas {replicas}'
        if ctr_def.get('cpu', None):
            cmd += f' --reserve-cpu {ctr_def["cpu"]}'
        if ctr_def.get('memoryReservation', None):
            cmd += f' --reserve-memory {int(ctr_def["memoryReservation"]) * 1048576}'  # convert to bytes
        cmd += ' --with-registry-auth'
        env.update(env_to_dict(ctr_def.get('environment', [])))
        cmd += env_to_string(
            dict_to_env(env),
            opt='-e'
        )
        cmd += ports_to_string(
            ctr_def.get('portMappings', []),
            opt='-p'
        )
        cmd += ' --network all'
        cmd += mounts_to_string(
            ctr_def.get('mountPoints', {}),
            self.get_mounts(task_name),
            opt='--mount'
        )
        cmd += ' ' + ctr_def['image']
        if ctr_def.get('command', None):
            cmd += ' ' + ' '.join(ctr_def['command'])
        cmd = '\'' + cmd + '\''
        self.make_volumes(volumes_to_dict(app_task.get('volumes', [])))
        node_mod = self.client.get_module('node')
        try:
            node_mod.mgr_run(
                cmd,
                capture='discard'
            )
        except CommandError as e:
            self.error('failed to launch service, please check the task command')

    def handle_update(self, args):
        self.update(args.task, args.replicas)

    def update(self, task_name, replicas=None):
        ctx = self.get_context()
        task_mod = self.client.get_module('task')
        app_task = task_mod.get_app_task()
        try:
            env = env_to_dict(task_mod.get_container_definition(app_task, '_', fail=False)['environment'])
        except TypeError:
            env = {}
        ctr_def = task_mod.get_container_definition(app_task, task_name)
        svc_name = f'{ctx["app"]}-{ctr_def["name"]}'
        cmd = f'$(aws --region {ctx["region"]} ecr get-login);'
        cmd += f' docker pull {ctr_def["image"]};'
        cmd += f' docker service update'
        if replicas:
            cmd += f' --replicas {replicas}'
        if ctr_def.get('cpu', None):
            cmd += f' --reserve-cpu {ctr_def["cpu"]}'
        if ctr_def.get('memoryReservation', None):
            cmd += f' --reserve-memory {int(ctr_def["memoryReservation"]) * 1048576}'  # convert to bytes
        cmd += ' --with-registry-auth'
        env.update(env_to_dict(ctr_def.get('environment', [])))
        cmd += env_to_string(
            dict_to_env(env),
            opt='--env-add'
        )
        cmd += ports_to_string(
            ctr_def.get('portMappings', []),
            opt='--publish-add'
        )
        cmd += mounts_to_string(
            ctr_def.get('mountPoints', {}),
            self.get_mounts(task_name),
            opt='--mount-add'
        )
        cmd += ' --force'
        cmd += ' --image ' + ctr_def['image']
        if ctr_def.get('command', None):
            cmd += ' --args "' + ' '.join(ctr_def['command']) + '"'
        cmd += f' {svc_name}'
        cmd = '\'' + cmd + '\''
        self.make_volumes(volumes_to_dict(app_task.get('volumes', [])))
        node_mod = self.client.get_module('node')
        try:
            node_mod.mgr_run(
                cmd,
                capture='discard'
            )
        except CommandError as e:
            self.error('failed to update service, please check the task command')

    def handle_remove(self, args):
        mach_mod = self.client.get_module('machine')
        mach = mach_mod.get_selected()
        mach_mod.ssh_run(
            'docker service rm {}'.format(args.task),
            name=mach,
            capture='discard'
        )
        if args.volumes:
            task_mod = self.client.get_module('task')
            task = task_mod.get_task(task_mod.get_task_name())
            self.delete_volumes(volumes_to_dict(task.get('volumes', [])))

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
        task_mod = self.client.get_module('task')
        return task_mod.get_task(name, escape)

    def get_container_definition(self, task, name):
        task_mod = self.client.get_module('task')
        return task_mod.get_container_definition(task, name)

    def make_volumes(self, volumes):
        node_mod = self.client.get_module('node')
        for name, src in volumes.items():
            cmd = f'docker volume create --name {name}'
            try:
                mach_mod.mgr_run(
                    cmd,
                    capture='discard'
                )
            except self.CommandError:
                pass

    def delete_volumes(self, volumes):
        mach_mod = self.client.get_module('machine')
        mach = mach_mod.get_selected()
        for name, src in volumes.items():
            cmd = 'docker volume rm {}'.format(name)
            try:
                mach_mod.ssh_run(
                    cmd,
                    name=mach,
                    capture='discard'
                )
            except self.CommandError:
                pass

    def get_mounts(self, task):
        node_mod = self.client.get_module('node')
        try:
            data = node_mod.mgr_run(
                f'docker service inspect {task}',
                capture='json'
            )
        except CommandError:
            data = None
        if data:
            mounts = data[0]['Spec']['TaskTemplate']['ContainerSpec'].get('Mounts', [])
            mounts = dict([(m['Source'], m['Target']) for m in mounts])
            return mounts
        else:
            return {}

    def get_my_context(self):
        return {}
