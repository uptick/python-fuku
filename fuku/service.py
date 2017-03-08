from .module import Module
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

        p = subp.add_parser('add')
        p.add_argument('task')
        p.add_argument('--replicas', '-r')
        p.add_argument('--update', '-u', action='store_true')
        p.set_defaults(service_handler=self.handle_add)

        p = subp.add_parser('remove')
        p.add_argument('task')
        p.add_argument('--volumes', '-v', action='store_true', help='remove volumes')
        p.set_defaults(service_handler=self.handle_remove)

        p = subp.add_parser('list')
        p.add_argument('task', nargs='?')
        p.set_defaults(service_handler=self.handle_list)

        p = subp.add_parser('logs', help='get logs')
        p.add_argument('name', help='container name')
        p.set_defaults(service_handler=self.handle_logs)

    def handle_add(self, args):
        self.add(args.task, args.replicas, args.update)

    def add(self, task_name, replicas=None, update=False):
        task_mod = self.client.get_module('task')
        task = task_mod.get_task(task_mod.get_task_name())
        try:
            env = env_to_dict(task_mod.get_container_definition(task, '_')['environment'])
        except IndexError:
            env = {}
        ctr_def = task_mod.get_container_definition(task, task_name)
        cmd = '$dollar(aws --region $region ecr get-login);'
        cmd += ' docker pull {};'.format(ctr_def['image'])
        cmd += ' docker service {}'.format(
            'update' if update else 'create'
        )
        if not update:
            cmd += ' --name {}'.format(ctr_def['name'])
        if replicas:
            cmd += ' --replicas {}'.format(replicas)
        env.update(env_to_dict(ctr_def.get('environment', [])))
        cmd += env_to_string(
            dict_to_env(env),
            opt='--env-add' if update else '-e'
        )
        cmd += ports_to_string(
            ctr_def.get('portMappings', []),
            opt='--publish-add' if update else '-p'
        )
        if not update:
            cmd += ' --network all'
        cmd += mounts_to_string(
            ctr_def.get('mountPoints', {}),
            self.get_mounts(task_name),
            opt='--mount-add' if update else '--mount'
        )
        if update:
            cmd += ' --force'
        if update:
            cmd += ' --image '
        else:
            cmd += ' '
        cmd += ctr_def['image']
        if ctr_def.get('command', None):
            if update:
                cmd += ' --args "'
            else:
                cmd += ' '
            cmd += ' '.join(ctr_def['command'])
            if update:
                cmd += '"'
        if update:
            cmd += ' ' + ctr_def['name']
        cmd = '\'' + cmd + '\''
        self.create_volumes(volumes_to_dict(task.get('volumes', [])))
        mach_mod = self.client.get_module('machine')
        mach = mach_mod.get_selected()
        try:
            mach_mod.ssh_run(
                cmd,
                name=mach,
                capture='discard'
            )
        except self.CommandError:
            self.error('failed to launch service, please check the task command')

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

    def handle_list(self, args):
        mach_mod = self.client.get_module('machine')
        mach = mach_mod.get_selected()
        if args.task:
            mach_mod.ssh_run(
                'docker service inspect {}'.format(args.task),
                name=mach,
                capture=False
            )
        else:
            mach_mod.ssh_run(
                'docker service ls',
                name=mach,
                capture=False
            )

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

    def create_volumes(self, volumes):
        mach_mod = self.client.get_module('machine')
        mach = mach_mod.get_selected()
        for name, src in volumes.items():
            cmd = 'docker volume create --name {}'.format(name)
            try:
                mach_mod.ssh_run(
                    cmd,
                    name=mach,
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
        mach_mod = self.client.get_module('machine')
        mach = mach_mod.get_selected()
        try:
            data = mach_mod.ssh_run(
                'docker service inspect {}'.format(task),
                name=mach,
                capture='json'
            )
        except self.CommandError:
            data = None
        if data:
            mounts = data[0]['Spec']['TaskTemplate']['ContainerSpec'].get('Mounts', [])
            mounts = dict([(m['Source'], m['Target']) for m in mounts])
            return mounts
        else:
            return {}
