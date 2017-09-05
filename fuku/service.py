import json

from .module import Module
from .runner import CommandError
from .utils import (
    json_serial,
    StoreKeyValuePair,
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
        p.add_argument('--min-healthy', default=50, help='minimum healthy tasks (%%)')
        p.add_argument('--placement', '-p', action=StoreKeyValuePair, nargs='*', help='set placement')
        p.set_defaults(service_handler=self.handle_make)

        p = subp.add_parser('up', help='update a service')
        p.add_argument('task', metavar='TASK', help='task name')
        p.add_argument('--replicas', '-r', help='number of replicas')
        p.add_argument('--placement', '-p', action=StoreKeyValuePair, nargs='*', help='set placement')
        p.set_defaults(service_handler=self.handle_update)

        p = subp.add_parser('scale', help='scale a service')
        p.add_argument('task', metavar='TASK', help='task name')
        p.add_argument('replicas', metavar='REPLICAS', help='number of replicas')
        p.set_defaults(service_handler=self.handle_scale)

        p = subp.add_parser('redeploy', help='redeploy all')
        p.add_argument('tasks', metavar='TASKS', nargs='*', help='task name')
        p.set_defaults(service_handler=self.handle_redeploy)

        p = subp.add_parser('rm', help='remove a service')
        p.add_argument('task', metavar='TASK', help='task name')
        p.set_defaults(service_handler=self.handle_remove)

        p = subp.add_parser('run', help='run a command')
        p.add_argument('task', metavar='TASK', help='task name')
        p.add_argument('command', metavar='COMMAND', nargs='+', help='command to run')
        p.set_defaults(service_handler=self.handle_run)

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
            svc_name = self.get_name()
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
        svc_name = self.get_name()
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
        self.update(args.task, args.replicas, placement=args.placement)

    def update(self, task_name, replicas=None):
        ctx = self.get_context()
        task_mod = self.client.get_module('task')
        app_task = task_mod.get_app_task()
        try:
            env = env_to_dict(task_mod.get_container_definition(app_task, '_', fail=False)['environment'])
        except TypeError:
            env = {}
        ctr_def = task_mod.get_container_definition(app_task, task_name)
        svc_name = self.get_name()
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
        self.remove(args.task)

    # def handle_remove(self, args):
    #     mach_mod = self.get_module('node')
    #     mach = mach_mod.get_selected()
    #     mach_mod.ssh_run(
    #         'docker service rm {}'.format(args.task),
    #         name=mach,
    #         capture='discard'
    #     )
    #     if args.volumes:
    #         task_mod = self.client.get_module('task')
    #         task = task_mod.get_task(task_mod.get_task_name())
    #         self.delete_volumes(volumes_to_dict(task.get('volumes', [])))

    def handle_run(self, args):
        self.run(args.task, args.command)

    def run(self, task_name, command):
        task_mod = self.get_module('task')
        ecs_cli = self.get_boto_client('ecs')
        ec2 = self.get_boto_resource('ec2')
        ctx = self.get_context()
        family = '_' + task_mod.get_task_family(task_name)
        cluster = f'fuku-{ctx["cluster"]}'
        task_arns = ecs_cli.list_tasks(
            cluster=cluster,
            family=family,
            desiredStatus='RUNNING'
        )['taskArns']
        if not task_arns:
            self.error('no running tasks for that service')
        # task_id = task_arns[0][task_arns[0].rfind('/') + 1:]
        cinst_arn = ecs_cli.describe_tasks(
            cluster=cluster,
            tasks=[
                task_arns[0]
            ]
        )['tasks'][0]['containerInstanceArn']
        inst_id = ecs_cli.describe_container_instances(
            cluster=cluster,
            containerInstances=[
                cinst_arn
            ]
        )['containerInstances'][0]['ec2InstanceId']
        inst = ec2.Instance(inst_id)
        cmd = f'docker exec -it `docker ps | grep {family} | awk \'{{ print $1 }}\' | head -1` {" ".join(command)}'
        node_mod = self.get_module('node')
        node_mod.ssh_run(cmd, inst=inst, tty=True)

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

    def handle_redeploy(self, args):
        self.redeploy(args.tasks)

    def redeploy(self, task_names):
        if not task_names:
            for svc in self.iter_services():
                self.update(svc)
        else:
            for tn in task_names:
                self.update(tn)

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

    def get_name(self, task_name):
        ctx = self.get_context()
        return f'{ctx["app"]}-{task_name}'

    def dependency_removal(self, module, id):
        if module.name == 'task':
            if self.is_running(id):
                return [
                    {
                        'module': self,
                        'id': id
                    }
                ]
        return []


class EcsService(Service):
    def add_arguments(self, parser):
        subp = parser.add_subparsers(help='service help')

        p = subp.add_parser('ls', help='list services')
        p.add_argument('task', metavar='TASK', nargs='?', help='task name')
        p.set_defaults(service_handler=self.handle_list)

        p = subp.add_parser('mk', help='make a service')
        p.add_argument('task', metavar='TASK', help='task name')
        p.add_argument('--expose', '-e', action='store_true', help='expose through load-balancer')
        p.add_argument('--replicas', '-r', help='number of replicas')
        # p.add_argument('--mode', '-m', default='', choices=['global', ''], help='placement mode')
        p.add_argument('--placement', '-p', action=StoreKeyValuePair, nargs='*', help='set placement')
        p.add_argument('--min-healthy', default=50, help='minimum healthy tasks (%%)')
        p.add_argument('--max-healthy', default=200, help='maximum healthy tasks (%%)')
        p.set_defaults(service_handler=self.handle_make)

        p = subp.add_parser('up', help='update a service')
        p.add_argument('task', metavar='TASK', help='task name')
        p.add_argument('--replicas', '-r', help='number of replicas')
        p.add_argument('--placement', '-p', action=StoreKeyValuePair, nargs='*', help='set placement')
        # p.add_argument('--min-healthy', help='minimum healthy tasks (%%)')
        # p.add_argument('--max-healthy', default=200, help='maximum healthy tasks (%%)')
        p.set_defaults(service_handler=self.handle_update)

        p = subp.add_parser('scale', help='scale a service')
        p.add_argument('task', metavar='TASK', help='task name')
        p.add_argument('replicas', metavar='REPLICAS', help='number of replicas')
        p.set_defaults(service_handler=self.handle_scale)

        p = subp.add_parser('redeploy', help='redeploy all')
        p.add_argument('tasks', metavar='TASKS', nargs='*', help='task name')
        p.set_defaults(service_handler=self.handle_redeploy)

        p = subp.add_parser('wait', help='wait for deployment')
        p.add_argument('tasks', metavar='TASK', nargs='*', help='task name')
        p.add_argument('--stable', '-s', action='store_true', help='wait for stable services')
        p.set_defaults(service_handler=self.handle_wait)

        p = subp.add_parser('rm', help='remove a service')
        p.add_argument('task', metavar='TASK', help='task name')
        p.set_defaults(service_handler=self.handle_remove)

        p = subp.add_parser('run', help='run a command')
        p.add_argument('task', metavar='TASK', help='task name')
        p.add_argument('command', metavar='COMMAND', nargs='+', help='command to run')
        p.set_defaults(service_handler=self.handle_run)

    def list(self, task_name):
        if task_name:
            data = self.describe_service(task_name)
            # ecs = self.get_boto_client('ecs')
            # ctx = self.get_context()
            # data = ecs.describe_services(
            #     cluster=f'fuku-{ctx["cluster"]}',
            #     services=[
            #         f'fuku-{ctx["app"]}-{task_name}'
            #     ]
            # )['services'][0]
            print(json.dumps(data, default=json_serial, indent=2))
        else:
            for svc in self.iter_services(task_name):
                print(svc)

    def describe_service(self,task_name,  app_name=None):
        ecs = self.get_boto_client('ecs')
        ctx = self.get_context()
        app_name = ctx.get('app', app_name)
        data = ecs.describe_services(
            cluster=f'fuku-{ctx["cluster"]}',
            services=[
                f'fuku-{app_name}-{task_name}'
            ]
        )['services'][0]
        return data

    def handle_make(self, args):
        self.make(args.task, args.replicas, args.expose, min_healthy=args.min_healthy,
                  max_healthy=args.max_healthy, placement=args.placement)

    def make(self, task_name, replicas=None, expose=False, mode=None, min_healthy=50, max_healthy=200, placement=None):
        ctx = self.get_context()
        cluster = f'fuku-{ctx["cluster"]}'
        task_mod = self.client.get_module('task')
        app_task = task_mod.get_task(None)
        task = task_mod.get_task(task_name)
        task['family'] = '_' + task['family']
        env = env_to_dict(task_mod.get_container_definition(app_task, ctx['app'])['environment'])
        ctr_def = task_mod.get_container_definition(task, task_name)
        env.update(env_to_dict(ctr_def['environment']))
        env['TASK_NAME'] = task_name
        ctr_def['environment'] = dict_to_env(env)
        task['containerDefinitions'] = [ctr_def]
        ecs_cli = self.get_boto_client('ecs')
        skip = set(['taskDefinitionArn', 'revision', 'status', 'requiresAttributes'])
        task = ecs_cli.register_task_definition(**dict([
            (k, v) for k, v in task.items() if k not in skip
        ]))['taskDefinition']
        # TODO: Deregister previous task definitions.
        kwargs = {
            'cluster': cluster,
            'serviceName': f'fuku-{ctx["app"]}-{task_name}',
            'taskDefinition': f'{task["family"]}:{task["revision"]}',
            'desiredCount': int(replicas) if replicas is not None else 1,
            'deploymentConfiguration': {
                'maximumPercent': int(max_healthy),
                'minimumHealthyPercent': int(min_healthy)
            },
            'placementStrategy': [{
                'type': 'spread',
                'field': 'attribute:ecs.availability-zone'
            }]
        }
        # if mode == 'global':
        #     kwargs['placementConstraints'] = {
        #         'type': 'distinctInstance'
        #     }
        if placement:
            attr = list(placement.keys())[0]
            val = list(placement.values())[0]
            kwargs['placementConstraints'] = [
                {
                    'type': 'memberOf',
                    'expression': f'attribute:{attr} == {val}'
                }
            ]
        if expose:
            kwargs['loadBalancers'] = [
                {
                    'targetGroupArn': self.get_module('app').get_target_group_arn(),
                    'containerName': task_name,
                    'containerPort': 80
                }
            ]
            kwargs['role'] = 'ecsServiceRole'
        ecs_cli.create_service(**kwargs)

    def update(self, task_name, replicas=None, mode=None, placement=None):
        ctx = self.get_context()
        cluster = f'fuku-{ctx["cluster"]}'
        task_mod = self.client.get_module('task')
        app_task = task_mod.get_task(None)
        task = task_mod.get_task(task_name)
        task['family'] = '_' + task['family']
        env = env_to_dict(task_mod.get_container_definition(app_task, ctx['app'])['environment'])
        ctr_def = task_mod.get_container_definition(task, task_name)
        env.update(env_to_dict(ctr_def['environment']))
        env['TASK_NAME'] = f'{ctx["app"]}.{task_name}'
        ctr_def['environment'] = dict_to_env(env)
        task['containerDefinitions'] = [ctr_def]
        ecs_cli = self.get_boto_client('ecs')
        skip = set(['taskDefinitionArn', 'revision', 'status', 'requiresAttributes'])
        task = ecs_cli.register_task_definition(**dict([
            (k, v) for k, v in task.items() if k not in skip
        ]))['taskDefinition']
        # TODO: Deregister previous task definitions.
        kwargs = {
            'cluster': cluster,
            'service': f'fuku-{ctx["app"]}-{task_name}',
            'taskDefinition': f'{task["family"]}:{task["revision"]}',
        }
        if replicas:
            kwargs['desiredCount'] = int(replicas) if replicas is not None else 1
        ecs_cli.update_service(**kwargs)

    def handle_scale(self, args):
        self.scale(args.task, args.replicas)

    def scale(self, task_name, replicas=None):
        ctx = self.get_context()
        cluster = f'fuku-{ctx["cluster"]}'
        family = f'_fuku-{ctx["cluster"]}-{ctx["app"]}-{task_name}'
        ecs_cli = self.get_boto_client('ecs')
        task = ecs_cli.describe_task_definition(
            taskDefinition=family
        )['taskDefinition']
        ecs_cli.update_service(
            cluster=cluster,
            service=f'fuku-{ctx["app"]}-{task_name}',
            taskDefinition=f'{family}:{task["revision"]}',
            desiredCount=int(replicas) if replicas is not None else 1
        )

    def handle_wait(self, args):
        self.wait(args.tasks, args.stable)

    def wait(self, task_names, stable):
        if not task_names:
            task_names = list(self.iter_services())
        ecs = self.get_boto_client('ecs')
        ctx = self.get_context()
        waiter = ecs.get_waiter('services_stable')
        waiter.wait(
            cluster=f'fuku-{ctx["cluster"]}',
            services=[
                f'fuku-{ctx["app"]}-{n}'
                for n in task_names
            ]
        )

    def remove(self, task_name):
        self.confirm_remove(task_name)
        self.scale(task_name, 0)
        ctx = self.get_context()
        cluster = f'fuku-{ctx["cluster"]}'
        ecs_cli = self.get_boto_client('ecs')
        svc = f'fuku-{ctx["app"]}-{task_name}'
        # waiter = ecs_cli.get_waiter('services_inactive')
        # waiter.wait(
        #     cluster=cluster,
        #     services=[svc]
        # )
        ecs_cli.delete_service(
            cluster=cluster,
            service=svc
        )

    def iter_services(self, task_name=None, app_name=None):
        ecs_cli = self.get_boto_client('ecs')
        paginator = ecs_cli.get_paginator('list_services')
        ctx = self.get_context()
        cluster = f'fuku-{ctx["cluster"]}'
        services = paginator.paginate(cluster=cluster)
        app_name = ctx.get('app', app_name)
        for svcs in services:
            for s in svcs['serviceArns']:
                ii = s.rfind('/')
                _, app, name = s[ii + 1:].split('-')
                if app == app_name:
                    yield name

    def is_running(self, task_name):
        ecs_cli = self.get_boto_client('ecs')
        ctx = self.get_context()
        results = ecs_cli.describe_services(
            cluster=f'fuku-{ctx["cluster"]}',
            services=[
                f'fuku-{ctx["app"]}-{task_name}'
            ]
        )
        try:
            return len(results['services']) > 0
        except KeyError:
            return False
