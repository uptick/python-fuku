from .module import Module


class Container(Module):
    dependencies = ['machine']

    def __init__(self, **kwargs):
        super().__init__('container', **kwargs)

    def add_arguments(self, parser):
        subp = parser.add_subparsers(help='container help')

        p = subp.add_parser('list', help='list containers')
        p.add_argument('--all', '-a', action='store_true', help='list inactive containers')
        p.set_defaults(container_handler=self.handle_list)

        # addp = subp.add_parser('add', help='add a machine')
        # addp.set_defaults(machine_handler=self.add)

        # p = subp.add_parser('register', help='register a container definition')
        # p.add_argument('name')
        # p.add_argument('image')
        # p.set_defaults(container_handler=self.handle_register)

        # uregp = subp.add_parser('unregister', help='unregister a container')
        # uregp.add_argument('name', help='container name')
        # uregp.set_defaults(container_handler=self.unregister)

        # pushp = subp.add_parser('push', help='push a container')
        # pushp.add_argument('name', help='container name')
        # pushp.set_defaults(container_handler=self.push)

        p = subp.add_parser('run', help='run a container')
        p.add_argument('--name', '-n', help='container name')
        p.add_argument('--task', '-t', help='task name')
        p.set_defaults(container_handler=self.handle_run)

        p = subp.add_parser('remove', help='remove a container')
        p.add_argument('name', help='container name')
        p.set_defaults(container_handler=self.handle_remove)

        p = subp.add_parser('stop', help='remove a container')
        p.add_argument('name', help='container name')
        p.set_defaults(container_handler=self.handle_stop)

        p = subp.add_parser('attach', help='attach to a container')
        p.add_argument('name', help='container name')
        p.set_defaults(container_handler=self.handle_attach)

        # selp = subp.add_parser('select', help='select a machine')
        # selp.add_argument('instance_id', help='machine instance ID')
        # selp.set_defaults(machine_handler=self.select)

    def handle_list(self, args):
        mach_mod = self.client.get_module('machine')
        mach = mach_mod.get_selected()
        cmd = 'fuku-agent list'
        if not args.all:
            cmd += ' -r'
        data = mach_mod.ssh_run(
            cmd,
            name=mach,
            capture='json'
        )
        for ctr in data['result']:
            print(ctr)

    def handle_run(self, args):
        mach_mod = self.client.get_module('machine')
        mach = mach_mod.get_selected()
        cmd = 'fuku-agent run'
        if args.name:
            cmd += ' -n {}'.format(args.name)
        if args.task:
            cmd += ' -t {}'.format(args.task)
        data = mach_mod.ssh_run(
            cmd,
            name=mach,
            capture='json'
        )
        if data['status'] != 'ok':
            print(data['result'])

    def handle_remove(self, args):
        mach_mod = self.client.get_module('machine')
        mach = mach_mod.get_selected()
        data = mach_mod.ssh_run(
            'fuku-agent remove {} -d'.format(args.name),
            name=mach,
            capture='json'
        )
        if data['status'] != 'ok':
            print(data['result'])

    def handle_stop(self, args):
        mach_mod = self.client.get_module('machine')
        mach = mach_mod.get_selected()
        data = mach_mod.ssh_run(
            'fuku-agent remove {}'.format(args.name),
            name=mach,
            capture='json'
        )
        if data['status'] != 'ok':
            print(data['result'])

    def handle_attach(self, args):
        mach_mod = self.client.get_module('machine')
        mach = mach_mod.get_selected()
        cmd = 'docker exec -i -t {} bash'.format(args.name)
        mach_mod.ssh_run(
            cmd,
            name=mach,
            tty=True,
            capture=False
        )

    def get_image(self, name):
        pass
        # app = self.client.get_selected('app')
        # ctrs = self.store.get('containers', {}).get(app['name'], {})
        # try:
        #     return ctrs[name]
        # except KeyError:
        #     self.error('unknown container')

    def login(self):
        data = self.run('$aws ecr get-login')
        self.run(data)

    def create_repository(self, repo):
        self.login()
        data = self.run(
            '$aws ecr create-repository'
            ' --repository-name {}'.format(
                repo
            ),
            capture='json'
        )
        return data['repository']['repositoryUri']

    def delete_repository(self, repo):
        self.login()
        self.run(
            '$aws ecr delete-repository'
            ' --repository-name {}'.format(
                repo
            )
        )

    def get_image(self, name):
        app = self.client.get_selected('app')
        repo = '%s-%s' % (app, name)
        self.login()
        data = self.run(
            '$aws describe-images'
            ' --repository-name {}'
            .format(
                repo
            )
        )
        print(data)
        return data

    def register(self, args):
        app = self.client.get_selected('app')
        name = args.name
        repo = '%s-%s' % (app, repo)
        self.create_repository(repo)
        # ctrs = self.store.setdefault('containers', {}).setdefault(app['name'], {})
        # ctrs[name] = {
        #     'name': name,
        #     'repo': repo,
        #     'repo_uri': uri,
        #     'full_repo': full_repo,
        #     'app': app['name'],
        # }

    def unregister(self, args):
        ctr = self.get_image(args.name)
        self.delete_repository(ctr['full_repo'])
        # del self.store['containers'][ctr['app']][args.name]

    def list(self, args):
        app = self.client.get_selected('app')
        # ctrs = self.store.get('containers', {}).get(app['name'], {})
        # for ctr in ctrs.keys():
        #     print('{}'.format(ctr))

    def push(self, args):
        ctr = self.get_image(args.name)
        # uri = ctr['repo_uri']
        self.run(
            'docker tag {}:latest {}:latest'.format(
                ctr['name'],
                uri
            )
        )
        self.run('docker push {}:latest'.format(uri))

    def launch(self, args):
        name = args.name
        ctr = self.get_container(name)
        mach_mod = self.client.get_module('machine')
        mach = mach_mod.get_selected()
        mach_mod.ssh_run(
            mach['name'],
            '/root/pull.sh $region {}'.format(ctr['repo_uri'])
        )
