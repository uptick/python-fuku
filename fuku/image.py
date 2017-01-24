from .module import Module


class Image(Module):
    dependencies = ['app']

    def __init__(self, **kwargs):
        super().__init__('image', **kwargs)

    def add_arguments(self, parser):
        subp = parser.add_subparsers(help='image help')

        p = subp.add_parser('list', help='list images')
        p.add_argument('--global', '-g', action='store_true', help='list global images')
        p.set_defaults(image_handler=self.handle_list)

        p = subp.add_parser('add', help='add an image')
        p.add_argument('name', help='image name')
        p.add_argument('--global', '-g', action='store_true', help='store globally')
        p.set_defaults(image_handler=self.handle_add)

        p = subp.add_parser('connect', help='connect a local image')
        p.add_argument('name', help='image name')
        p.add_argument('local', nargs='?', help='local image name')
        p.add_argument('--show', '-s', action='store_true', help='show current')
        p.add_argument('--global', '-g', action='store_true', help='global image')
        p.set_defaults(image_handler=self.handle_connect)

        # uregp = subp.add_parser('unregister', help='unregister a container')
        # uregp.add_argument('name', help='container name')
        # uregp.set_defaults(container_handler=self.unregister)

        p = subp.add_parser('push', help='push an image')
        p.add_argument('name', help='image name')
        p.add_argument('--global', '-g', action='store_true', help='global image')
        p.set_defaults(image_handler=self.handle_push)

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
        repos = self.get_repositories(getattr(args, 'global'))
        for repo in repos:
            print(repo)

    def handle_add(self, args):
        app = self.client.get_selected('app')
        if getattr(args, 'global'):
            repo = args.name
        else:
            repo = '%s-%s' % (app, args.name)
        self.create_repository(repo)

    def handle_connect(self, args):
        repos = self.get_repositories(getattr(args, 'global'))
        if args.name not in repos:
            self.error('image does not exist')
        x = self.store.setdefault('images', {})
        name = args.name
        if getattr(args, 'global'):
            name = '/' + name
        x = x.setdefault(name, {})
        if args.show:
            local = x.get('local', None)
            if local:
                print(local)
        else:
            x['local'] = args.local

    def handle_push(self, args):
        name = args.name
        if getattr(args, 'global'):
            name = '/' + name
        local = self.store.get('images', {}).get(name, {}).get('local', None)
        if not local:
            self.error('image not connected')
        uri = self.get_uri(args.name, getattr(args, 'global'))
        self.run(
            'docker tag {} {}:latest'.format(
                local,
                uri
            )
        )
        self.login()
        self.run('docker push {}:latest'.format(uri), capture=False)

    def login(self):
        data = self.run('$aws ecr get-login')
        self.run(data)

    def create_repository(self, repo):
        data = self.run(
            '$aws ecr create-repository'
            ' --repository-name {}'.format(
                repo
            ),
            capture='json'
        )
        return data['repository']['repositoryUri']

    def delete_repository(self, repo):
        self.run(
            '$aws ecr delete-repository'
            ' --repository-name {}'.format(
                repo
            )
        )

    def get_repositories(self, _global=False):
        app = self.client.get_selected('app')
        data = self.run(
            '$aws ecr describe-repositories'
            ' --query \'repositories[*].repositoryName\'',
            capture='json'
        )
        pre = app + '-'
        if _global:
            return [d for d in data if '-' not in d]
        else:
            return [d[len(pre):] for d in data if d.startswith(pre)]

    def get_uri(self, name, _global=False):
        app = self.client.get_selected('app')
        repo = name
        if not _global:
            repo = app + '-' + repo
        data = self.run(
            '$aws ecr describe-repositories'
            ' --repository-name {}'
            ' --query \'repositories[*].repositoryUri\''
            .format(repo),
            capture='json'
        )
        return data[0]

    def get_image(self, name):
        app = self.client.get_selected('app')
        repo = '%s-%s' % (app, name)
        data = self.run(
            '$aws ecr describe-images'
            ' --repository-name {}'
            .format(
                repo
            ),
            capture=True
        )
        return data

    def unregister(self, args):
        ctr = self.get_image(args.name)
        self.delete_repository(ctr['full_repo'])
        # del self.store['containers'][ctr['app']][args.name]

    def launch(self, args):
        name = args.name
        ctr = self.get_container(name)
        mach_mod = self.client.get_module('machine')
        mach = mach_mod.get_selected()
        mach_mod.ssh_run(
            mach['name'],
            '/root/pull.sh $region {}'.format(ctr['repo_uri'])
        )
