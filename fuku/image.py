from .module import Module


class Image(Module):
    dependencies = ['app']

    def __init__(self, **kwargs):
        super().__init__('image', **kwargs)

    def add_arguments(self, parser):
        subp = parser.add_subparsers(help='image help')

        p = subp.add_parser('add', help='add an image')
        p.add_argument('name', help='image name')
        p.set_defaults(image_handler=self.handle_add)

        p = subp.add_parser('list', help='list images')
        p.set_defaults(image_handler=self.handle_list)

        p = subp.add_parser('connect', help='connect to local image')
        p.add_argument('name', help='image name')
        p.add_argument('local', nargs='?', help='local image name')
        p.add_argument('--show', '-s', action='store_true',
                       help='show current')
        p.set_defaults(image_handler=self.handle_connect)

        # uregp = subp.add_parser('unregister', help='unregister a container')
        # uregp.add_argument('name', help='container name')
        # uregp.set_defaults(container_handler=self.unregister)

        p = subp.add_parser('push', help='push an image')
        p.add_argument('name', help='image name')
        p.set_defaults(image_handler=self.handle_push)

    def handle_add(self, args):
        if args.name in self.get_repositories(True):
            self.error('image by that name already exists')
        if args.name[0] == '/':
            repo = args.name[1:]
        else:
            app = self.client.get_selected('app')
            repo = '%s-%s' % (app, args.name)
        self.create_repository(repo)

    def handle_list(self, args):
        repos = self.get_repositories(True)
        for repo in repos:
            print(repo)

    def handle_connect(self, args):
        repos = self.get_repositories(True)
        if args.name not in repos:
            self.error('image does not exist')
        app = self.client.get_selected('app')
        x = self.store.setdefault('images', {}).setdefault(app, {}) # HERE
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
        if name[0] == '/':
            local = self.store.get('images', {}).get(name, {}).get('local', None)
        else:
            app = self.client.get_selected('app')
            local = self.store.get('images', {}).get(app, {}).get(name, {}).get('local', None)
        if not local:
            self.error('image not connected')
        uri = self.get_uri(args.name)
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
        results = []
        if _global:
            results += ['/' + d for d in data if '-' not in d]
        results += [d[len(pre):] for d in data if d.startswith(pre)]
        return results

    def get_uri(self, name):
        app = self.client.get_selected('app')
        repo = name
        if repo[0] != '/':
            repo = app + '-' + repo
        else:
            repo = repo[1:]
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

    def get_image_name(self, name):
        if name[0] != '!':
            if name[0] == '/':
                img = self.get_uri(name[1:], _global=True)
            else:
                img = self.get_uri(name)
        else:
            img = name[1:]
        return img
