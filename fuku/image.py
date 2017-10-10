from .module import Module


class Image(Module):
    dependencies = ['app']

    def __init__(self, **kwargs):
        super().__init__('image', **kwargs)

    def add_arguments(self, parser):
        subp = parser.add_subparsers(help='image help')

        p = subp.add_parser('ls', help='list repositories/images')
        p.set_defaults(image_handler=self.handle_list)

        p = subp.add_parser('mk', help='make a repository')
        p.add_argument('name', metavar='NAME', help='repository name')
        p.set_defaults(image_handler=self.handle_make)

        p = subp.add_parser('connect', help='connect to local image')
        p.add_argument('repo', metavar='REPO', help='repository name')
        p.add_argument('local', metavar='LOCAL', nargs='?', help='local image name')
        p.set_defaults(image_handler=self.handle_connect)

        p = subp.add_parser('push', help='push a connected image')
        p.add_argument('repo', metavar='REPO', help='repository name')
        p.set_defaults(image_handler=self.handle_push)

    def handle_list(self, args):
        self.list()

    def list(self):
        for repo in self.iter_repositories():
            print(repo)

    def handle_make(self, args):
        self.make(args.name)

    def make(self, name):
        ctx = self.get_context()
        ecr = self.get_boto_client('ecr')
        if name in list(self.iter_repositories(ecr=ecr, ctx=ctx)):
            self.error('image by that name already exists')
        if name[0] == '/':
            repo = name[1:]
            self.validate(repo)
        else:
            self.validate(name)
            repo = f'{ctx["app"]}-{name}'
        ecr.create_repository(
            repositoryName=repo
        )

    def handle_connect(self, args):
        self.connect(args.repo, args.local)

    def connect(self, repo, local):
        if repo not in list(self.iter_repositories()):
            self.error(f'repository "{repo}" does not exist')
        if repo[0] == '/':
            x = self.store.setdefault('images', {})
        else:
            ctx = self.get_context()
            x = self.store.setdefault('images', {}).setdefault(ctx['app'], {})
        x = x.setdefault(repo, {})
        if not local:
            local = x.get('local', None)
            if local:
                print(local)
        else:
            x['local'] = local

    def handle_push(self, args):
        self.push(args.repo)

    def push(self, repo):
        ctx = self.get_context()
        ii = repo.find(':')
        if ii >= 0:
            tag = ':' + repo[ii + 1:]
            repo = repo[:ii]
        else:
            tag = ''
        if repo[0] == '/':
            local = self.store.get('images', {}).get(repo, {}).get('local', None)
        else:
            local = self.store.get('images', {}).get(ctx['app'], {}).get(repo, {}).get('local', None)
        if not local:
            self.error('image not connected')
        ecr = self.get_boto_client('ecr')
        uri = self.get_uri(repo, ctx=ctx, ecr=ecr)
        self.run(f'docker tag {local} {uri}{tag}')
        self.login(ctx=ctx)
        self.run(f'docker push {uri}{tag}', capture=False)

    def iter_repositories(self, ecr=None, ctx=None):
        if ctx is None:
            ctx = self.get_context()
        if ecr is None:
            ecr = self.get_boto_client('ecr')
        data = ecr.describe_repositories()
        data = [d['repositoryName'] for d in data['repositories'] if d['repositoryName'] != 'fuku']
        pre = ctx['app'] + '-'
        results = ['/' + d for d in data if '-' not in d]
        results += [d[len(pre):] for d in data if d.startswith(pre)]
        for res in results:
            yield res

    def get_my_context(self):
        return {}

    def get_uri(self, repo, ecr=None, ctx=None):
        if ctx is None:
            ctx = self.get_context()
        if ecr is None:
            ecr = self.get_boto_client('ecr')
        if repo[0] != '/':
            repo = f'{ctx["app"]}-{repo}'
        else:
            repo = repo[1:]
        ii = repo.rfind(':')
        if ii > -1:
            tag = ':' + repo[ii + 1:]
            repo = repo[:ii]
        else:
            tag = ''
        try:
            return ecr.describe_repositories(
                repositoryNames=[repo]
            )['repositories'][0]['repositoryUri'] + tag
        except KeyError:
            self.error('unknown repository')

    def login(self, ctx=None):
        # TODO: Should really be using the `get_authorization_token` thingo.
        if ctx is None:
            ctx = self.get_context()
        data = self.run(f'aws --profile={ctx["profile"]} --region={ctx["region"]} ecr get-login --no-include-email')
        self.run(data)

    # def create_repository(self, repo):
    #     data = self.run(
    #         '$aws ecr create-repository'
    #         ' --repository-name {}'.format(
    #             repo
    #         ),
    #         capture='json'
    #     )
    #     return data['repository']['repositoryUri']

    # def delete_repository(self, repo):
    #     self.run(
    #         '$aws ecr delete-repository'
    #         ' --repository-name {}'.format(
    #             repo
    #         )
    #     )

    # def get_image(self, name):
    #     app = self.client.get_selected('app')
    #     repo = '%s-%s' % (app, name)
    #     data = self.run(
    #         '$aws ecr describe-images'
    #         ' --repository-name {}'
    #         .format(
    #             repo
    #         ),
    #         capture=True
    #     )
    #     return data

    # def unregister(self, args):
    #     ctr = self.get_image(args.name)
    #     self.delete_repository(ctr['full_repo'])
    #     # del self.store['containers'][ctr['app']][args.name]

    # def launch(self, args):
    #     name = args.name
    #     ctr = self.get_container(name)
    #     mach_mod = self.client.get_module('machine')
    #     mach = mach_mod.get_selected()
    #     mach_mod.ssh_run(
    #         mach['name'],
    #         '/root/pull.sh $region {}'.format(ctr['repo_uri'])
    #     )

    def image_name_to_uri(self, name):
        if name[0] != '!':
            img = self.get_uri(name)
        else:
            img = name[1:]
        return img
