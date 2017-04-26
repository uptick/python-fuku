from .module import Module


class Configuration(Module):
    dependencies = ['region']

    def __init__(self, **kwargs):
        super().__init__('configuration', **kwargs)

    def add_arguments(self, parser):
        subp = parser.add_subparsers(help='configuration help')

        p = subp.add_parser('bucket', help='set FUKU bucket')
        p.add_argument('name', metavar='NAME', help='bucket name')
        p.set_defaults(configuration_handler=self.handle_bucket)

        p = subp.add_parser('ls', help='list configuration')
        p.set_defaults(configuration_handler=self.handle_list)

    def handle_bucket(self, args):
        self.bucket(args.name)

    def bucket(self, name):
        s3 = self.get_boto_resource('s3')
        bucket = s3.Bucket(name)
        try:
            bucket.load()
        except:
            bucket.create()
        self.store_set('bucket', name)

    def handle_list(self, args):
        self.list()

    def list(self):
        if 'bucket' in self.store:
            print(f'bucket: {self.store["bucket"]}')

    def get_my_context(self):
        ctx = {}
        if 'bucket' not in self.store:
            self.error('bucket not set')
        ctx['bucket'] = self.store['bucket']
        return ctx

    def save(self):
        cache = super().save()
        if 'bucket' in self.store:
            cache.update({
                'bucket': self.store['bucket']
            })
        return cache

    def load(self, cache):
        super().load(cache)
        if 'bucket' in cache:
            self.store['bucket'] = cache['bucket']
        else:
            try:
                del self.store['bucket']
            except KeyError:
                pass
