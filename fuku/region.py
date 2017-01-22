import os

from .module import Module


class Region(Module):
    dependencies = ['profile']
    regions = set([
        'ap-southeast-2'
    ])

    def __init__(self, **kwargs):
        super().__init__('region', **kwargs)
        self.aws_path = os.path.expanduser('~/.aws/credentials')

    def add_arguments(self, parser):
        subp = parser.add_subparsers(help='region help')

        selp = subp.add_parser('select', help='select a region')
        selp.add_argument('name', help='region name')
        selp.set_defaults(region_handler=self.select)

        listp = subp.add_parser('list', help='list regions')
        listp.set_defaults(region_handler=self.list)

    def config(self):
        cfg = {}
        sel = self.get_selected()
        if sel:
            cfg['region'] = sel
            cfg['aws'] = '$aws --region $region'
        return cfg

    def list(self, args):
        for r in sorted(list(self.regions)):
            print(r)

    def select(self, args):
        name = args.name
        if not name:
            try:
                del self.store['selected']
            except KeyError:
                pass
        else:
            if name not in self.regions:
                self.error('unknown region')
            self.store['selected'] = name
        self.clear_parent_selections()

    def get_selected(self, fail=True):
        sel = self.store.get('selected', None)
        if not sel and fail:
            self.error('no region currently selected')
        return sel
