from .module import Module


class Region(Module):
    dependencies = ['profile']
    regions = set([
        'ap-southeast-2'
    ])

    def __init__(self, **kwargs):
        super().__init__('region', **kwargs)

    def add_arguments(self, parser):
        subp = parser.add_subparsers(help='region help')

        p = subp.add_parser('ls', help='list regions')
        p.set_defaults(region_handler=self.handle_list)

        p = subp.add_parser('sl', help='select a region')
        p.add_argument('name', metavar='NAME', help='region name')
        p.set_defaults(region_handler=self.handle_select)

    def handle_list(self, args):
        self.list()

    def list(self):
        for r in sorted(list(self.regions)):
            print(r)

    def handle_select(self, args):
        self.select(args.name)

    def select(self, name):
        if name and name not in self.regions:
            self.error(f'no region "{name}"')

        self.store_set('selected', name)
        self.clear_parent_selections()

    def get_availability_zone(self, zone):
        return f'{self.get_selected()}{zone}'

    def get_my_context(self):
        ctx = {}
        sel = self.get_selected()
        if sel:
            ctx['region'] = sel
        return ctx

    def get_selected(self):
        sel = self.store_get('selected')
        if not sel:
            self.error('no region currently selected')
        return sel
