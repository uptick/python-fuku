from .module import Module


class Metrics(Module):
    dependencies = ['machine']

    def __init__(self, **kwargs):
        super().__init__('metrics', **kwargs)

    def add_arguments(self, parser):
        subp = parser.add_subparsers(help='metrics help')

        p = subp.add_parser('list', help='list metrics')
        p.add_argument('--all', '-a', action='store_true', help='include inactive metrics')
        p.set_defaults(metrics_handler=self.handle_list)

        p = subp.add_parser('add')
        p.add_argument('name', help='metric to add')
        p.set_defaults(metrics_handler=self.handle_add)

        p = subp.add_parser('remove')
        p.add_argument('name', help='metric to remove')
        p.set_defaults(metrics_handler=self.handle_remove)

    def handle_list(self, args):
        mach_mod = self.client.get_module('machine')
        if args.all:
            cmd = 'cat /usr/share/collectd/collectd-cloudwatch/src/cloudwatch/config/blocked_metrics'
        else:
            cmd = 'cat /usr/share/collectd/collectd-cloudwatch/src/cloudwatch/config/whitelist.conf'
        data = mach_mod.ssh_run(cmd, capture='text')
        if data:
            for l in data.splitlines()[2:]:
                print(l)

    def handle_add(self, args):
        mach_mod = self.client.get_module('machine')
        cmd = 'echo %s >> /usr/share/collectd/collectd-cloudwatch/src/cloudwatch/config/whitelist.conf' % args.name
        cmd += '; systemctl restart collectd'
        cmd = '\'' + cmd + '\''
        mach_mod.ssh_run(cmd, capture=False)

    def handle_remove(self, args):
        mach_mod = self.client.get_module('machine')
        cmd = 'echo args.name >> /usr/share/collectd/collectd-cloudwatch/src/cloudwatch/config/whitelist.conf'
        mach_mod.ssh_run(cmd, discard=False)
