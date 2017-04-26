from .module import Module


class Metrics(Module):
    dependencies = ['service']
    metric_choices = [
        'memory',
        'cpu'
    ]

    def __init__(self, **kwargs):
        super().__init__('metrics', **kwargs)

    def add_arguments(self, parser):
        subp = parser.add_subparsers(help='metrics help')

        p = subp.add_parser('ls', help='list metrics')
        p.add_argument('--all', '-a', action='store_true', help='include inactive metrics')
        p.set_defaults(metrics_handler=self.handle_list)

        p = subp.add_parser('mk', help='add metrics')
        p.add_argument('task', metavar='TASK', help='task name')
        p.add_argument('metric', metavar='METRIC', choices=self.metric_choices, help='metric')
        p.set_defaults(metrics_handler=self.handle_make)

        p = subp.add_parser('rm')
        p.add_argument('name', metavar='NAME', help='metric to remove')
        p.set_defaults(metrics_handler=self.handle_remove)

        p = subp.add_parser('clear')
        p.set_defaults(metrics_handler=self.handle_clear)

    def handle_list(self, args):
        self.list(args.all)

    def list(self, all):
        ctx = self.get_context()
        metrics = self.gets3(f'{ctx["cluster"]}/metrics.json') or {}
        for task, mets in metrics.items():
            print(f'{task} ({",".join( sorted( mets ) )})')

    def handle_make(self, args):
        self.make(args.task, args.metric)

    def make(self, task_name, met_name):
        ctx = self.get_context()
        metrics = self.gets3(f'{ctx["cluster"]}/metrics.json') or {}
        met_list = metrics.setdefault(task_name, [])
        if met_name in met_list:
            return
        met_list.append(met_name)
        self.puts3(f'{ctx["cluster"]}/metrics.json', metrics)
        met = f'docker-{ctx["app"]}-{task_name}.*-{met_name}.percent-'
        node_mod = self.client.get_module('node')
        cmd = f'echo {met} >> /usr/share/collectd/collectd-cloudwatch/src/cloudwatch/config/whitelist.conf'
        cmd += '; systemctl restart collectd'
        cmd = '\'' + cmd + '\''
        node_mod.all_run(cmd)

    def handle_remove(self, args):
        mach_mod = self.client.get_module('machine')
        cmd = 'echo args.name >> /usr/share/collectd/collectd-cloudwatch/src/cloudwatch/config/whitelist.conf'
        mach_mod.ssh_run(cmd, discard=False)

    def handle_clear(self, args):
        self.clear()

    def clear(self):
        ctx = self.get_context()
        self.puts3(f'{ctx["cluster"]}/metrics.json', {})
        cmd = f'truncate -s 0 /usr/share/collectd/collectd-cloudwatch/src/cloudwatch/config/whitelist.conf'
        cmd += '; systemctl restart collectd'
        cmd = '\'' + cmd + '\''
        node_mod = self.client.get_module('node')
        node_mod.all_run(cmd)
