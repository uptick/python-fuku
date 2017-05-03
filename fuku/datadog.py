from .module import Module


class Datadog(Module):
    dependencies = ['task']

    def __init__(self, **kwargs):
        super().__init__('datadog', **kwargs)

    def add_arguments(self, parser):
        subp = parser.add_subparsers(help='datadog help')

        p = subp.add_parser('mk', help='make a datadog task')
        p.add_argument('key', metavar='KEY', help='API key')
        p.set_defaults(datadog_handler=self.handle_make)

    def handle_make(self, args):
        self.make(args.key)

    def make(self, key):
        task_mod = self.get_module('task')
        task_mod.make('dd_agent', '!datadog/docker-dd-agent:latest', logs=False)
        task_mod.env_set(
            'dd_agent',
            {
                'API_KEY': key,
                'SD_BACKEND': 'docker',
            }
        )
        task_mod.volume_add('dd_agent', 'socket', '/var/run/docker.sock', '/var/run/docker.sock', read_only=True)
        task_mod.volume_add('dd_agent', 'proc', '/host/proc/', '/proc/', read_only=True)
        task_mod.volume_add('dd_agent', 'cgroup', '/host/sys/fs/cgroup', '/cgroup/', read_only=True)
