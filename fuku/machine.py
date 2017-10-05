import json
import re

from .module import Module


class Machine(Module):
    dependencies = ['app']
    ami_map = {
        # 'ap-southeast-2': 'ami-862211e5',  # docker enabled amazon
        'ap-southeast-2': 'ami-5c8fb13f',  # arch linux
    }

    def __init__(self, **kwargs):
        super().__init__('machine', **kwargs)

    def add_arguments(self, parser):
        subp = parser.add_subparsers(help='machine help')

        p = subp.add_parser('list', help='list machines')
        p.add_argument('name', nargs='?')
        p.set_defaults(machine_handler=self.list)

        addp = subp.add_parser('add', help='add a machine')
        addp.add_argument('name', help='machine name')
        addp.add_argument('--public', '-p', action='store_true',
                          help='assign public IP')
        addp.set_defaults(machine_handler=self.add)

        p = subp.add_parser('ip', help='allocate a public IP')
        p.set_defaults(machine_handler=self.handle_ip)

        addp = subp.add_parser('init')
        addp.add_argument('name', help='machine name')
        addp.set_defaults(machine_handler=self.handle_init_swarm)

        sshp = subp.add_parser('ssh', help='ssh to a machine')
        sshp.add_argument('name', nargs='?', help='machine name')
        sshp.set_defaults(machine_handler=self.ssh)

        p = subp.add_parser('scp', help='scp')
        p.add_argument('src')
        p.add_argument('dst')
        p.set_defaults(machine_handler=self.scp)

        remp = subp.add_parser('remove', help='remove a machine')
        remp.add_argument('name', help='machine name')
        remp.set_defaults(machine_handler=self.remove)

        selp = subp.add_parser('select', help='select a machine')
        selp.add_argument('name', nargs='?', help='machine name')
        selp.add_argument('--show', '-s', action='store_true', help='show currently selected')
        selp.set_defaults(machine_handler=self.select)

        p = subp.add_parser('stats', help='show streaming stats')
        p.add_argument('name', nargs='?', help='machine name')
        p.set_defaults(machine_handler=self.handle_stats)

        p = subp.add_parser('reboot', help='reboot a machine')
        p.add_argument('name', nargs='?', help='machine name')
        p.set_defaults(machine_handler=self.handle_reboot)

    def handle_reboot(self, args):
        app = self.client.get_selected('app')
        name = args.name
        if name and not self.exists(name):
            self.error('unknown machine')
        if not name:
            name = self.get_selected()
        inst = self.get_instance(name, app)
        self.run(
            '$aws ec2 reboot-instances'
            ' --instance-ids {}'.format(
                inst['InstanceId']
            )
        )

    def tag_instance(self, inst_id, tags):
        tags = ['Key=%s,Value=%s' % (k, v) for k, v in tags.items()]
        self.run(
            '$aws ec2 create-tags'
            ' --resources {}'
            ' --tags {}'.format(
                inst_id,
                ' '.join(tags)
            )
        )

    def allocate_address(self, inst_id):
        data = self.run(
            '$aws ec2 allocate-address'
            ' --domain vpc',
            capture='json'
        )
        alloc_id = data['AllocationId']
        public_ip = data['PublicIp']
        self.run(
            '$aws ec2 associate-address'
            ' --instance-id {}'
            ' --allocation-id {}'.format(
                inst_id,
                alloc_id
            )
        )
        return alloc_id, public_ip

    def release_address(self, alloc_id):
        self.run(
            '$aws ec2 release-address'
            ' --allocation-id {}'.format(
                alloc_id
            )
        )

    def run_instance(self, name, region):
        image = self.ami_map[region]
        ctx = self.merged_config({'machine': name}, use_self=False)
        with self.template_file('arch-user-data.sh', ctx) as user_data:
            inst_id = self.run(
                '$aws ec2 run-instances'
                ' --image-id {image}'
                ' --key-name fuku-$app'
                ' --security-group-ids "$security_group"'
                ' --user-data file://{user_data}'
                ' --instance-type t2.micro'
                ' --iam-instance-profile Name=ec2-profile'
                ' --associate-public-ip-address'
                ' --count 1'
                ' --query \'Instances[0].InstanceId\''
                .format(
                    user_data=user_data,
                    image=image
                ),
                capture='json'
            )
        return inst_id

    def delete_instance(self, inst_id):
        self.run(
            '$aws ec2 terminate-instances'
            ' --instance-ids "{}"'.format(
                inst_id
            )
        )

    def list_instances(self, app):
        # TODO: Don't select machines that are terminating.
        data = self.run(
            '$aws ec2 describe-instances'
            ' --filters Name=tag:app,Values=$app Name=instance-state-name,Values=pending,running,shutting-down,stopping,stopped'
            ' --query \'Reservations[*].Instances[*].{id:InstanceId,tags:Tags[*]}\'',
            capture='json'
        )
        insts = []
        if data:
            for r in data:
                for i in r:
                    insts.append({
                        'id': i['id'],
                        'tags': dict([(t['Key'], t['Value']) for t in i['tags']])
                    })
        return insts

    def get_instance(self, name, app):
        data = self.run(
            '$aws ec2 describe-instances'
            ' --filters Name=tag:app,Values=$app Name=tag:name,Values=$machine',
            {'machine': name},
            capture='json'
        )
        if not data:
            self.error('unable to retrieve instance')
        return data['Reservations'][0]['Instances'][0]

    def get_address(self, inst_id):
        data = self.run(
            '$aws ec2 describe-addresses'
            ' --filters Name=instance-id,Values=$inst_id',
            {'inst_id': inst_id},
            capture='json'
        )
        if not data:
            self.error('unable to retrieve address')
        return data['Addresses'][0]

    def wait(self, inst_id):
        self.run(
            '$aws ec2 wait instance-status-ok'
            ' --instance-ids "{}"'.format(
                inst_id
            )
        )

    def list(self, args):
        app = self.client.get_selected('app')
        if args.name:
            inst = self.get_instance(args.name, app)
            print(json.dumps(inst, indent=2))
        else:
            machs = self.list_instances(app)
            for m in machs:
                print('{}:{}'.format(m['tags']['name'], m['id']))

    def add(self, args):
        if not self.db.get('profile', {}).get('bucket', None):
            self.error('must set profile bucket')
        region = self.client.get_selected('region')
        app = self.client.get_selected('app')
        name = args.name
        if name and self.exists(name):
            self.error('name already used')
        inst_id = self.run_instance(name, region)
        self.tag_instance(inst_id, {
            'Name': '%s-%s' % (app, name),
            'name': name,
            'app': app,
            'manager': 'true'
        })
        self.wait(inst_id)
        self.run(
            '$aws s3 cp '
            ' {}'
            ' s3://$bucket/fuku/$app/machines/{}.json'.format(
                self.data_path('machine.json'),
                name
            )
        )
        if args.public:
            alloc_id, public_ip = self.allocate_address(inst_id)
        self.init_swarm(args.name)

    def handle_init_swarm(self, args):
        self.init_swarm(args.name)

    def init_swarm(self, name):
        app = self.client.get_selected('app')
        inst = self.get_instance(name, app)
        ip = inst['PrivateIpAddress']
        response = self.ssh_run(
            'docker swarm init --advertise-addr {}'.format(ip),
            name=name,
            capture='text'
        )
        try:
            token = re.search(r'token\s+(.*)\s+\\', response).group(1)
            port = re.search(r':(\d\d\d\d)', response).group(1)
        except AttributeError:
            self.error('init failed')
        self.tag_instance(inst['InstanceId'], {
            'swarmtoken': token,
            'swarmport': port
        })
        self.ssh_run(
            'docker network create --driver overlay all',
            name=name,
            capture='discard'
        )

    def remove(self, args):
        # TODO: Give an option to back out. Maybe even two.
        if not self.db.get('profile', {}).get('bucket', None):
            self.error('must set profile bucket')
        app = self.client.get_selected('app')
        name = args.name
        if not self.exists(name):
            self.error('no such machine')
        inst = self.get_instance(name, app)
        addr = self.get_address(inst['InstanceId'])
        if addr:
            self.release_address(addr['AllocationId'])
        self.delete_instance(inst['InstanceId'])
        self.run(
            '$aws s3 rm '
            ' s3://$bucket/fuku/$app/machines/$name.json',
            {'name': name}
        )
        sel = self.get_selected(fail=False)
        if sel == name:
            del self.store['selected']
        try:
            del self.store.get('machines', {})[name]
        except KeyError:
            pass

    def select(self, args):
        if args.show:
            sel = self.get_selected(fail=False)
            if sel:
                print(sel)
        else:
            name = args.name
            if name:
                if not self.exists(name):
                    self.error('unkown machine')
                self.store.setdefault('machines', {}).setdefault(name, {})
                self.store['selected'] = name
            else:
                try:
                    del self.store['selected']
                except KeyError:
                    pass
            self.clear_parent_selections()

    def handle_stats(self, args):
        self.ssh_run('docker stats', args.name)

    def handle_ip(self, args):
        app = self.client.get_selected('app')
        name = self.get_selected()
        inst = self.get_instance(name, app)
        id = inst['InstanceId']
        self.allocate_address(id)

    def ssh(self, args):
        self.ssh_run('', args.name)

    def ssh_run(self, cmd, name=None, tty=False, capture=None):
        app = self.client.get_selected('app')
        name = name or self.get_selected()
        inst = self.get_instance(name, app)
        ip = inst['PublicIpAddress']
        full_cmd = 'ssh%s -o "StrictHostKeyChecking no" -i "$pem" root@%s %s' % (' -t' if tty else '', ip, cmd)
        return self.run(full_cmd, capture=capture)

    def scp(self, args):
        app = self.client.get_selected('app')
        name = self.get_selected()
        inst = self.get_instance(name, app)
        ip = inst['PublicIpAddress']
        self.run(
            'scp -o "StrictHostKeyChecking no" -i "$pem" {} root@{}:{}'.format(
                args.src,
                ip,
                args.dst
            ),
            capture=False
        )

    def exists(self, name):
        app = self.client.get_selected('app')
        machs = self.list_instances(app)
        for m in machs:
            if m['tags']['name'] == name:
                return True
        return False

    def get_selected(self, fail=True):
        sel = self.store.get('selected', None)
        if not sel and fail:
            self.error('no machine selected')
        return sel
