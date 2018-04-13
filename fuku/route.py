from .module import Module


class Route(Module):
    dependencies = ['app']

    def __init__(self, **kwargs):
        super().__init__('route', **kwargs)

    def add_arguments(self, parser):
        subp = parser.add_subparsers(help='route help')

        p = subp.add_parser('mk', help='make a route 53 record set entry')
        p.add_argument('name', metavar='NAME', help='record set name')
        p.add_argument('zone', metavar='ZONE', help='hosted zone name')
        p.set_defaults(route_handler=self.handle_make)

        p = subp.add_parser('rm', help='remove a route 53 record set entry')
        p.add_argument('name', metavar='NAME', help='record set name')
        p.add_argument('zone', metavar='ZONE', help='hosted zone name')
        p.set_defaults(route_handler=self.handle_remove)

    def handle_make(self, args):
        self.make(args.name, args.zone)

    def make(self, name, zone_name):
        # get zone id
        zone = self.get_zone(zone_name)

        # find app target group and elb
        task_mod = self.client.get_module('app')
        target_group = task_mod.get_target_group()

        client = self.get_boto_client('elbv2')
        load_balancers = client.describe_load_balancers(
            LoadBalancerArns=target_group['LoadBalancerArns']
        )['LoadBalancers']
        elb_dns_name = load_balancers[0]['DNSName']

        # map dns name to elb
        client = self.get_boto_client('route53')
        client.change_resource_record_sets(
            HostedZoneId=zone['Id'],
            ChangeBatch= {
                'Changes': [{
                    'Action': 'UPSERT',
                    'ResourceRecordSet': {
                        'Name': f'{name}.{zone_name}.',
                        'Type': 'CNAME',
                        'TTL': 60,
                        'ResourceRecords': [{'Value': elb_dns_name}]
                    }
                }]
            })

    def handle_remove(self, args):
        self.remove(args.name, args.zone)

    def remove(self, name, zone_name):
        # get zone id
        zone = self.get_zone(zone_name)

        record_name = f'{name}.{zone_name}.'
        # make sure record set exists
        client = self.get_boto_client('route53')
        recordset = client.list_resource_record_sets(
            HostedZoneId=zone['Id'],
            StartRecordName=record_name,
            MaxItems='1'
        )['ResourceRecordSets'][0]
        assert recordset['Name'] == record_name, f'Recordset {record_name} not found.'

        # delete the recordset
        client.change_resource_record_sets(
            HostedZoneId=zone['Id'],
            ChangeBatch={
                'Changes': [{
                    'Action': 'DELETE',
                    'ResourceRecordSet': {
                        'Name': record_name,
                        'Type': 'CNAME',
                        'TTL': 60,
                        'ResourceRecords': recordset['ResourceRecords'],
                    }
                }]
            })

    def get_zone(self, zone_name):
        # find dns name
        paginate = self.get_boto_paginator('route53', 'list_hosted_zones').paginate()
        search = paginate.search(f'HostedZones[?Name == `"{zone_name}."`]')
        return list(search)[0]
