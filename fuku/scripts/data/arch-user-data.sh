#!/bin/bash
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

# NOTE: Because we use Python's Template substitution on this file, all dollar signs
# must be escaped with another dollar sign: i.e. $$.

# Prepare some packages.
pacman -Syy
pacman --noconfirm -S docker python-pip iotop # unzip
pip install awscli

# Configure docker and launch.
groupadd docker
sed -i 's!dockerd!dockerd --log-driver=awslogs --log-opt awslogs-region=$region --log-opt awslogs-group=/$app --log-opt awslogs-stream=$machine!g' /usr/lib/systemd/system/docker.service
systemctl enable docker
systemctl start docker

# Make logs a bit more efficient.
cat > /etc/systemd/journald.conf <<EOF
[Journal]
Storage=volatile
RuntimeMaxUse=10M
EOF

# Install a plugin for collectd to allow us to montior docker containers.
pacman --noconfirm -S collectd git
git clone https://github.com/signalfx/docker-collectd-plugin /usr/share/collectd/docker-collectd-plugin
pip install backports.ssl_match_hostname
pip install -r /usr/share/collectd/docker-collectd-plugin/requirements.txt

# Install a plugin to allow pushing to AWS metrics.
git clone https://github.com/awslabs/collectd-cloudwatch /usr/share/collectd/collectd-cloudwatch
sed -i 's!PluginInstance!Container!g' /usr/share/collectd/collectd-cloudwatch/src/cloudwatch/modules/metricdata.py
cat > /usr/share/collectd/collectd-cloudwatch/src/cloudwatch/config <<EOF
region="$region"
host="$machine"
whitelist_pass_through=False
debug=False
EOF
cat > /usr/share/collectd/collectd-cloudwatch/src/cloudwatch/config <<EOF
EOF
# .*.1-cpu.percent-
# .*.1-memory.percent-
