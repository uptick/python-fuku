#!/bin/bash
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

# NOTE: Because we use Python's Template substitution on this file, all dollar signs
# must be escaped with another dollar sign: i.e. $$.

# Set the bucket to be used for agent in our bashrc.
echo FUKU_BUCKET=$bucket >> /etc/environment
echo FUKU_APP=$app >> /etc/environment
echo FUKU_MACHINE=$machine >> /etc/environment
echo FUKU_REGION=$region >> /etc/environment

# Prepare some packages.
pacman -Syy
pacman --noconfirm -S docker unzip python-pip git

# Install awscli and credentials for various AWS purposes.
pip install awscli filelock docker fuku

# Configure docker and launch.
groupadd docker
sed -i 's/dockerd/dockerd --log-driver=journald/g' /usr/lib/systemd/system/docker.service
systemctl enable docker
systemctl start docker

# Install AWS logging from journald.
curl -sOL https://github.com/saymedia/journald-cloudwatch-logs/releases/download/v0.0.1/journald-cloudwatch-logs-linux.zip && unzip journald-cloudwatch-logs-linux.zip
cp journald-cloudwatch-logs/journald-cloudwatch-logs /usr/bin/
mkdir -p /var/lib/journald-cloudwatch-logs
rm -rf journald-cloudwatch-logs
cat > /etc/journald-cloudwatch-logs.conf <<EOF
log_group = "$app
log_priority = "info"
state_file = "/var/lib/journald-cloudwatch-logs/state"
EOF
cat > /etc/systemd/system/journald-cloudwatch-logs.service <<EOF
[Unit]
Description=journald-cloudwatch-logs
Wants=basic.target
After=basic.target network.target

[Service]
ExecStart=/usr/bin/journald-cloudwatch-logs /etc/journald-cloudwatch-logs.conf
KillMode=process
Restart=on-failure
RestartSec=42s

[Install]
WantedBy=multi-user.target
EOF
systemctl enable journald-cloudwatch-logs.service
systemctl start journald-cloudwatch-logs.service

# # Setup some default keys to use before we have a DNS, and
# # haven't run certbot yet.
# mkdir -p /etc/letsencrypt/default
# openssl req -x509 -newkey rsa:2048 -keyout /etc/letsencrypt/default/privkey.key -out /etc/letsencrypt/default/cert.pem -nodes -subj '/CN=localhost'
# cat /etc/letsencrypt/default/privkey.key /etc/letsencrypt/default/cert.pem > /etc/letsencrypt/default/fullchain.pem

cat > /etc/systemd/system/fuku.service <<EOF
[Unit]
Description=fuku
Requires=docker.service
After=docker.service

[Service]
Type=simple
EnvironmentFile=/etc/environment
ExecStart=/usr/bin/fuku-agent run
ExecReload=/usr/bin/fuku-agent run
Restart=on-failure
RestartSec=3

[Install]
WantedBy=multi-user.target
EOF
systemctl enable fuku.service
