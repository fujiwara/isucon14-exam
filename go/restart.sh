#!/bin/bash

set -eux
make

for host in 192.168.0.12 192.168.0.194; do
    rsync -av ~/env.sh $host:/home/isucon/env.sh
    sudo rsync -av --delete --exclude .others ../ $host:/home/isucon/webapp/
    sudo rsync -av /etc/nginx/ $host:/etc/nginx/
done

for host in 192.168.0.11 192.168.0.12 192.168.0.194; do
    ssh $host 'sudo rm -f /var/log/nginx/access.log; sudo systemctl restart nginx; sudo systemctl restart isuride-go.service'
done

# localhost
sudo truncate --size 0 /var/log/mysql/mysql-slow.log
mysqladmin -uisucon -pisucon flush-logs
