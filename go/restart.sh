#!/bin/bash

set -eux
make
sudo rm -f /var/log/nginx/access.log
sudo systemctl restart nginx
sudo systemctl restart isuride-go.service
sudo chmod 644 /var/log/nginx/access.log
sudo truncate --size 0 /var/log/mysql/mysql-slow.log
mysqladmin -uisucon -pisucon flush-logs
sudo journalctl -u isuride-go.service -f
