#!/bin/bash

set -ex

sudo chmod a+rwx -R /sys/fs/cgroup/cpu,cpuacct
sudo chmod a+rwx -R /sys/fs/cgroup/devices

aws s3 cp s3://mmlake-jobs/streaming/pool.xml /home/hadoop/files/pool.xml;

aws s3 cp s3://mmlake-jobs/streaming/metrics.properties /home/hadoop/files/metrics.properties;
