#!/bin/bash
echo 'Coping script from S3...';
aws s3 cp s3://mmlake-jobs/streaming/pool.xml /home/hadoop/files/pool.xml;

aws s3 cp s3://mmlake-jobs/streaming/metrics.properties /home/hadoop/files/metrics.properties;
