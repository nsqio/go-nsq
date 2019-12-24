#!/bin/bash

set -e

wget http://bitly-downloads.s3.amazonaws.com/nsq/$NSQ_DOWNLOAD.tar.gz
tar zxvf $NSQ_DOWNLOAD.tar.gz
export PATH=$NSQ_DOWNLOAD/bin:$PATH

export GO111MODULE=on
./test.sh
