#!/usr/bin/env zsh

sudo sysctl -w net.ipv4.tcp_congestion_control=reno
sudo sysctl -w net.ipv4.ip_local_port_range="10000 65535"

ulimit -n 512000
../sql/init.sh && ENV=local-dev SCORE_TARGET=0 MATCHING_LIMIT_PER_SEC=99999999 CHAIR_MATCHING_DELAY_MS=50 cargo r --release
