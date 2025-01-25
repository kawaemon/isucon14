#!/bin/bash
set -exu

graph=.kprivate/flamegraph
addr2line=.kprivate/addr2line

if [ ! -d $graph ]; then
    git clone https://github.com/brendangregg/FlameGraph.git $graph --filter=blob:none
fi
if [ ! -d $addr2line ]; then
    git clone https://github.com/gimli-rs/addr2line.git $addr2line --filter=blob:none
fi

ulimit -n 65536
sudo sysctl -w kernel.perf_event_paranoid=1

unset DOCKER_HOST
# sudo docker compose up nginx

sudo perf script --addr2line $addr2line/target/release/addr2line -i ./nginx/perf/perf.data > perf.data.scripted
cat ./perf.data.scripted | ./filter-perf-script.pl | $graph/stackcollapse-perf.pl --all > perf.data.collapsed

cat ./perf.data.collapsed | $graph/flamegraph.pl --width 1920 --color=java > out.svg
cat ./perf.data.collapsed | $graph/flamegraph.pl --width 1920 --color=java --reverse > out.reverse.svg
cat ./perf.data.collapsed | $graph/flamegraph.pl --width 1920 --color=java --flamechart > out.flamechart.svg
chromium ./out*.svg
