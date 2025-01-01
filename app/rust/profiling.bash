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

pushd $addr2line
cargo b --features=bin --release --bin=addr2line
popd

../sql/init.sh
ulimit -n 65536

cargo build --release --bin isuride
sudo perf record -F 150 --call-graph dwarf ./target/release/isuride || true

sudo PATH=$addr2line/target/release:$PATH perf script \
  | $graph/stackcollapse-perf.pl --all > perf.data.collapsed

cat ./perf.data.collapsed | $graph/flamegraph.pl --width 1920 --color=java > out.svg
cat ./perf.data.collapsed | $graph/flamegraph.pl --width 1920 --color=java --reverse > out.reverse.svg
cat ./perf.data.collapsed | $graph/flamegraph.pl --width 1920 --color=java --flamechart > out.flamechart.svg
chromium ./out*.svg
