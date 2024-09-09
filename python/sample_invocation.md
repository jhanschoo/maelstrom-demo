Clone the repository (parent folder) into the maelstrom binary distribution (as a release of https://github.com/jepsen-io/maelstrom), and invoke the samples from this folder e.g. with

```
../../maelstrom test -w lin-kv --time-limit 60 --nemesis partition --nemesis-interval 10 --test-count 10 --node-count 5 --concurrency 4n --rate 30 --bin "/bin/python3" "$PWD/s06_raft/__main__.py"
```
