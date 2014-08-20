# eskka - discovery plugin for elasticsearch

eskka aims at providing a robust [Zen](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/modules-discovery-zen.html) replacement. It most closely resembles Zen unicast discovery in that no external coordinator is required but you need to configure seed nodes. 

It builds on top of Akka Cluster which uses a [gossip protocol](http://en.wikipedia.org/wiki/Gossip_protocol). It will help to read the [spec](http://doc.akka.io/docs/akka/snapshot/common/cluster.html#cluster).

We use a quorum of configured seed nodes for resolving partitions when a failure is detected (configurable thresholds) by 'downing' the affected node.

If any node (including the master) loses reachability with a quorum of seed nodes, it clears its internal elasticsearch cluster state and kicks off rejoin attempts.

There is no master election per-se, it is [deterministically the 'oldest'](http://doc.akka.io/docs/akka/2.3.4/contrib/cluster-singleton.html) master-eligible member of the cluster.

## installation

Head to [Releases](https://github.com/shikhar/eskka/releases) on github.

### pre-packaged

Use the plugin script with the arguments `--url https://eskka.s3.amazonaws.com/eskka-$version.zip --install eskka`.

### package it

Clone the repository. Use the [sbt](http://www.scala-sbt.org/#install) target `pack`, which will generate a plugin zip under `target/`. 

Then use the plugin script with the arguments `--url file:///path/to/eskka-$version.zip --install eskka`.

## configuration

eskka runs on a different port to both elasticsearch's http and internal transport.

`discovery.type` - you will need to set this to `eskka.EskkaDiscoveryModule`

`discovery.eskka.seed_nodes` - array of `[host:port,..]`. The port is optional and will default to 9400. The seed nodes will default to `[<discovery.eskka.host>:9400]` if unspecified. It is probably the most important piece of configuration as a quorum `(n/2 + 1)` of seed nodes is used in partition resolution - so ideally you would have 3 or more. A note from the [Akka Cluster docs](http://doc.akka.io/docs/akka/snapshot/java/cluster-usage.html#Joining_to_Seed_Nodes):

> The seed nodes can be started in any order and it is not necessary to have all seed nodes running, but the node configured as the first element in the seed-nodes configuration list must be started when initially starting a cluster, otherwise the other seed-nodes will not become initialized and no other node can join the cluster. The reason for the special first seed node is to avoid forming separated islands when starting from an empty cluster. It is quickest to start all configured seed nodes at the same time (order doesn't matter), otherwise it can take up to the configured seed-node-timeout until the nodes can join.

`discovery.eskka.host` - the [elasticsearch logical host setting values](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/modules-network.html#modules-network) are supported here. It will fallback to `transport.bind_host`, `transport.host`, `network.bind_host`, `network.host` and `_local_`, in that order.

`discovery.eskka.port` - port ranges are not supported, this must be an int. Defaults to 0 in case this is a client node, and 9400 otherwise.

`discovery.eskka.heartbeat_interval` - time value. "How often keep-alive heartbeat messages should be sent to each connection." Defaults to 1s.

`discovery.eskka.acceptable_heartbeat_pause` - time value. "Number of potentially lost/delayed heartbeats that will be accepted before considering it to be an anomaly. This margin is important to be able to survive sudden, occasional, pauses in heartbeat arrivals, due to for example garbage collect or network drop." Defaults to 3s.

`discovery.eskka.partition.eval-delay` - time value. The delay after which we will start evaluating a partitioned node by arranging for a distributed ping by the seed nodes. It defaults to 5s.

`discovery.eskka.partition.ping-timeout` - time value. If a quorum of seed nodes affirmatively times out in contacting the partitioned node, it will be downed. It defaults to 2s.

## caveats

### be explicit about hostnames

The `disovery.eskka.host` value is used to configure akka with the address to bind on and communicate using. akka remoting is picky about only accepting messages on the address it was bound with, and you can't use the wildcard address.

Ideally, you should configure `discovery.eskka.host` or one of the ES config values it will fallback to explicitly (i.e. `transport.bind_host`, `transport.host`, `network.bind_host` or `network.host`).

If you want _hostnames_ to be used, avoid logical setting values because for values like `_local_` or `_eth0_`, eskka will resolve them to the resulting IP address.

The addresses that you specify in `discovery.eskka.seed_nodes` must line up exactly with the `discovery.eskka.host` configuration on the respective nodes.

### tone down the logging

Akka remoting logging is at this time fairly verbose, so unless you find the humm of the cluster comforting you may want to tone it down by updating `logging.yml`:

```
logger:
   akka: WARN
```

## example configurations

In any of these examples, the specification of hostname may equivalently be for `transport.bind_host`, `transport.host`, `network.bind_host` or `network.host` (as far as eskka is concerned).

### three instances on three separate hosts using the default port

three hosts are `n1.xyz.com`, `n2.xyz.com` and `n3.xyz.com`. since we are using the default eskka port `9400`, it need not be specified.

*on n1*

```
discovery.type: eskka.EskkaDiscoveryModule

discovery.eskka.host: n1.xyz.com

discovery.eskka.seed_nodes: ["n1.xyz.com", "n2.xyz.com", "n3.xyz.com"]
```

*on n2*

```
discovery.type: eskka.EskkaDiscoveryModule

discovery.eskka.host: n2.xyz.com

discovery.eskka.seed_nodes: ["n1.xyz.com", "n2.xyz.com", "n3.xyz.com"]

```

*on n3*

```
discovery.type: eskka.EskkaDiscoveryModule

discovery.eskka.host: n3.xyz.com

discovery.eskka.seed_nodes: ["n1.xyz.com", "n2.xyz.com", "n3.xyz.com"]

```

### three instances on three separate hosts using different ports

addresses are `n1.xyz.com:9401`, `n2.xyz.com:9402` and `n3.xyz.com:9403`.

*on n1*

```
discovery.type: eskka.EskkaDiscoveryModule

discovery.eskka.host: n1.xyz.com

discovery.eskka.port: 9401

discovery.eskka.seed_nodes: ["n1.xyz.com:9401", "n2.xyz.com:9402", "n3.xyz.com:9403"]
```

*on n2*

```
discovery.type: eskka.EskkaDiscoveryModule

discovery.eskka.host: n2.xyz.com

discovery.eskka.port: 9402

discovery.eskka.seed_nodes: ["n1.xyz.com:9401", "n2.xyz.com:9402", "n3.xyz.com:9403"]
```

*on n3*

```
discovery.type: eskka.EskkaDiscoveryModule

discovery.eskka.host: n3.xyz.com

discovery.eskka.port: 9403

discovery.eskka.seed_nodes: ["n1.xyz.com:9401", "n2.xyz.com:9402", "n3.xyz.com:9403"]
```

