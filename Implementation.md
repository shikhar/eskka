# References

[Akka Cluster Spec](http://doc.akka.io/docs/akka/snapshot/common/cluster.html) - in particular the membership lifecycle.

Elasticsearch [ZenDiscovery](https://github.com/elasticsearch/elasticsearch/blob/master/src/main/java/org/elasticsearch/discovery/zen/ZenDiscovery.java), which implements the [Discovery](https://github.com/elasticsearch/elasticsearch/blob/master/src/main/java/org/elasticsearch/discovery/Discovery.java) interface. All with the goal of handling [ClusterState](https://github.com/elasticsearch/elasticsearch/blob/master/src/main/java/org/elasticsearch/cluster/ClusterState.java) updates - our focus is on the [DiscoveryNodes](https://github.com/elasticsearch/elasticsearch/blob/master/src/main/java/org/elasticsearch/cluster/node/DiscoveryNodes.java) that go into it.

# Plumbing

## `eskka.EskkaDiscoveryModule`

Here we bind `EskkaDiscovery` to be the ES `Discovery` implementation to be used.

eskka will get used if the config specifies: 

```
discovery.type: eskka.EskkaDiscoveryModule
```

## `eskka.EskkaDiscovery`

`EskkaDiscovery` gets all the objects we will need from ES via Guice DI in its constructor.

It is implementing the `Discovery` interface from ES.

`doStart()/doStop()/doClose()` are the life-cycle methods we are hooking into.

Here the Akka cluster to be used gets initialized or torn down. Initialization takes the form of creating an EskkaCluster and `start()`ing it. Since the startup is non-blocking, we have some timeout handling where if the timeout expires we attempt a do-over. When creating the EskkaCluster in `makeEskkaCluster()`, we also provide it a `restartHook`, so it can also invoke this do-over logic at any point.

`publish()` is delegated to the `EskkaCluster`.

## `eskka.EskkaCluster`

This is providing the meat to the `EskkaDiscovery` implementation.

The akka `ActorSystem` is created via `makeActorSystem()` which gets some properties from the ES settings that eskka allows for configuring. Most of the custom configuration happens here (the other place is the `application.conf` file).

The akka `Cluster` extension is initialized using this `ActorSystem`.

When we are advised to `start()`, we register a callback with the cluster for when the current node has been marked as UP. This is where we carry-out the remaining initialization of starting eskka app actors, which will be described: `Pinger`, `Follower`, `QuorumBasedPartitionMonitor`, `Master` (managed via `ClusterSingletonManager`), `QuorumLossAbdicator`.

For the purpose of informing any `InitialStateDiscoveryListener`s which may have been registered, we register a request with the `Follower` actor to send a message when it has received its first state publish.

For `publish()`, we send a `PublishReq` message to the `Master` with the sender being a `PublishResponseHandler` which is responsible for feeding-back to the ES `AckListener`. Note that acks are only expected from non-master nodes.

# Actors

## `eskka.singleton.ClusterSingletonManager`

It is a copy-paste from `akka.contrib.pattern.ClusterSingletonManager`, implementing the [cluster singleton pattern](http://doc.akka.io/docs/akka/snapshot/contrib/cluster-singleton.html). Copying into app namespace is the [suggested way](http://doc.akka.io/docs/akka/snapshot/contrib/) of using `akka-contrib`.

## `eskka.Master`

This actor is managed by a cluster singleton, ensuring it will only be started on a single node at any time, which is annointed ES master in state publishing.

Discovery of nodes is managed by subscribing to the Akka cluster's `MemberEvent`s:

- UP: consider for inclusion in the ES `DiscoveryNodes` 
- EXITED: remove from consideration
- REMOVED: remove from consideration

The difference between being in consideration for inclusion in `DiscoveryNodes` and being actually included, is the time between a node being UP for the Akka cluster and receiving from the corresponding remote `Follower` actor its `ActorRef` and `DiscoveryNode` info.

Discovery state changes are propagated to ES by being queued up (`pendingDiscoverySubmits`) and drained periodically (`DrainQueuedDiscoverySubmits`). Draining involves calling ES `ClusterService.submitStateUpdateTask()`, where an update task is essentially a function `ClusterState -> ClusterState` (implemented in `discoveryState()`). This submit will eventually trigger a publish.

External requests to publish `ClusterState` to other nodes in the cluster are received as `PublishReq` messages. Here the `ClusterState` is serialized and compressed and transmitted to the remote `Follower` actors. The transmission is chunked because the serialized state can be large (there are reports of tens/hundreds of MB's) and we don't want to overload the akka remoting layer with huge messages. We forward the original `sender()` along, for the `Follower` to send its `PublishAck` to.

## `eskka.Follower`

It handles:

- Responding to `Master` 'announcement' with its `ActorRef` and the local `DiscoveryNode` so it can be recognized as part of the ES cluster. 
- Assembling publish request chunks, verifying their consistency. Deserializing the `ClusterState` received in this manner and submitting this update to the local ES `ClusterService`.
- Keeping track of the first submission received (this gets used for the `InitialStateDiscoveryListener` callbacks).

## `eskka.Pinger`

This actor's raison-de-etre is for A to be able to request B to ping C, and for A to receive a response from B whether the ping succeeded within the specified timeout.

## `eskka.QuorumBasedPartitionMonitor`

This is central to eskka's partition-resolution strategy. We subscribe to cluster `MemberEvent`s as well as `ReachabilityEvent`s.

Member events (UP, EXITED, REMOVED) feed into state tracking of which voting nodes are available - their `Pinger` remote `ActorRef`s are stored.

Reachability events (UNREACHABLE, REACHABLE) are used towards failure-handling. In case of an unreachable member, we schedule an 'evaluation' after a randomized delay. In case we receive a reachable event before the decision to down it has been made, the evaluation is cancelled.

The evaluation takes the form of requesting pings from all voting members to the unreachable node. If a quorum of them responds that the ping timed-out, the unreachable node is marked as `DOWN` from the Akka cluster, which will cause the member removal events to be received on subscribers and any resulting consequences (e.g. master failover via the `ClusterSingletonManager` if indicated, or discovery state update from the master).

## `eskka.QuorumLossAbdicator`

This actor ensures that ES nodes on a minority partition do not stay active. It regularly checks whether a quorum of the voting members are UP and REACHABLE as far as the Akka cluster layer is concerned. If not, the ES cluster state is cleared and the global eskka `restartHook` is invoked, which should trigger eskka reinitialization.
