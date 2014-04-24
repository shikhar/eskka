package eskka

import scala.collection.immutable.SortedSet
import scala.concurrent.duration.FiniteDuration

import akka.actor.{Actor, ActorLogging, Address}
import akka.cluster.{Cluster, ClusterEvent, Member}
import akka.cluster.ClusterEvent.CurrentClusterState

class PartitionResolver(revaluateInterval: FiniteDuration) extends Actor with ActorLogging {

  import PartitionResolver._

  import context.dispatcher

  private[this] val cluster = Cluster(context.system)

  override def preStart() {
    cluster.subscribe(self, ClusterEvent.InitialStateAsEvents, classOf[ClusterEvent.UnreachableMember])
  }

  override def receive = {

    case ClusterEvent.UnreachableMember(m) =>
      scheduleReval(m, "first observation")

    case Revaluate(m) =>
      val cs = cluster.state
      if (cs.members.contains(m)) {
        if (cs.unreachable.contains(m)) {
          if (seenByQuorumOfSeedNodes(cluster.settings.SeedNodes, cs)) {
            if (isMaster(cs, m.address)) {
              log.info("[{}] is master, and its unreachability has has been observed by quorum of master-eligible nodes -- downing!", m)
              cluster.down(m.address)
            } else {
              scheduleReval(m, "not master")
            }
          } else {
            scheduleReval(m, "not been observed by a quorum of master-eligible nodes, we may be on a minority partition")
          }
        } else {
          log.info("[{}] is reachable now", m)
        }
      } else {
        log.info("[{}] is no longer part of the cluster", m)
      }

  }

  private def scheduleReval(m: Member, reason: String) {
    log.info("Scheduling revaluation of unreachable [{}] after [{}] -- reason [{}]", m, revaluateInterval, reason)
    context.system.scheduler.scheduleOnce(revaluateInterval, self, Revaluate(m))
  }

  override def postStop() {
    cluster.unsubscribe(self)
  }

}

object PartitionResolver {

  private val MemberAgeOrdering = Ordering.fromLessThan[Member](_.isOlderThan(_))

  private case class Revaluate(member: Member)

  private def masterEligibleMembersByAge(cs: CurrentClusterState) =
    SortedSet(cs.members.filter(_.hasRole(MasterRole)).toSeq: _*)(MemberAgeOrdering)

  private def isMaster(cs: CurrentClusterState, address: Address) =
    masterEligibleMembersByAge(cs).headOption.exists(_.address == address)

  private def seenByQuorumOfSeedNodes(seedNodes: Seq[Address], cs: CurrentClusterState) =
    seedNodes.count(cs.seenBy.contains) >= (seedNodes.size / 2) + 1

}
