package eskka

import akka.util.{ ByteString, ByteStringBuilder }
import org.elasticsearch.cluster.node.DiscoveryNode
import org.elasticsearch.cluster.{ ClusterName, ClusterState }
import org.elasticsearch.common.compress.deflate.DeflateCompressor
import org.elasticsearch.common.io.stream._

object ClusterStateSerialization {

  private val compressor = new DeflateCompressor

  def toBytes(clusterState: ClusterState): ByteString = {
    val bsb = new ByteStringBuilder
    val out = compressor.streamOutput(new OutputStreamStreamOutput(bsb.asOutputStream))
    clusterState.writeTo(out)
    out.close()
    bsb.result()
  }

  def fromBytes(bytes: ByteString, localNode: DiscoveryNode): ClusterState = {
    val in = compressor.streamInput(new InputStreamStreamInput(bytes.iterator.asInputStream))
    ClusterState.Builder.readFrom(in, localNode)
  }

}
