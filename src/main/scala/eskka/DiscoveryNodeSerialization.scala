package eskka

import akka.util.{ ByteString, ByteStringBuilder }
import org.elasticsearch.cluster.node.DiscoveryNode
import org.elasticsearch.common.io.stream.{ InputStreamStreamInput, OutputStreamStreamOutput }

object DiscoveryNodeSerialization {

  def fromBytes(bytes: ByteString): DiscoveryNode = {
    DiscoveryNode.readNode(new InputStreamStreamInput(bytes.iterator.asInputStream))
  }

  def toBytes(node: DiscoveryNode): ByteString = {
    val bsb = new ByteStringBuilder
    val out = new OutputStreamStreamOutput(bsb.asOutputStream)
    node.writeTo(out)
    out.close()
    bsb.result()
  }

}
