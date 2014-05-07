package eskka

import org.elasticsearch.Version
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.node.DiscoveryNode
import org.elasticsearch.common.compress.CompressorFactory
import org.elasticsearch.common.io.stream._

object ClusterStateSerialization {

  def toBytes(version: Version, clusterState: ClusterState): Array[Byte] = {
    val bStream = new BytesStreamOutput
    val out = new HandlesStreamOutput(CompressorFactory.defaultCompressor.streamOutput(bStream))
    out.setVersion(version)
    ClusterState.Builder.writeTo(clusterState, out)
    out.close()
    bStream.bytes.toBytes
  }

  def fromBytes(version: Version, bytes: Array[Byte], localNode: DiscoveryNode): ClusterState = {
    val compressor = Option(CompressorFactory.compressor(bytes))
    val bStream = new BytesStreamInput(bytes, false)
    val in = compressor.fold(CachedStreamInput.cachedHandles(bStream))(CachedStreamInput.cachedHandlesCompressed(_, bStream))
    in.setVersion(version)
    ClusterState.Builder.readFrom(in, localNode)
  }

}
