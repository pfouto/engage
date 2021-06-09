import messages.mixed.MetadataFlush
import pt.unl.fct.di.novasys.network.data.Host

data class TreeFile(val nodes: Map<String, PartitionInfo>, val links: List<List<String>>)

data class PartitionInfo(val local_db: Boolean, val partitions: List<String>?)

data class NeighState(
    var connected: Boolean = false, var partitions: Map<String, Pair<Host, Int>> = mutableMapOf(),
    var pendingMF: MetadataFlush? = null,
)