import messages.mixed.MetadataFlush
import pt.unl.fct.di.novasys.network.data.Host

data class TreeFile(val nodes: Map<String, PartitionInfo>, val links: List<List<String>>)

data class PartitionInfo(val local_db: Boolean, val name: String?, val partitions: List<String>?)

data class NeighState(
    var connected: Boolean = false, var partitions: Map<String, Pair<Host, Int>> = mutableMapOf(),
    var pendingMF: MetadataFlush? = null, var timerId: Long = -1,
) {
    override fun toString(): String {
        return "(${if (connected) "up" else "down"}, ${partitions.keys}" +
                "${if (pendingMF != null) ", mf=$pendingMF" else ""}${if (timerId != -1L) ", tid=$timerId" else ""})"
    }
}