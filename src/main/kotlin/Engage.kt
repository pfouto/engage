import messages.peer.GossipMessage
import org.apache.logging.log4j.LogManager
import pt.unl.fct.di.novasys.babel.core.GenericProtocol
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.channel.simpleclientserver.SimpleServerChannel
import pt.unl.fct.di.novasys.channel.simpleclientserver.events.ClientDownEvent
import pt.unl.fct.di.novasys.channel.simpleclientserver.events.ClientUpEvent
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel
import pt.unl.fct.di.novasys.channel.tcp.events.*
import pt.unl.fct.di.novasys.network.data.Host
import timers.GossipTimer
import timers.ReconnectTimer
import java.net.InetAddress
import java.util.*

const val DEFAULT_SERVER_PORT = "1500"
const val DEFAULT_PEER_PORT = "1600"

class Engage(props: Properties, myInfo: PartitionInfo, links: ArrayList<String>) :
    GenericProtocol("Engage", 100) {

    private val logger = LogManager.getLogger()

    private val peerChannel: Int
    private val me: Host
    private val serverChannel: Int
    private val peerPort: Int

    private val reconnectInterval: Long = props.getProperty("reconnect_interval", "1000").toLong()
    private val gossipInterval: Long = props.getProperty("gossip_interval", "5000").toLong()

    private val localDB: Boolean = myInfo.local_db
    private val partitions: List<String> = myInfo.partitions ?: emptyList()
    private val neighbours: Map<Host, NeighState>

    init {
        val address = props.getProperty("address")
        peerPort = props.getProperty("peer.port", DEFAULT_PEER_PORT).toInt()
        me = Host(InetAddress.getByName(address), peerPort)

        val serverProps = Properties()
        serverProps.setProperty(SimpleServerChannel.ADDRESS_KEY, address)
        serverProps.setProperty(SimpleServerChannel.PORT_KEY, props.getProperty("server.port", DEFAULT_SERVER_PORT))
        serverChannel = createChannel(SimpleServerChannel.NAME, serverProps)

        val peerProps = Properties()
        peerProps.setProperty(TCPChannel.ADDRESS_KEY, address)
        peerProps.setProperty(TCPChannel.PORT_KEY, peerPort.toString())
        peerChannel = createChannel(TCPChannel.NAME, peerProps)

        neighbours = mutableMapOf()
        for (link in links) {
            val addr = InetAddress.getByName(link) ?: throw AssertionError("Could not read neighbour $link")
            neighbours[Host(addr, peerPort)] = NeighState()
        }
    }

    override fun init(props: Properties?) {
        registerChannelEventHandler(peerChannel, OutConnectionFailed.EVENT_ID, this::onOutConnectionFailed)
        registerChannelEventHandler(peerChannel, OutConnectionDown.EVENT_ID, this::onOutConnectionDown)
        registerChannelEventHandler(peerChannel, InConnectionDown.EVENT_ID, this::onInConnectionDown)
        registerChannelEventHandler(peerChannel, OutConnectionUp.EVENT_ID, this::onOutConnectionUp)
        registerChannelEventHandler(peerChannel, InConnectionUp.EVENT_ID, this::onInConnectionUp)
        registerChannelEventHandler(serverChannel, ClientDownEvent.EVENT_ID, this::onClientDown)
        registerChannelEventHandler(serverChannel, ClientUpEvent.EVENT_ID, this::onClientUp)

        registerTimerHandler(ReconnectTimer.TIMER_ID, this::onReconnectTimer)
        registerTimerHandler(GossipTimer.TIMER_ID, this::onGossipTimer)

        registerMessageSerializer(peerChannel, GossipMessage.MSG_ID, GossipMessage.serializer)
        registerMessageHandler(peerChannel, GossipMessage.MSG_ID, this::onGossipMessage, this::onMessageFailed)

        neighbours.keys.forEach {
            logger.info("Connecting to $it")
            openConnection(it, peerChannel)
        }

        setupPeriodicTimer(GossipTimer(), gossipInterval, gossipInterval)

        logger.info("Engage started, neighbours: {}", neighbours)
    }

    private fun onGossipTimer(timer: GossipTimer, uId: Long) {
        logger.info("Gossip:")
        logger.info(neighbours)
        neighbours.filter { it.value.connected }.forEach { (t, _) -> sendMessage(peerChannel, buildMessageFor(t), t) }
    }

    private fun buildMessageFor(t: Host): GossipMessage {
        val map: MutableMap<String, Pair<Host, Int>> = mutableMapOf()
        partitions.forEach { map[it] = Pair(me, 1) }
        neighbours.filter { it.key != t }.forEach { (_, pI) ->
            pI.partitions.forEach { (p, pair) ->
                if (map[p] == null || map[p]!!.second > (pair.second + 1))
                    map[p] = Pair(pair.first, pair.second + 1)
            }
        }
        return GossipMessage(map)
    }

    private fun onGossipMessage(msg: GossipMessage, from: Host, sourceProto: Short, channelId: Int) {
        logger.info("Received $msg from ${from.address.canonicalHostName}")
        val info = neighbours[from] ?: throw AssertionError("Not in neighbours list: $from")
        info.partitions = msg.parts
    }

    private fun onMessageFailed(msg: ProtoMessage, to: Host, destProto: Short, cause: Throwable, channelId: Int) {
        logger.warn("Message $msg to $to failed: ${cause.localizedMessage}")
    }

    private fun onOutConnectionUp(event: OutConnectionUp, channelId: Int) {
        logger.info("Connected to ${event.node}")
        val info = neighbours[event.node] ?: throw AssertionError("Not in neighbours list: ${event.node}")
        info.connected = true
    }

    private fun onOutConnectionFailed(event: OutConnectionFailed<ProtoMessage>, channelId: Int) {
        logger.warn("Failed connecting to ${event.node}: ${event.cause.localizedMessage}, retrying in $reconnectInterval")
        setupTimer(ReconnectTimer(event.node), reconnectInterval)
    }

    private fun onReconnectTimer(timer: ReconnectTimer, uId: Long) {
        logger.info("Connecting to ${timer.node}")
        openConnection(timer.node, peerChannel)
    }

    private fun onOutConnectionDown(event: OutConnectionDown, channelId: Int) {
        logger.warn("Lost connection to ${event.node}, reconnecting in $reconnectInterval")
        setupTimer(ReconnectTimer(event.node), reconnectInterval)
        val info = neighbours[event.node] ?: throw AssertionError("Not in neighbours list: ${event.node}")
        info.connected = false
    }

    private fun onInConnectionUp(event: InConnectionUp, channelId: Int) {
        logger.debug("Connection in from ${event.node}")
    }

    private fun onInConnectionDown(event: InConnectionDown, channelId: Int) {
        logger.debug("Connection out from ${event.node}")
    }

    private fun onClientUp(event: ClientUpEvent, channelId: Int) {

    }

    private fun onClientDown(event: ClientDownEvent, channelId: Int) {

    }

}