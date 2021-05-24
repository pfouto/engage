import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.config.DefaultDriverOption
import com.datastax.oss.driver.api.core.config.DriverConfigLoader
import messages.peer.GossipMessage
import messages.server.TargetsMessage
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
import java.time.Duration
import java.util.*
import kotlin.system.exitProcess


const val DEFAULT_SERVER_PORT = "1500"
const val DEFAULT_PEER_PORT = "1600"

class Engage(
    props: Properties,
    myInfo: PartitionInfo,
    links: ArrayList<String>,
    private val targets: Map<String, List<String>>,
    private val all: Set<String>,
) :
    GenericProtocol("Engage", 100) {

    private val logger = LogManager.getLogger()

    private val peerChannel: Int
    private val me: Host
    private val serverChannel: Int?
    private val peerPort: Int

    private val reconnectInterval: Long = props.getProperty("reconnect_interval", "5000").toLong()
    private val gossipInterval: Long = props.getProperty("gossip_interval", "5000").toLong()

    private val localDB: Boolean = myInfo.local_db
    private val partitions: List<String> = myInfo.partitions ?: emptyList()
    private val neighbours: Map<Host, NeighState>

    init {
        val address = props.getProperty("address")
        peerPort = props.getProperty("peer.port", DEFAULT_PEER_PORT).toInt()
        me = Host(InetAddress.getByName(address), peerPort)

        serverChannel = if (localDB) {
            val serverProps = Properties()
            serverProps.setProperty(SimpleServerChannel.ADDRESS_KEY, "localhost")
            serverProps.setProperty(SimpleServerChannel.PORT_KEY, props.getProperty("server.port", DEFAULT_SERVER_PORT))
            createChannel(SimpleServerChannel.NAME, serverProps)
        } else {
            null
        }

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
        if (serverChannel != null) {
            registerChannelEventHandler(serverChannel, ClientDownEvent.EVENT_ID, this::onClientDown)
            registerChannelEventHandler(serverChannel, ClientUpEvent.EVENT_ID, this::onClientUp)
            registerMessageSerializer(serverChannel, TargetsMessage.MSG_ID, TargetsMessage.serializer)
        }

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
        logger.debug("Gossip: $neighbours")
        neighbours.filter { it.value.connected }.keys.forEach { sendMessage(peerChannel, buildMessageFor(it), it) }
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
        logger.debug("Received $msg from ${from.address.canonicalHostName}")
        val info = neighbours[from] ?: throw AssertionError("Not in neighbours list: $from")
        info.partitions = msg.parts
    }

    private fun onMessageFailed(msg: ProtoMessage, to: Host, destProto: Short, cause: Throwable, channelId: Int) {
        logger.warn("Message $msg to $to failed: ${cause.localizedMessage}")
    }

    private fun onOutConnectionUp(event: OutConnectionUp, channelId: Int) {
        logger.info("Connected out to ${event.node}")
        val info = neighbours[event.node] ?: throw AssertionError("Not in neighbours list: ${event.node}")
        info.connected = true
    }

    private fun onOutConnectionFailed(event: OutConnectionFailed<ProtoMessage>, channelId: Int) {
        logger.warn("Failed connecting out to ${event.node}: ${event.cause.localizedMessage}, retrying in $reconnectInterval")
        setupTimer(ReconnectTimer(event.node), reconnectInterval)
    }

    private fun onReconnectTimer(timer: ReconnectTimer, uId: Long) {
        logger.debug("Connecting out to ${timer.node}")
        openConnection(timer.node, peerChannel)
    }

    private fun onOutConnectionDown(event: OutConnectionDown, channelId: Int) {
        logger.warn("Lost connection out to ${event.node}, reconnecting in $reconnectInterval")
        setupTimer(ReconnectTimer(event.node), reconnectInterval)
        val info = neighbours[event.node] ?: throw AssertionError("Not in neighbours list: ${event.node}")
        info.connected = false
    }

    private fun onInConnectionUp(event: InConnectionUp, channelId: Int) {
        logger.debug("Connection in up from ${event.node}")
    }

    private fun onInConnectionDown(event: InConnectionDown, channelId: Int) {
        logger.warn("Connection in down from ${event.node}")
    }

    private fun onClientUp(event: ClientUpEvent, channelId: Int) {
        logger.info("Client connection up from ${event.client}, creating partitions and tables")

        val loader = DriverConfigLoader.programmaticBuilder()
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(10)).build()
        CqlSession.builder().withConfigLoader(loader).build().use { session ->
            try {
                for (partition in partitions) {
                    logger.debug("Dropping partition $partition")
                    session.execute("DROP KEYSPACE IF EXISTS $partition")
                    logger.debug("Creating partition $partition")
                    session.execute("CREATE KEYSPACE $partition WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
                    logger.debug("Creating table $partition.usertable")
                    session.execute("create table ${partition}.usertable (y_id varchar primary key, field0 varchar, clock blob)")
                }
            } catch (e: Exception) {
                logger.error("Error setting up partitions, exiting... ${e.message}")
                e.printStackTrace()
                exitProcess(1)
            }
        }
        logger.debug("Sending targets msg: $targets")
        sendMessage(serverChannel!!, TargetsMessage(targets, all), event.client)
        logger.info("Setup completed.")
    }

    private fun onClientDown(event: ClientDownEvent, channelId: Int) {
        logger.warn("Client connection down from ${event.client}: ${event.cause}")
    }

}