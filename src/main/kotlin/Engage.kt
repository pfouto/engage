import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.config.DefaultDriverOption
import com.datastax.oss.driver.api.core.config.DriverConfigLoader
import messages.mixed.MetadataFlush
import messages.mixed.UpdateNot
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
import timers.FlushTimer
import timers.GossipTimer
import timers.ReconnectTimer
import java.net.InetAddress
import java.net.InetSocketAddress
import java.time.Duration
import java.util.*
import kotlin.streams.toList
import kotlin.system.exitProcess

const val DEFAULT_SERVER_PORT = "1500"
const val DEFAULT_PEER_PORT = "1600"

class Engage(
    props: Properties, private val myName: String, myInfo: PartitionInfo, links: ArrayList<String>,
    private val targets: Map<String, List<String>>,
    private val all: Map<String, String>,
) : GenericProtocol("Engage", 100) {

    private val logger = LogManager.getLogger()

    private val me: Host
    private val peerChannel: Int
    private val serverChannel: Int?
    private var localClient: Host? = null
    private val peerPort: Int

    private val reconnectInterval: Long = props.getProperty("reconnect_interval", "5000").toLong()
    private val gossipInterval: Long = props.getProperty("gossip_interval", "5000").toLong()

    private val mfEnabled: Boolean
    private val mfTimeoutMs: Long
    private val bayouStabMs: Int

    private val localDB: Boolean = myInfo.local_db
    private val partitions: List<String> = myInfo.partitions ?: emptyList()
    private val neighbours: Map<Host, NeighState>

    private var firstClient: Boolean

    private var nUpdateNotMessages: Int
    private var nMetadataFlushMessages: Int

    init {
        val address = props.getProperty("address")
        peerPort = props.getProperty("peer.port", DEFAULT_PEER_PORT).toInt()
        me = Host(InetAddress.getByName(address), peerPort)
        mfEnabled = props.getProperty("mf_enabled").toBoolean()
        mfTimeoutMs = props.getProperty("mf_timeout_ms").toLong()
        bayouStabMs = props.getProperty("bayou.stab_ms").toInt()
        firstClient = props.getProperty("setup_cass").toBoolean()

        nUpdateNotMessages = 0
        nMetadataFlushMessages = 0

        serverChannel = if (localDB) {
            val serverProps = Properties()
            serverProps.setProperty(SimpleServerChannel.ADDRESS_KEY, "localhost")
            serverProps.setProperty(SimpleServerChannel.PORT_KEY, props.getProperty("server.port", DEFAULT_SERVER_PORT))
            serverProps.setProperty(SimpleServerChannel.HEARTBEAT_INTERVAL_KEY, "3000")
            serverProps.setProperty(SimpleServerChannel.HEARTBEAT_TOLERANCE_KEY, "10000")

            createChannel(SimpleServerChannel.NAME, serverProps)
        } else null

        val peerProps = Properties()
        peerProps.setProperty(TCPChannel.ADDRESS_KEY, address)
        peerProps.setProperty(TCPChannel.PORT_KEY, peerPort.toString())
        peerProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "3000")
        peerProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "10000")
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

        registerMessageSerializer(peerChannel, GossipMessage.MSG_ID, GossipMessage.serializer)
        registerMessageSerializer(peerChannel, MetadataFlush.MSG_ID, MetadataFlush.serializer)
        registerMessageSerializer(peerChannel, UpdateNot.MSG_ID, UpdateNot.serializer)

        registerMessageHandler(peerChannel, GossipMessage.MSG_ID, this::onGossipMessage, this::onMessageFailed)
        registerMessageHandler(peerChannel, UpdateNot.MSG_ID, this::onPeerUpdateNot, this::onMessageFailed)
        registerMessageHandler(peerChannel, MetadataFlush.MSG_ID, this::onPeerMetadataFlush, this::onMessageFailed)

        if (serverChannel != null) {
            registerChannelEventHandler(serverChannel, ClientDownEvent.EVENT_ID, this::onClientDown)
            registerChannelEventHandler(serverChannel, ClientUpEvent.EVENT_ID, this::onClientUp)

            registerMessageSerializer(serverChannel, TargetsMessage.MSG_ID, TargetsMessage.serializer)
            registerMessageSerializer(serverChannel, UpdateNot.MSG_ID, UpdateNot.serializer)
            registerMessageSerializer(serverChannel, MetadataFlush.MSG_ID, MetadataFlush.serializer)

            registerMessageHandler(serverChannel,
                UpdateNot.MSG_ID,
                this::onClientUpdateNot,
                this::onMessageFailed)
        }

        registerTimerHandler(ReconnectTimer.TIMER_ID, this::onReconnectTimer)
        registerTimerHandler(GossipTimer.TIMER_ID, this::onGossipTimer)
        registerTimerHandler(FlushTimer.TIMER_ID, this::onFlushTimer)

        neighbours.keys.forEach {
            setupTimer(ReconnectTimer(it), 1500)
        }

        setupPeriodicTimer(GossipTimer(), 1500 + gossipInterval, gossipInterval)

        logger.info("Engage started, me $me, mf ${if (mfEnabled) "YES" else "NO"}" +
                "${if (mfEnabled) ", mfTo $mfTimeoutMs" else ""}, bTo $bayouStabMs neighs: " +
                "${neighbours.keys.stream().map { s -> s.address.hostAddress.substring(10) }.toList()}")
    }

    private fun onGossipTimer(timer: GossipTimer, uId: Long) {
        if (logger.isDebugEnabled)
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
        if (logger.isDebugEnabled)
            logger.debug("Received $msg from ${from.address.hostAddress}")
        val info = neighbours[from] ?: throw AssertionError("Not in neighbours list: $from")
        info.partitions = msg.parts
    }

    private fun onFlushTimer(timer: FlushTimer, uId: Long) {
        if (!mfEnabled) throw AssertionError("Received $timer but mf are disabled")
        if (logger.isDebugEnabled)
            logger.debug("Flush: ${timer.edge}")
        val neighState = neighbours[timer.edge]!!
        if (neighState.pendingMF != null) {
            nMetadataFlushMessages++
            sendMessage(peerChannel, neighState.pendingMF, timer.edge)
            neighState.pendingMF = null
        }
    }

    private fun propagateUN(msg: UpdateNot, sourceEdge: Host?) {
        neighbours.forEach { (neigh, nState) ->
            if (neigh != sourceEdge) {
                if (msg.part == "migration" || nState.partitions.containsKey(msg.part)) {
                    //Direct propagate (and flush MF if enabled)
                    val toSend: UpdateNot
                    if (mfEnabled && nState.pendingMF != null) {
                        toSend = msg.copyMergingMF(nState.pendingMF!!)
                        nState.pendingMF = null
                        cancelTimer(nState.timerId)
                    } else toSend = msg
                    nUpdateNotMessages++
                    sendMessage(peerChannel, toSend, neigh)
                } else if (mfEnabled) {
                    if(mfTimeoutMs > 0) {
                        //Either merge msg to mf, or store and create timer
                        if (nState.pendingMF != null) {
                            if (msg.mf != null)
                                nState.pendingMF!!.merge(msg.mf)
                            nState.pendingMF!!.merge(msg.source, msg.vUp)
                        } else {
                            nState.pendingMF = MetadataFlush.single(msg.source, msg.vUp)
                            if (msg.mf != null)
                                nState.pendingMF!!.merge(msg.mf)
                            nState.timerId = setupTimer(FlushTimer(neigh), mfTimeoutMs)
                        }
                    } else {
                        val mf = MetadataFlush.single(msg.source, msg.vUp)
                        if (msg.mf != null) mf.merge(msg.mf)
                        nMetadataFlushMessages++
                        sendMessage(peerChannel,  mf, neigh)
                    }
                }
            }
        }
    }

    private fun onClientUpdateNot(msg: UpdateNot, from: Host, sourceProto: Short, channelId: Int) {
        if (logger.isDebugEnabled)
            logger.debug("Received $msg from client")
        propagateUN(msg, null)
    }

    private fun onPeerUpdateNot(msg: UpdateNot, from: Host, sourceProto: Short, channelId: Int) {
        if (logger.isDebugEnabled)
            logger.debug("Received $msg from peer ${from.address.hostAddress}")
        if (!neighbours.containsKey(from)) throw AssertionError("Msg from unknown neigh $from")
        propagateUN(msg, from)

        if (serverChannel != null) {
            if (msg.part == "migration" || partitions.contains(msg.part)) {
                sendMessage(serverChannel, msg, localClient!!)
            } else if (mfEnabled) {
                val single = MetadataFlush.single(msg.source, msg.vUp)
                if (msg.mf != null) single.merge(msg.mf)
                sendMessage(serverChannel, single, localClient!!)
            }
        }
    }

    private fun onPeerMetadataFlush(msg: MetadataFlush, from: Host, sourceProto: Short, channelId: Int) {
        if (!mfEnabled) throw AssertionError("Received $msg but mf are disabled")
        if (logger.isDebugEnabled)
            logger.debug("Received $msg from ${from.address.hostAddress}")
        if (!neighbours.containsKey(from)) throw AssertionError("Msg from unknown neigh $from")

        neighbours.forEach { (neigh, nState) ->
            if (neigh != from) {
                val toSend: MetadataFlush
                if (nState.pendingMF != null) {
                    toSend = nState.pendingMF!!
                    toSend.merge(msg)
                    nState.pendingMF = null
                    cancelTimer(nState.timerId)
                } else {
                    toSend = msg
                }
                nUpdateNotMessages++
                sendMessage(peerChannel, toSend, neigh)
            }
        }
        if (serverChannel != null)
            sendMessage(serverChannel, msg, localClient!!)
    }

    private fun onMessageFailed(msg: ProtoMessage, to: Host, destProto: Short, cause: Throwable, channelId: Int) {
        logger.warn("Message $msg to $to failed: ${cause.localizedMessage}")
    }

    private fun onOutConnectionUp(event: OutConnectionUp, channelId: Int) {
        if (logger.isDebugEnabled)
            logger.debug("Connected out to ${event.node}")
        val info = neighbours[event.node] ?: throw AssertionError("Not in neighbours list: ${event.node}")
        info.connected = true
    }

    private fun onOutConnectionFailed(event: OutConnectionFailed<ProtoMessage>, channelId: Int) {
        logger.warn("Failed connecting out to ${event.node}: ${event.cause.localizedMessage}, retrying in $reconnectInterval")
        setupTimer(ReconnectTimer(event.node), reconnectInterval)
    }

    private fun onReconnectTimer(timer: ReconnectTimer, uId: Long) {
        if (logger.isDebugEnabled)
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
        if (logger.isDebugEnabled)
            logger.debug("Connection in up from ${event.node}")
    }

    private fun onInConnectionDown(event: InConnectionDown, channelId: Int) {
        if (logger.isDebugEnabled)
            logger.warn("Connection in down from ${event.node}")
    }

    private fun onClientUp(event: ClientUpEvent, channelId: Int) {
        if (firstClient) {
            firstClient = false
            logger.info("Client connection up from ${event.client}, creating partitions and tables")
            localClient = event.client
            try {
                val loader = DriverConfigLoader.programmaticBuilder()
                    .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(10)).build()
                CqlSession.builder().addContactPoint(InetSocketAddress.createUnresolved(me.address.hostAddress, 9042))
                    .withLocalDatacenter("datacenter1").withConfigLoader(loader).build().use { session ->

                        if (logger.isDebugEnabled) logger.debug("Dropping partition migration")
                        session.execute("DROP KEYSPACE IF EXISTS migration")
                        if (logger.isDebugEnabled) logger.debug("Creating partition migration")
                        session.execute("CREATE KEYSPACE migration WITH replication = " +
                                "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
                        if (logger.isDebugEnabled) logger.debug("Creating table migration.migration")
                        session.execute("create table migration.migration " +
                                "(y_id varchar primary key, field0 varchar, clock blob)")

                        for (partition in partitions) {
                            if (logger.isDebugEnabled) logger.debug("Dropping partition $partition")
                            session.execute("DROP KEYSPACE IF EXISTS $partition")
                            if (logger.isDebugEnabled) logger.debug("Creating partition $partition")
                            session.execute("CREATE KEYSPACE $partition WITH replication = " +
                                    "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
                            if (logger.isDebugEnabled) logger.debug("Creating table $partition.usertable")
                            session.execute("create table ${partition}.usertable " +
                                    "(y_id varchar primary key, field0 varchar, clock blob)")
                        }

                    }
            } catch (e: Exception) {
                logger.error("Error setting up partitions, exiting... ${e.message}")
                e.printStackTrace()
                exitProcess(1)
            }
            logger.info("Setup completed.")
        } else {
            logger.info("Client connection up from ${event.client}, skipping partitions recreation")
            localClient = event.client
        }
        if (logger.isDebugEnabled) logger.debug("Sending targets msg: $targets")
        sendMessage(serverChannel!!, TargetsMessage(myName, targets, all, bayouStabMs), event.client)
    }

    private fun onClientDown(event: ClientDownEvent, channelId: Int) {
        logger.warn("Client connection down from ${event.client}: ${event.cause}")
    }

    fun finalLogs() {
        logger.info("Number of message: {} {} {}",
            nUpdateNotMessages+nMetadataFlushMessages, nUpdateNotMessages, nMetadataFlushMessages)
    }

}