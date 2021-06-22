import com.google.gson.Gson
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import pt.unl.fct.di.novasys.babel.core.Babel
import java.io.Reader
import java.net.Inet4Address
import java.net.InetAddress
import java.net.NetworkInterface
import java.nio.file.Files
import java.nio.file.Paths
import java.security.InvalidParameterException
import java.util.*
import kotlin.collections.ArrayList

fun main(args: Array<String>) {

    System.setProperty("log4j2.configurationFile", "config/log4j2.xml")

    val logger: Logger = LogManager.getLogger()
    val hostname = InetAddress.getLocalHost().hostName
    //Strip suffix
    val me = if (hostname.indexOf('.') != -1) hostname.subSequence(0, hostname.indexOf('.')) else hostname
    logger.info("Me $me")

    val reader: Reader = Files.newBufferedReader(Paths.get("config/tree.json"))
    val tree = Gson().fromJson(reader, TreeFile::class.java)
    reader.close()

    val myInfo = tree.nodes[me] ?: throw AssertionError("Am not part of tree")
    val myNeighs = tree.links.filter { it.contains(me) }.map { it.first { p -> p != me } }.toCollection(ArrayList())

    val partitionTargets: MutableMap<String, MutableList<String>> = mutableMapOf()
    if(myInfo.local_db) {
        for (partition in myInfo.partitions!!) {
            partitionTargets[partition] = arrayListOf()
            tree.nodes.filter { (h, _) -> h != hostname && h != me }.forEach { (h, pI) ->
                if (pI.local_db && pI.partitions!!.contains(partition))
                    partitionTargets[partition]!!.add(h)
            }
        }
        logger.info(partitionTargets)
    }

    val all = tree.nodes.map { n -> n.key }.toSet()

    val props = Babel.loadConfig(args, "config/properties.conf")
    addInterfaceIp(props)

    val babel = Babel.getInstance()

    //logger.info(tree)

    val engage = Engage(props, myInfo, myNeighs, partitionTargets, all)
    babel.registerProtocol(engage)
    engage.init(null)

    babel.start()

    Runtime.getRuntime().addShutdownHook(Thread { logger.info("Goodbye") })
}

fun getIpOfInterface(interfaceName: String?): String? {
    val inetAddress = NetworkInterface.getByName(interfaceName).inetAddresses
    var currentAddress: InetAddress
    while (inetAddress.hasMoreElements()) {
        currentAddress = inetAddress.nextElement()
        if (currentAddress is Inet4Address && !currentAddress.isLoopbackAddress())
            return currentAddress.getHostAddress()
    }
    return null
}

fun addInterfaceIp(props: Properties) {
    var interfaceName: String
    if (props.getProperty("interface").also { interfaceName = it } != null) {
        val ip = getIpOfInterface(interfaceName)
        if (ip != null) props.setProperty("address", ip)
        else throw InvalidParameterException("Property interface is set to $interfaceName, but has no ip")
    }
}