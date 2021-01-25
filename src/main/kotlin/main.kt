import com.google.gson.Gson
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import pt.unl.fct.di.novasys.babel.core.Babel
import java.io.Reader
import java.net.Inet4Address
import java.net.InetAddress
import java.net.NetworkInterface
import java.net.SocketException
import java.nio.file.Files
import java.nio.file.Paths
import java.security.InvalidParameterException
import java.util.*
import kotlin.collections.ArrayList

private val logger: Logger = LogManager.getLogger()

fun main(args: Array<String>) {

    val reader: Reader = Files.newBufferedReader(Paths.get("config/tree.json"))
    val tree = Gson().fromJson(reader, TreeFile::class.java)
    reader.close()

    val me = InetAddress.getLocalHost().hostName
    logger.info("Me $me")
    val myInfo = tree.nodes[me] ?: throw AssertionError("Am not part of tree")

    val collect = tree.links.filter { it.contains(me) }.map { it.first { p -> p != me } }.toCollection(ArrayList())

    val props = Babel.loadConfig(args, "config/properties.conf")
    addInterfaceIp(props)

    val babel = Babel.getInstance()

    logger.info(tree)

    val engage = Engage(props, myInfo, collect)
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