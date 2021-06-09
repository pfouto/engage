package messages.mixed

import Clock
import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer
import java.net.InetAddress

class UpdateNotification(
    val source: InetAddress, val vUp: Int, val partition: String, val vectorClock: Clock,
    val data: ByteArray?, val mf: MetadataFlush?,
) : ProtoMessage(MSG_ID) {

    companion object {
        const val MSG_ID: Short = 202

        val serializer = object : ISerializer<UpdateNotification> {

            override fun serialize(msg: UpdateNotification, out: ByteBuf) {
                out.writeBytes(msg.source.address)
                out.writeInt(msg.vUp)
                utils.serializeString(msg.partition, out)
                Clock.serializer.serialize(msg.vectorClock, out)
                if (msg.data != null) {
                    out.writeInt(msg.data.size)
                    out.writeBytes(msg.data)
                } else {
                    out.writeInt(0)
                }
                if (msg.mf != null) {
                    out.writeBoolean(true)
                    MetadataFlush.serializer.serialize(msg.mf, out)
                } else {
                    out.writeBoolean(false)
                }
            }

            override fun deserialize(input: ByteBuf): UpdateNotification {
                val addrBytes = ByteArray(4)
                input.readBytes(addrBytes)
                val vUp: Int = input.readInt()
                val partition: String = utils.deserializeString(input)
                val clock = Clock.serializer.deserialize(input)
                val dataSize: Int = input.readInt()
                val data: ByteArray?
                if (dataSize > 0) {
                    data = ByteArray(dataSize)
                    input.readBytes(data)
                } else data = null
                val mf: MetadataFlush?
                val mfPresent: Boolean = input.readBoolean()
                mf = if (mfPresent) MetadataFlush.serializer.deserialize(input) else null
                return UpdateNotification(InetAddress.getByAddress(addrBytes), vUp, partition, clock, data, mf)
            }
        }
    }

    override fun toString(): String {
        return "UpdateNotification(source=$source, vUp=$vUp, partition='$partition', vectorClock=$vectorClock, data=${data?.contentToString()}, mf=$mf)"
    }

    fun copyMergingMF(newMF: MetadataFlush): UpdateNotification {
        if(mf != null) newMF.merge(mf)
        return UpdateNotification(source, vUp, partition, vectorClock, data, newMF)
    }

}