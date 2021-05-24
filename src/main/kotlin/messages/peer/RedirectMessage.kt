package messages.peer

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer

class RedirectMessage(val partition: String, val data: ByteArray) : ProtoMessage(MSG_ID) {

    companion object {
        const val MSG_ID: Short = 103

        val serializer = object : ISerializer<RedirectMessage> {

            override fun serialize(msg: RedirectMessage, out: ByteBuf) {
                utils.serializeString(msg.partition, out)
                out.writeInt(msg.data.size)
                out.writeBytes(msg.data)
            }

            override fun deserialize(input: ByteBuf): RedirectMessage {
                val partition = utils.deserializeString(input)
                val arrayLength = input.readInt()
                val data = ByteArray(arrayLength)
                input.readBytes(data)
                return RedirectMessage(partition, data)
            }
        }
    }

    override fun toString(): String {
        return "PropagateMessage(partition='$partition', data=${data.contentToString()})"
    }

}