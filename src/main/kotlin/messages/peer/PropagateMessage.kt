package messages.peer

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer

class PropagateMessage(val partition: String, val data: ByteArray) : ProtoMessage(MSG_ID) {

    companion object {
        const val MSG_ID: Short = 102

        val serializer = object : ISerializer<PropagateMessage> {

            override fun serialize(msg: PropagateMessage, out: ByteBuf) {
                utils.serializeString(msg.partition, out)
                out.writeInt(msg.data.size)
                out.writeBytes(msg.data)
            }

            override fun deserialize(input: ByteBuf): PropagateMessage {
                val partition = utils.deserializeString(input)
                val arrayLength = input.readInt()
                val data = ByteArray(arrayLength)
                input.readBytes(data)
                return PropagateMessage(partition, data)
            }
        }
    }

    override fun toString(): String {
        return "PropagateMessage(partition='$partition', data=${data.contentToString()})"
    }

}