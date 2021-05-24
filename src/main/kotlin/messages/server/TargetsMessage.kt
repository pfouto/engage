package messages.server

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer

class TargetsMessage(val targets: Map<String, List<String>>, val all: Set<String>) : ProtoMessage(MSG_ID) {

    companion object {
        const val MSG_ID: Short = 201

        val serializer = object : ISerializer<TargetsMessage> {

            override fun serialize(msg: TargetsMessage, out: ByteBuf) {
                out.writeInt(msg.targets.size)
                msg.targets.forEach {
                    utils.serializeString(it.key, out)
                    out.writeInt(it.value.size)
                    it.value.forEach { h -> utils.serializeString(h, out) }
                }
                out.writeInt(msg.all.size)
                msg.all.forEach { utils.serializeString(it, out) }
            }

            override fun deserialize(input: ByteBuf): TargetsMessage {
                val nElements = input.readInt()
                val map: MutableMap<String, MutableList<String>> = mutableMapOf()
                for (i in 0 until nElements) {
                    val key = utils.deserializeString(input)
                    val size = input.readInt()
                    map[key] = arrayListOf()
                    for (j in 0 until size)
                        map[key]!!.add(utils.deserializeString(input))
                }
                val nAll = input.readInt()
                val all: MutableSet<String> = mutableSetOf()
                for (i in 0 until nAll)
                    all.add(utils.deserializeString(input))
                return TargetsMessage(map, all)
            }
        }
    }

    override fun toString(): String {
        return "TargetsMessage(targets=$targets, all=$all)"
    }


}