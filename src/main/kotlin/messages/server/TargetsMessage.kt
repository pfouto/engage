package messages.server

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer

class TargetsMessage(val myName: String, val targets: Map<String, List<String>>, val all: Map<String, String>, val bayouStabMs: Int) :
    ProtoMessage(MSG_ID) {

    companion object {
        const val MSG_ID: Short = 201

        val serializer = object : ISerializer<TargetsMessage> {

            override fun serialize(msg: TargetsMessage, out: ByteBuf) {
                utils.serializeString(msg.myName, out)
                out.writeInt(msg.targets.size)
                msg.targets.forEach {
                    utils.serializeString(it.key, out)
                    out.writeInt(it.value.size)
                    it.value.forEach { h -> utils.serializeString(h, out) }
                }
                out.writeInt(msg.all.size)
                msg.all.forEach { (k, v) -> utils.serializeString(k, out); utils.serializeString(v, out) }
                out.writeInt(msg.bayouStabMs);
            }

            override fun deserialize(input: ByteBuf): TargetsMessage {
                val myName = utils.deserializeString(input)
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
                val all: MutableMap<String,String> = mutableMapOf()
                for (i in 0 until nAll){
                    val key = utils.deserializeString(input);
                    val value = utils.deserializeString(input);
                    all[key] = value
                }
                val bayouStabMs = input.readInt()
                return TargetsMessage(myName, map, all, bayouStabMs)
            }
        }
    }

    override fun toString(): String {
        return "TargetsMessage(myName=$myName, targets=$targets, all=$all, bayouStabMs=$bayouStabMs)"
    }


}