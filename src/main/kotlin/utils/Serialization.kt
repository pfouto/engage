package utils

import io.netty.buffer.ByteBuf

fun serializeString(s: String, byteBuf: ByteBuf) {
    val byteArray = s.toByteArray(Charsets.UTF_8)
    byteBuf.writeInt(byteArray.size)
    byteBuf.writeBytes(byteArray)
}

fun deserializeString(byteBuf: ByteBuf): String {
    val size = byteBuf.readInt()
    val stringBytes = ByteArray(size)
    byteBuf.readBytes(stringBytes)
    return String(stringBytes, Charsets.UTF_8)
}