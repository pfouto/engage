package timers

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer
import pt.unl.fct.di.novasys.network.data.Host


class GossipTimer : ProtoTimer(TIMER_ID) {

    override fun clone(): ProtoTimer = this

    companion object {
        const val TIMER_ID: Short = 102
    }
}