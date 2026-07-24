package com.calmcut.studio.worker

import com.calmcut.studio.domain.StoryboardEvent

/**
 * Buffers out-of-order events per storyboard and releases them strictly in
 * version order ("支持乱序版本缓冲"). Events are expected to advance the version
 * by exactly one; a gap parks later events until the missing version arrives.
 * Events whose version is at or below the last processed version are stale and
 * dropped.
 */
class VersionBuffer(private val startVersion: Long = 0) {

    sealed interface BufferResult {
        /** Events ready to process, in ascending version order. */
        data class Ready(val events: List<StoryboardEvent>) : BufferResult
        /** Event buffered awaiting an earlier version. */
        data object Buffered : BufferResult
        /** Event dropped because its version was already processed. */
        data object Stale : BufferResult
    }

    private var lastReleased = startVersion
    private val pending = sortedMapOf<Long, StoryboardEvent>()

    val expectedNext: Long get() = lastReleased + 1

    /** Offer one event; returns the contiguous run now releasable (possibly empty). */
    fun offer(event: StoryboardEvent): BufferResult {
        if (event.version <= lastReleased) return BufferResult.Stale
        if (pending.containsKey(event.version)) return BufferResult.Buffered
        pending[event.version] = event

        if (event.version != lastReleased + 1 && !pending.containsKey(lastReleased + 1)) {
            return BufferResult.Buffered
        }
        val released = ArrayList<StoryboardEvent>()
        var next = lastReleased + 1
        while (pending.containsKey(next)) {
            released += pending.remove(next)!!
            lastReleased = next
            next++
        }
        return if (released.isEmpty()) BufferResult.Buffered else BufferResult.Ready(released)
    }

    /** Fast-forward the buffer after processed versions are confirmed elsewhere. */
    fun advanceTo(version: Long) {
        if (version > lastReleased) {
            lastReleased = version
            val it = pending.keys.iterator()
            while (it.hasNext()) if (it.next() <= lastReleased) it.remove()
        }
    }

    val bufferedVersions: Set<Long> get() = pending.keys.toSet()
}
