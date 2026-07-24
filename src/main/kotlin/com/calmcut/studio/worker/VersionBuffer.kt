package com.calmcut.studio.worker

import com.calmcut.studio.event.StoryboardEvent
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.PriorityBlockingQueue

class VersionBuffer {
    private val buffers = ConcurrentHashMap<String, AggregateBuffer>()

    fun offer(event: StoryboardEvent): BufferResult {
        val buffer = buffers.getOrPut(event.aggregateId) { AggregateBuffer(event.aggregateId) }
        return buffer.offer(event)
    }

    fun getProcessedVersion(aggregateId: String): Long {
        return buffers[aggregateId]?.processedVersion ?: 0L
    }

    fun setProcessedVersion(aggregateId: String, version: Long) {
        buffers.getOrPut(aggregateId) { AggregateBuffer(aggregateId) }.processedVersion = version
    }

    fun size(aggregateId: String): Int = buffers[aggregateId]?.size() ?: 0

    fun clear(aggregateId: String) {
        buffers.remove(aggregateId)
    }

    private class AggregateBuffer(val aggregateId: String) {
        @Volatile
        var processedVersion: Long = 0L
        private val pending = PriorityBlockingQueue<StoryboardEvent>(11, compareBy { it.version })

        @Synchronized
        fun offer(event: StoryboardEvent): BufferResult {
            if (event.version <= processedVersion) {
                return BufferResult.Duplicate(event)
            }

            val existing = pending.find { it.version == event.version }
            if (existing != null) {
                return BufferResult.Duplicate(event)
            }

            pending.add(event)

            val ready = mutableListOf<StoryboardEvent>()
            while (pending.isNotEmpty() && pending.peek().version == processedVersion + 1) {
                val next = pending.poll()
                ready.add(next)
                processedVersion = next.version
            }

            return if (ready.isNotEmpty()) {
                BufferResult.Ready(ready)
            } else {
                val expected = processedVersion + 1
                BufferResult.Buffered(event, expected, event.version)
            }
        }

        @Synchronized
        fun rollback(version: Long) {
            if (version <= processedVersion) {
                processedVersion = version - 1
            }
        }

        fun size(): Int = pending.size
    }

    sealed class BufferResult {
        data class Ready(val events: List<StoryboardEvent>) : BufferResult()
        data class Buffered(val event: StoryboardEvent, val expectedVersion: Long, val receivedVersion: Long) : BufferResult()
        data class Duplicate(val event: StoryboardEvent) : BufferResult()
    }
}
