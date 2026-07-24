package com.calmcut.studio.import

import kotlinx.coroutines.flow.Flow

/**
 * Collects [this] flow into chunks of [size], invoking [onChunk] with each chunk
 * and the cumulative count of emitted (post-skip) items. The first [skip] items
 * are dropped so an import can resume from a checkpoint. Collection stops early
 * as soon as [onChunk] returns false (used for cooperative cancellation).
 *
 * Memory stays bounded to one chunk regardless of the total number of items,
 * which is what makes million-segment streaming imports feasible.
 */
suspend fun <T> Flow<T>.collectChunked(
    size: Int,
    skip: Long = 0,
    onChunk: suspend (batch: List<T>, consumed: Long) -> Boolean,
) {
    val buffer = ArrayList<T>(size)
    var seen = 0L        // total items observed from the source
    var consumed = skip  // total items forwarded to onChunk (incl. skipped)
    var active = true

    collectWhile { item ->
        seen++
        if (seen <= skip) return@collectWhile true // resume: drop already-applied
        buffer.add(item)
        if (buffer.size >= size) {
            consumed += buffer.size
            active = onChunk(buffer.toList(), consumed)
            buffer.clear()
        }
        active
    }
    if (active && buffer.isNotEmpty()) {
        consumed += buffer.size
        onChunk(buffer.toList(), consumed)
    }
}

/** Collects until [predicate] returns false. */
private suspend fun <T> Flow<T>.collectWhile(predicate: suspend (T) -> Boolean) {
    try {
        collect { value ->
            if (!predicate(value)) throw StopCollect
        }
    } catch (_: StopCollect) {
        // normal early termination
    }
}

private object StopCollect : RuntimeException() {
    private fun readResolve(): Any = StopCollect
    override fun fillInStackTrace(): Throwable = this
}
