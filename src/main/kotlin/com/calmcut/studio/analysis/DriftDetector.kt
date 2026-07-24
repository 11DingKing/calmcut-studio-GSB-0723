package com.calmcut.studio.analysis

import com.calmcut.studio.domain.Segment

/** Outcome of comparing a persisted projection against an authoritative rebuild. */
data class DriftResult(
    val drifted: Boolean,
    val details: List<String>,
) {
    companion object {
        val NONE = DriftResult(false, emptyList())
    }
}

/**
 * Detects "projection drift": divergence between the projection currently stored
 * in the read tables and the projection obtained by replaying the event log from
 * zero. Used both as a periodic self-heal check and in tests. Comparison is on
 * the canonical segment set and the canonical analysis findings.
 */
object DriftDetector {

    fun compareSegments(
        stored: Collection<Segment>,
        rebuilt: Collection<Segment>,
    ): DriftResult {
        val details = mutableListOf<String>()
        val storedById = stored.associateBy { it.id }
        val rebuiltById = rebuilt.associateBy { it.id }

        (storedById.keys - rebuiltById.keys).forEach {
            details += "segment '$it' present in projection but absent from rebuild"
        }
        (rebuiltById.keys - storedById.keys).forEach {
            details += "segment '$it' present in rebuild but absent from projection"
        }
        (storedById.keys intersect rebuiltById.keys).forEach { id ->
            if (storedById[id] != rebuiltById[id]) {
                details += "segment '$id' differs: projection=${storedById[id]} rebuild=${rebuiltById[id]}"
            }
        }
        return if (details.isEmpty()) DriftResult.NONE else DriftResult(true, details)
    }

    fun compareAnalysis(stored: AnalysisResult, rebuilt: AnalysisResult): DriftResult {
        val details = mutableListOf<String>()
        if (stored.ruleVersion != rebuilt.ruleVersion) {
            details += "rule version differs: projection=${stored.ruleVersion} rebuild=${rebuilt.ruleVersion}"
        }
        val a = stored.canonical()
        val b = rebuilt.canonical()
        if (a != b) {
            details += "analysis findings diverge: projection=${a.size} findings, rebuild=${b.size} findings"
            val extra = a - b.toSet()
            val missing = b - a.toSet()
            extra.forEach { details += "  only in projection: ${it.code}/${it.anchorSegmentId}" }
            missing.forEach { details += "  only in rebuild: ${it.code}/${it.anchorSegmentId}" }
        }
        return if (details.isEmpty()) DriftResult.NONE else DriftResult(true, details)
    }
}
