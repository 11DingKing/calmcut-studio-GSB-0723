package com.calmcut.studio.analysis

import com.calmcut.studio.domain.model.RiskFinding

internal fun dedupeFindings(findings: List<RiskFinding>): List<RiskFinding> {
    val seen = mutableSetOf<String>()
    return findings.filter { f ->
        val key = "${f.ruleId}:${f.segmentIds.sorted().joinToString(",")}:${f.timeRangeMs?.first}-${f.timeRangeMs?.second}"
        seen.add(key)
    }
}

internal fun findingsMatchSet(
    incremental: List<RiskFinding>,
    full: List<RiskFinding>
): DriftResult {
    val incGrouped = incremental.groupBy { it.ruleId }
    val fullGrouped = full.groupBy { it.ruleId }
    val allRules = incGrouped.keys + fullGrouped.keys

    val mismatches = mutableListOf<String>()
    for (rule in allRules) {
        val incList = incGrouped[rule] ?: emptyList()
        val fullList = fullGrouped[rule] ?: emptyList()

        if (incList.size != fullList.size) {
            mismatches.add("Rule $rule: incremental has ${incList.size} findings, full has ${fullList.size}")
            continue
        }

        val incKeys = incList.map { it.segmentIds.sorted().joinToString(",") }.toSet()
        val fullKeys = fullList.map { it.segmentIds.sorted().joinToString(",") }.toSet()
        if (incKeys != fullKeys) {
            mismatches.add("Rule $rule: segment set mismatch. inc=$incKeys full=$fullKeys")
        }
    }

    return DriftResult(
        hasDrift = mismatches.isNotEmpty(),
        mismatches = mismatches,
        incrementalCount = incremental.size,
        fullCount = full.size
    )
}

data class DriftResult(
    val hasDrift: Boolean,
    val mismatches: List<String>,
    val incrementalCount: Int,
    val fullCount: Int
)
