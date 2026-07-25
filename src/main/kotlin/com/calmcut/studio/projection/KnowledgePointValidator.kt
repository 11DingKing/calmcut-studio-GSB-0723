package com.calmcut.studio.projection

import com.calmcut.studio.domain.KnowledgePointSet

/** Result of validating original vs revision knowledge-point integrity. */
data class IntegrityResult(val ok: Boolean, val violations: List<String>) {
    companion object {
        val OK = IntegrityResult(true, emptyList())
    }
}

/**
 * Validates knowledge-point completeness between the original and revised
 * storyboard ("原版/修订版知识点完整性校验"):
 *  - every original knowledge point must still be represented in the revision,
 *  - every revision knowledge point must reference a known original id,
 *  - ids must be unique within each set.
 */
object KnowledgePointValidator {
    fun validate(kp: KnowledgePointSet): IntegrityResult {
        val violations = mutableListOf<String>()

        val originalIds = kp.original.map { it.id }
        val revisionIds = kp.revision.map { it.id }

        originalIds.groupingBy { it }.eachCount().filterValues { it > 1 }.keys
            .forEach { violations += "duplicate original knowledge point id '$it'" }
        revisionIds.groupingBy { it }.eachCount().filterValues { it > 1 }.keys
            .forEach { violations += "duplicate revision knowledge point id '$it'" }

        val originalSet = originalIds.toSet()
        val revisionSet = revisionIds.toSet()

        originalSet.filterNot { it in revisionSet }
            .forEach { violations += "original knowledge point '$it' missing from revision" }
        revisionSet.filterNot { it in originalSet }
            .forEach { violations += "revision knowledge point '$it' has no matching original" }

        return if (violations.isEmpty()) IntegrityResult.OK else IntegrityResult(false, violations)
    }
}
