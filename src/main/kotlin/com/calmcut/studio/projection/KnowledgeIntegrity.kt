package com.calmcut.studio.projection

import com.calmcut.studio.db.DatabaseFactory
import com.calmcut.studio.domain.model.StoryboardSegment
import com.calmcut.studio.event.KnowledgeIntegrityTable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import java.time.Instant

class KnowledgeIntegrityChecker {

    private val json = Json { ignoreUnknownKeys = true; encodeDefaults = true }

    fun verify(segments: List<StoryboardSegment>): List<IntegrityResult> {
        val byKp = segments.filter { it.knowledgePointId != null }
            .groupBy { it.knowledgePointId!! }

        return byKp.map { (kpId, kpSegments) ->
            val original = kpSegments.filter { it.isOriginal }.map { it.id }.sorted()
            val revised = kpSegments.filter { !it.isOriginal }.map { it.id }.sorted()
            val intact = original.toSet() == revised.toSet() || original.size == revised.size
            val details = buildString {
                append("Original segments: ${original.size}, Revised segments: ${revised.size}")
                if (!intact) append("; MISMATCH detected")
            }
            IntegrityResult(kpId, original, revised, intact, details)
        }
    }

    suspend fun saveResults(timelineId: String, results: List<IntegrityResult>) = DatabaseFactory.dbQuery {
        KnowledgeIntegrityTable.deleteWhere { KnowledgeIntegrityTable.timelineId eq timelineId }
        results.forEach { r ->
            KnowledgeIntegrityTable.insert {
                it[KnowledgeIntegrityTable.timelineId] = timelineId
                it[kpId] = r.kpId
                it[originalSegments] = json.encodeToString(r.originalSegments)
                it[revisedSegments] = json.encodeToString(r.revisedSegments)
                it[intact] = r.intact
                it[details] = r.details
                it[checkedAt] = Instant.now()
            }
        }
    }

    data class IntegrityResult(
        val kpId: String,
        val originalSegments: List<String>,
        val revisedSegments: List<String>,
        val intact: Boolean,
        val details: String
    )
}
