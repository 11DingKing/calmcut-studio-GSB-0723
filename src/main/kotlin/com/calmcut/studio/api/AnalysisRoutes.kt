package com.calmcut.studio.api

import com.calmcut.studio.projection.DriftDetector
import com.calmcut.studio.projection.ProjectionRebuilder
import com.calmcut.studio.projection.ProjectionRepository
import io.ktor.http.*
import io.ktor.server.response.*
import io.ktor.server.routing.*

class AnalysisRoutes(
    private val repository: ProjectionRepository,
    private val driftDetector: DriftDetector,
    private val rebuilder: ProjectionRebuilder
) {
    fun register(routing: Routing) {
        with(routing) {
            get("/api/timelines/{timelineId}/analysis") {
                val timelineId = call.parameters["timelineId"]!!
                val findings = repository.loadFindings(timelineId)
                val state = repository.loadTimeline(timelineId)

                if (state == null) {
                    call.respond(HttpStatusCode.NotFound, ErrorResponse("not_found", "Timeline not found"))
                    return@get
                }

                call.respond(
                    AnalysisResponse(
                        timelineId = timelineId,
                        ruleVersion = "1.0.0",
                        analysisVersion = state.version,
                        findings = findings.map { f ->
                            RiskFindingDto(
                                findingId = f.findingId,
                                ruleId = f.ruleId,
                                ruleVersion = f.ruleVersion,
                                severity = f.severity.name,
                                segmentIds = f.segmentIds,
                                timeRangeStartMs = f.timeRangeMs?.first,
                                timeRangeEndMs = f.timeRangeMs?.second,
                                evidence = f.evidence,
                                suggestion = f.suggestion
                            )
                        },
                        isIncremental = true,
                        driftCheckPassed = null
                    )
                )
            }

            post("/api/timelines/{timelineId}/analysis/recompute") {
                val timelineId = call.parameters["timelineId"]!!
                val state = repository.loadTimeline(timelineId)
                if (state == null) {
                    call.respond(HttpStatusCode.NotFound, ErrorResponse("not_found", "Timeline not found"))
                    return@post
                }
                val drift = driftDetector.repairIfDrifted(timelineId)
                call.respond(
                    mapOf(
                        "timelineId" to timelineId,
                        "driftDetected" to drift.hasDrift,
                        "mismatches" to drift.mismatches,
                        "incrementalCount" to drift.incrementalCount,
                        "fullCount" to drift.fullCount
                    )
                )
            }

            post("/api/admin/rebuild/{timelineId}") {
                val timelineId = call.parameters["timelineId"]!!
                val result = rebuilder.rebuildFromScratch(timelineId)
                call.respond(
                    mapOf(
                        "timelineId" to result.timelineId,
                        "eventsProcessed" to result.eventsProcessed,
                        "finalVersion" to result.finalVersion,
                        "drift" to result.drift?.hasDrift
                    )
                )
            }

            post("/api/admin/rebuild-all") {
                val results = rebuilder.rebuildAll()
                call.respond(
                    mapOf(
                        "timelines" to results.map { r ->
                            mapOf(
                                "timelineId" to r.timelineId,
                                "eventsProcessed" to r.eventsProcessed,
                                "drift" to (r.drift?.hasDrift ?: false)
                            )
                        }
                    )
                )
            }

            get("/api/admin/drift/{timelineId}") {
                val timelineId = call.parameters["timelineId"]!!
                val drift = driftDetector.checkTimeline(timelineId)
                call.respond(
                    mapOf(
                        "timelineId" to timelineId,
                        "hasDrift" to drift.hasDrift,
                        "mismatches" to drift.mismatches
                    )
                )
            }
        }
    }
}
