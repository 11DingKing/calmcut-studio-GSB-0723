package com.calmcut.studio.api

import com.calmcut.studio.import.ImportStatus
import com.calmcut.studio.import.StreamingImportService
import com.calmcut.studio.import.TruncatedStreamException
import com.calmcut.studio.worker.IdempotentProcessor
import com.calmcut.studio.worker.RebuildService
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.call
import io.ktor.server.request.receive
import io.ktor.server.request.receiveChannel
import io.ktor.server.response.respond
import io.ktor.server.routing.Route
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.route
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch

/**
 * HTTP surface. Write endpoints append versioned events; read endpoints serve
 * the latest analysis projection. Import endpoints drive the streaming importer
 * (durable staging + cancel/resume). Recovery endpoints expose DLQ replay and
 * projection rebuild, both auditable. All analysis output is descriptive of
 * editing structure only and never asserts clinical/medical conclusions.
 */
fun Route.storyboardRoutes(
    writes: StoryboardWriteService,
    repo: com.calmcut.studio.worker.WorkerRepository,
    imports: StreamingImportService,
    processor: IdempotentProcessor,
    rebuilds: RebuildService,
    appScope: CoroutineScope,
) {
    route("/storyboards") {

        post("/batch-import") {
            val req = call.receive<BatchImportRequest>()
            val event = writes.batchImport(req)
            call.respond(HttpStatusCode.Accepted, EventAckResponse(event.eventId, event.storyboardId, event.version))
        }

        post("/segments") {
            val req = call.receive<CreateSegmentRequest>()
            val event = writes.createSegment(req)
            call.respond(HttpStatusCode.Accepted, EventAckResponse(event.eventId, event.storyboardId, event.version))
        }

        post("/segments/update") {
            val req = call.receive<UpdateSegmentRequest>()
            val event = writes.updateSegment(req)
            call.respond(HttpStatusCode.Accepted, EventAckResponse(event.eventId, event.storyboardId, event.version))
        }

        post("/segments/delete") {
            val req = call.receive<DeleteSegmentRequest>()
            val event = writes.deleteSegment(req)
            call.respond(HttpStatusCode.Accepted, EventAckResponse(event.eventId, event.storyboardId, event.version))
        }

        get("/{id}/analysis") {
            val id = call.parameters["id"]!!
            val result = repo.loadAnalysis(id)
                ?: return@get call.respond(HttpStatusCode.NotFound, ErrorResponse("no analysis for storyboard $id"))
            call.respond(AnalysisResponse.from(result))
        }

        // Rebuild the projection from zero by replaying the event log (auditable).
        post("/{id}/rebuild") {
            val id = call.parameters["id"]!!
            val result = rebuilds.rebuild(id)
            call.respond(
                mapOf(
                    "storyboardId" to result.storyboardId,
                    "version" to result.version.toString(),
                    "eventsReplayed" to result.eventsReplayed.toString(),
                    "findings" to result.analysis.findings.size.toString(),
                )
            )
        }

        get("/{id}/audit") {
            val id = call.parameters["id"]!!
            call.respond(processor.auditTrail(id).map(::auditToMap))
        }
    }

    route("/imports") {
        // Streaming NDJSON ingest: one JSON-encoded Segment per line. The request
        // body is parsed incrementally straight from the socket and staged to
        // PostgreSQL in bounded-memory chunks — resident memory is O(chunkSize),
        // never O(total). The apply phase is launched after the body is fully
        // staged. Supply ?jobId=<id> to resume/retry the same upload idempotently;
        // an X-Total-Segments header enables truncation detection.
        post("/{storyboardId}/stream") {
            val storyboardId = call.parameters["storyboardId"]!!
            val requestedJobId = call.request.queryParameters["jobId"]
            val expectedTotal = call.request.headers["X-Total-Segments"]?.toLongOrNull()

            val jobId = imports.ensureJob(requestedJobId, storyboardId)
            val channel = call.receiveChannel()
            try {
                val result = imports.ingestNdjson(jobId, channel, expectedTotal)
                // Kick off the durable apply phase; it is exactly-once and resumable.
                appScope.launch { imports.run(jobId) }
                val p = imports.progress(jobId)!!
                call.respond(
                    HttpStatusCode.Accepted,
                    ImportJobResponse(p.jobId, p.storyboardId, p.status.name, p.total, p.processed, result.staged),
                )
            } catch (e: TruncatedStreamException) {
                // Partial upload: staged rows are preserved for a later resume.
                val p = imports.progress(jobId)!!
                call.respond(
                    HttpStatusCode.UnprocessableEntity,
                    ImportJobResponse(p.jobId, p.storyboardId, p.status.name, p.total, p.processed, e.staged),
                )
            } catch (e: Exception) {
                // Broken stream (client disconnect): keep staged rows; report partial.
                val p = imports.progress(jobId)
                if (p != null) {
                    call.respond(
                        HttpStatusCode.UnprocessableEntity,
                        ImportJobResponse(p.jobId, p.storyboardId, p.status.name, p.total, p.processed, p.staged),
                    )
                } else {
                    call.respond(HttpStatusCode.BadRequest, ErrorResponse("ingest failed: ${e.message}"))
                }
            }
        }

        get("/{jobId}") {
            val jobId = call.parameters["jobId"]!!
            val p = imports.progress(jobId)
                ?: return@get call.respond(HttpStatusCode.NotFound, ErrorResponse("no import job $jobId"))
            call.respond(ImportJobResponse(p.jobId, p.storyboardId, p.status.name, p.total, p.processed, p.staged))
        }

        post("/{jobId}/cancel") {
            val jobId = call.parameters["jobId"]!!
            val ok = imports.requestCancel(jobId)
            if (ok) call.respond(HttpStatusCode.Accepted, mapOf("cancelRequested" to true))
            else call.respond(HttpStatusCode.NotFound, ErrorResponse("no import job $jobId"))
        }

        // Actually resume the job from the DB checkpoint against the staged source.
        post("/{jobId}/resume") {
            val jobId = call.parameters["jobId"]!!
            val current = imports.progress(jobId)
                ?: return@post call.respond(HttpStatusCode.NotFound, ErrorResponse("no import job $jobId"))
            if (current.status == ImportStatus.COMPLETED) {
                return@post call.respond(HttpStatusCode.Conflict, ErrorResponse("job $jobId already completed"))
            }
            appScope.launch { imports.resume(jobId) }
            call.respond(HttpStatusCode.Accepted, ImportJobResponse(current.jobId, current.storyboardId, "RESUMING", current.total, current.processed, current.staged))
        }
    }

    route("/dlq") {
        get {
            call.respond(processor.deadLetters().map {
                mapOf(
                    "eventId" to it.event.eventId,
                    "storyboardId" to it.event.storyboardId,
                    "version" to it.event.version.toString(),
                    "error" to it.error,
                    "attempts" to it.attempts.toString(),
                    "replayed" to it.replayed.toString(),
                    "replayedAt" to (it.replayedAt ?: ""),
                )
            })
        }

        // Replay a dead-lettered event through the normal pipeline (auditable).
        post("/{eventId}/replay") {
            val eventId = call.parameters["eventId"]!!
            val results = processor.replayDeadLetter(eventId)
            if (results.isEmpty()) {
                call.respond(HttpStatusCode.UnprocessableEntity, ErrorResponse("replay produced no result for $eventId"))
            } else {
                call.respond(mapOf("eventId" to eventId, "replayedVersions" to results.map { it.version.toString() }))
            }
        }

        get("/{eventId}/audit") {
            val eventId = call.parameters["eventId"]!!
            call.respond(processor.auditTrail(eventId).map(::auditToMap))
        }
    }
}

private fun auditToMap(a: com.calmcut.studio.worker.AuditEntry): Map<String, String> = mapOf(
    "kind" to a.kind,
    "target" to a.target,
    "detail" to a.detail,
    "outcome" to a.outcome,
    "createdAt" to a.createdAt,
)
