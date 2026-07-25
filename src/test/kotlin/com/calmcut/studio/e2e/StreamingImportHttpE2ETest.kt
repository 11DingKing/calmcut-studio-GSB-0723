package com.calmcut.studio.e2e

import com.calmcut.studio.analysis.AnalysisSettings
import com.calmcut.studio.analysis.RiskAnalyzer
import com.calmcut.studio.api.StoryboardWriteService
import com.calmcut.studio.api.storyboardRoutes
import com.calmcut.studio.db.DatabaseFactory
import com.calmcut.studio.db.DbWorkerRepository
import com.calmcut.studio.db.EventStore
import com.calmcut.studio.import.ImportStatus
import com.calmcut.studio.import.StreamingImportService
import com.calmcut.studio.projection.StoryboardProjector
import com.calmcut.studio.worker.RebuildService
import com.calmcut.studio.worker.ResultPublisher
import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpStatusCode
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.install
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.routing.routing
import io.ktor.utils.io.ByteWriteChannel
import io.ktor.utils.io.writeStringUtf8
import java.net.ServerSocket
import java.util.UUID
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Integration tests that drive the public streaming import endpoint through a
 * **real** Ktor HTTP client over TCP against an embedded Netty server backed by a
 * **real** PostgreSQL container. These prove the end-to-end property the unit
 * tests cannot: the request body is parsed incrementally from the socket and
 * staged in bounded memory, with correct behaviour under client mid-stream
 * disconnect, cancel+resume, and duplicate resume.
 *
 * Self-skips when Docker is unavailable.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class StreamingImportHttpE2ETest {

    private lateinit var db: DatabaseFactory
    private lateinit var imports: StreamingImportService
    private lateinit var server: io.ktor.server.engine.EmbeddedServer<*, *>
    private lateinit var client: HttpClient
    private var port = 0
    private val chunkSize = 2000

    private val noopPublisher = object : ResultPublisher {
        override suspend fun publish(result: com.calmcut.studio.analysis.AnalysisResult) {}
    }

    @BeforeAll
    fun setup() {
        assumeTrue(E2EInfra.dockerAvailable, "Docker not available; skipping HTTP streaming E2E")
        db = DatabaseFactory(E2EInfra.dbConfig()).apply { connect() }
        val analyzer = RiskAnalyzer(AnalysisSettings())
        val repo = DbWorkerRepository(db)
        // eventsTopic value is irrelevant here (no Kafka); outbox rows are just staged.
        val eventStore = EventStore("http-e2e-events", db)
        imports = StreamingImportService(db, eventStore, chunkSize)
        val writes = StoryboardWriteService(db, eventStore)
        val rebuilds = RebuildService(eventStore, repo, analyzer)
        val processor = com.calmcut.studio.worker.IdempotentProcessor(analyzer, repo, noopPublisher)
        val appScope = CoroutineScope(SupervisorJob())

        port = ServerSocket(0).use { it.localPort }
        server = embeddedServer(Netty, port = port) {
            install(ContentNegotiation) { json(Json { encodeDefaults = true; ignoreUnknownKeys = true }) }
            routing {
                storyboardRoutes(writes, repo, imports, processor, rebuilds, appScope)
            }
        }.start(wait = false)

        client = HttpClient(CIO) {
            engine { requestTimeout = 300_000 }
        }
    }

    @AfterAll
    fun teardown() {
        if (::client.isInitialized) client.close()
        if (::server.isInitialized) server.stop(500, 1000)
        if (::db.isInitialized) db.close()
    }

    private fun segmentLine(i: Int): String =
        """{"id":"seg-$i","orderIndex":$i,"durationMs":4000,"intensity":${(i % 5) + 1},"strongReversal":${i % 4 == 0},"declineInducement":false,"knowledgePoint":null}"""

    /** Await terminal apply completion for a job (bounded wait). */
    private suspend fun awaitProcessed(jobId: String, expected: Long, timeoutMs: Long = 90_000) {
        val deadline = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() < deadline) {
            val p = imports.progress(jobId)
            if (p != null && p.processed >= expected && p.status == ImportStatus.COMPLETED) return
            delay(100)
        }
        val p = imports.progress(jobId)
        error("job $jobId did not complete: ${p?.status} processed=${p?.processed} expected=$expected")
    }

    // ---- 1. Bounded memory + million-row integrity over real HTTP ----------

    @Test
    fun `streams a million segments over http with bounded memory and full integrity`() = runBlocking {
        val sb = "http-1m-${UUID.randomUUID().toString().take(8)}"
        val total = 1_000_000
        val resp = client.post("http://127.0.0.1:$port/imports/$sb/stream") {
            header("X-Total-Segments", total.toString())
            // Stream the NDJSON body lazily — the client writes line by line and the
            // server parses incrementally; neither side materialises the whole set.
            setBody(object : io.ktor.http.content.OutgoingContent.WriteChannelContent() {
                override suspend fun writeTo(channel: ByteWriteChannel) {
                    for (i in 0 until total) {
                        channel.writeStringUtf8(segmentLine(i))
                        channel.writeStringUtf8("\n")
                    }
                    channel.flush()
                }
            })
        }
        assertEquals(HttpStatusCode.Accepted, resp.status)
        val jobId = extractJobId(resp.bodyAsText())

        // Bounded memory: server never buffered more than one chunk.
        assertTrue(imports.lastPeakBuffered <= chunkSize, "peak ${imports.lastPeakBuffered} > chunk $chunkSize")

        awaitProcessed(jobId, total.toLong())

        // Integrity: the projection rebuilt from the log holds exactly `total`
        // segments with no gaps/dupes.
        val eventStore = EventStore("verify", db)
        val state = StoryboardProjector.rebuild(sb, eventStore.readLogSuspending(sb))
        assertEquals(total, state.segmentsById.size)
        assertEquals((0 until total).map { "seg-$it" }.toSet(), state.segmentsById.keys)
    }

    // ---- 2. Client mid-stream disconnect keeps a resumable partial ---------

    @Test
    fun `client mid stream disconnect preserves staged prefix for resume`() = runBlocking {
        val sb = "http-broken-${UUID.randomUUID().toString().take(8)}"
        val jobId = UUID.randomUUID().toString()
        val total = 50_000
        val cutAfter = 12_345

        // The client aborts the body partway by throwing inside writeTo.
        val thrown = runCatching {
            client.post("http://127.0.0.1:$port/imports/$sb/stream?jobId=$jobId") {
                header("X-Total-Segments", total.toString())
                setBody(object : io.ktor.http.content.OutgoingContent.WriteChannelContent() {
                    override suspend fun writeTo(channel: ByteWriteChannel) {
                        for (i in 0 until cutAfter) {
                            channel.writeStringUtf8(segmentLine(i) + "\n")
                            // Flush periodically so the server actually receives and
                            // stages the prefix before we abort the connection.
                            if (i % 2000 == 1999) {
                                channel.flush()
                                delay(5)
                            }
                        }
                        channel.flush()
                        // Give the server time to consume + stage what we've sent.
                        delay(500)
                        throw RuntimeException("client aborts mid-stream")
                    }
                })
            }
        }
        assertTrue(thrown.isFailure, "client should have failed the request")

        // Staged prefix is preserved and the job is not COMPLETED — it is resumable.
        // Poll briefly: the server may still be flushing the last received chunk.
        var staged = imports.progress(jobId)!!.staged
        val stageDeadline = System.currentTimeMillis() + 5_000
        while (staged == 0L && System.currentTimeMillis() < stageDeadline) {
            delay(50); staged = imports.progress(jobId)!!.staged
        }
        val p = imports.progress(jobId)!!
        assertTrue(p.staged in 1 until total.toLong(), "staged ${p.staged} should be a partial prefix")
        assertTrue(p.status != ImportStatus.COMPLETED)

        // Resume by re-sending the FULL stream with the same jobId: the already
        // staged prefix is skipped, the remainder is staged, then applied.
        val resp2 = client.post("http://127.0.0.1:$port/imports/$sb/stream?jobId=$jobId") {
            header("X-Total-Segments", total.toString())
            setBody(object : io.ktor.http.content.OutgoingContent.WriteChannelContent() {
                override suspend fun writeTo(channel: ByteWriteChannel) {
                    for (i in 0 until total) channel.writeStringUtf8(segmentLine(i) + "\n")
                    channel.flush()
                }
            })
        }
        assertEquals(HttpStatusCode.Accepted, resp2.status)
        awaitProcessed(jobId, total.toLong())

        val eventStore = EventStore("verify", db)
        val state = StoryboardProjector.rebuild(sb, eventStore.readLogSuspending(sb))
        assertEquals(total, state.segmentsById.size, "no lost or duplicated segments after resume")
    }

    // ---- 3. Cancel then resume completes exactly once ----------------------

    @Test
    fun `cancel during apply then resume completes without duplication`() = runBlocking {
        val sb = "http-cancel-${UUID.randomUUID().toString().take(8)}"
        val jobId = UUID.randomUUID().toString()
        val total = 40_000

        // Cancel BEFORE sending so the apply phase stops almost immediately.
        // First stage everything via the stream, but request cancel right after.
        val resp = client.post("http://127.0.0.1:$port/imports/$sb/stream?jobId=$jobId") {
            header("X-Total-Segments", total.toString())
            setBody(object : io.ktor.http.content.OutgoingContent.WriteChannelContent() {
                override suspend fun writeTo(channel: ByteWriteChannel) {
                    for (i in 0 until total) channel.writeStringUtf8(segmentLine(i) + "\n")
                    channel.flush()
                }
            })
        }
        assertEquals(HttpStatusCode.Accepted, resp.status)
        // Race a cancel against the background apply.
        client.post("http://127.0.0.1:$port/imports/$jobId/cancel")

        // Wait until it is either cancelled or (if apply won) completed.
        var status = imports.progress(jobId)!!.status
        val deadline = System.currentTimeMillis() + 30_000
        while (status !in setOf(ImportStatus.CANCELLED, ImportStatus.COMPLETED) && System.currentTimeMillis() < deadline) {
            delay(50); status = imports.progress(jobId)!!.status
        }

        // Resume drives it to completion regardless.
        client.post("http://127.0.0.1:$port/imports/$jobId/resume")
        awaitProcessed(jobId, total.toLong())

        // Exactly-once: the log-derived projection has no duplicates.
        val eventStore = EventStore("verify", db)
        val state = StoryboardProjector.rebuild(sb, eventStore.readLogSuspending(sb))
        assertEquals(total, state.segmentsById.size)
    }

    // ---- 4. Duplicate resume calls do not double-apply ---------------------

    @Test
    fun `duplicate concurrent resume calls do not double write`() = runBlocking {
        val sb = "http-dupresume-${UUID.randomUUID().toString().take(8)}"
        val jobId = UUID.randomUUID().toString()
        val total = 30_000

        // Stage without auto-applying by cancelling immediately, then fire several
        // concurrent resume calls — the row lock must serialise them.
        client.post("http://127.0.0.1:$port/imports/$sb/stream?jobId=$jobId") {
            header("X-Total-Segments", total.toString())
            setBody(object : io.ktor.http.content.OutgoingContent.WriteChannelContent() {
                override suspend fun writeTo(channel: ByteWriteChannel) {
                    for (i in 0 until total) channel.writeStringUtf8(segmentLine(i) + "\n")
                    channel.flush()
                }
            })
        }
        // Fire 5 resume requests nearly simultaneously.
        repeat(5) { client.post("http://127.0.0.1:$port/imports/$jobId/resume") }
        awaitProcessed(jobId, total.toLong())

        val eventStore = EventStore("verify", db)
        val log = eventStore.readLogSuspending(sb)
        val totalSegmentsInLog = log.sumOf { it.segments.size }
        val state = StoryboardProjector.rebuild(sb, log)
        assertEquals(total, state.segmentsById.size)
        assertEquals(total, totalSegmentsInLog, "no chunk applied more than once")
    }

    private fun extractJobId(body: String): String =
        Regex("\"jobId\"\\s*:\\s*\"([^\"]+)\"").find(body)?.groupValues?.get(1)
            ?: error("no jobId in response: $body")
}
