package com.calmcut.studio.import

import com.calmcut.studio.config.AppConfig
import com.calmcut.studio.domain.model.ImportState
import com.calmcut.studio.event.BatchImported
import com.calmcut.studio.event.EventStore
import com.calmcut.studio.testutil.TestFixtures
import io.mockk.mockk
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class StreamingImportTest {

    private lateinit var eventStore: EventStore
    private lateinit var config: AppConfig

    @BeforeEach
    fun setup() {
        config = AppConfig(
            db = AppConfig.DbConfig("jdbc:h2:mem:test", "sa", "", 5),
            kafka = AppConfig.KafkaConfig(
                "localhost:9092", "test", "events", "results", "dlq", "all", "earliest"
            ),
            analysis = TestFixtures.defaultAnalysisConfig(),
            import = AppConfig.ImportConfig(chunkSize = 10, maxConcurrentChunks = 4),
            outbox = AppConfig.OutboxConfig(100, 100)
        )
        eventStore = mockk(relaxed = true)
    }

    @Test
    fun `import state progress calculation`() {
        val state = ImportState(
            jobId = "job-1", timelineId = "tl-1",
            totalCount = 100, processedCount = 50, failedCount = 0,
            status = ImportState.Status.IN_PROGRESS, cancelRequested = false
        )
        assertEquals(0.5, state.progress)
        assertFalse(state.isTerminal)
    }

    @Test
    fun `zero total count gives zero progress`() {
        val state = ImportState(
            jobId = "job-1", timelineId = "tl-1",
            totalCount = 0, processedCount = 0, failedCount = 0,
            status = ImportState.Status.PENDING, cancelRequested = false
        )
        assertEquals(0.0, state.progress)
    }

    @Test
    fun `terminal states are detected correctly`() {
        assertTrue(ImportState("j", "t", 10, 10, 0, ImportState.Status.COMPLETED, false).isTerminal)
        assertTrue(ImportState("j", "t", 10, 5, 0, ImportState.Status.CANCELLED, true).isTerminal)
        assertTrue(ImportState("j", "t", 10, 3, 1, ImportState.Status.FAILED, false).isTerminal)
        assertFalse(ImportState("j", "t", 10, 3, 0, ImportState.Status.PENDING, false).isTerminal)
        assertFalse(ImportState("j", "t", 10, 3, 0, ImportState.Status.IN_PROGRESS, false).isTerminal)
    }

    @Test
    fun `batch import event carries correct mode and segments`() {
        val segments = TestFixtures.continuousSegments(5)
        val event = BatchImported(
            timelineId = "tl-1", version = 1, segments = segments,
            mode = BatchImported.ImportMode.APPEND
        )
        assertEquals(BatchImported.ImportMode.APPEND, event.mode)
        assertEquals(5, event.segments.size)
        assertEquals("BATCH_IMPORTED", event.eventType)
        assertEquals(1L, event.version)
    }

    @Test
    fun `batch import replace mode`() {
        val segments = TestFixtures.continuousSegments(3)
        val event = BatchImported(
            timelineId = "tl-1", version = 2, segments = segments,
            mode = BatchImported.ImportMode.REPLACE
        )
        assertEquals(BatchImported.ImportMode.REPLACE, event.mode)
    }

    @Test
    fun `import config has correct chunk size`() {
        assertEquals(10, config.import.chunkSize)
        assertEquals(4, config.import.maxConcurrentChunks)
    }
}
