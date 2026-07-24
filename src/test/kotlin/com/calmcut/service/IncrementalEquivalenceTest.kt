package com.calmcut.service

import com.calmcut.domain.*
import com.calmcut.domain.events.*
import com.calmcut.domain.rules.*
import com.calmcut.infrastructure.messaging.BufferedEvent
import com.calmcut.infrastructure.messaging.OutOfOrderEventBuffer
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import java.util.UUID

class IncrementalEquivalenceTest {

    private lateinit var config: RiskRuleConfig
    private lateinit var rules: List<RiskRule>

    @BeforeEach
    fun setup() {
        config = RiskRuleConfig()
        rules = RiskRules.allRules(config)
    }

    @Test
    fun `incremental analysis should match full analysis after single segment addition`() {
        val sid = StoryboardId.generate()
        val baseSegments = generateMixedSegments(sid, 30)

        val fullFindings = rules.flatMap { it.evaluate(baseSegments.sortedBy { s -> s.startTimeMs }) }

        val newSegment = StoryboardSegment(
            id = SegmentId.generate(),
            storyboardId = sid,
            order = 30,
            startTimeMs = 30 * 3000L,
            endTimeMs = 31 * 3000L,
            stimulusIntensity = 5,
            hasReversal = true
        )
        val afterAdd = (baseSegments + newSegment).sortedBy { it.startTimeMs }
        val fullFindingsAfter = rules.flatMap { it.evaluate(afterAdd) }

        val affectedRange = (newSegment.startTimeMs - config.windowSizeMs)..(newSegment.endTimeMs + config.windowSizeMs)
        val incrementalFindings = rules.flatMap { rule ->
            val incremental = rule.evaluate(afterAdd, affectedRange)
            incremental
        }

        val unaffectedFull = fullFindings.filter { f ->
            f.windowStartMs == null || f.windowEndMs == null ||
            f.windowEndMs < affectedRange.start || f.windowStartMs > affectedRange.endInclusive
        }

        val combined = (unaffectedFull + incrementalFindings)
            .distinctBy { Triple(it.ruleId, it.windowStartMs, it.evidence.description.hashCode()) }

        val fullKeySet = fullFindingsAfter.map { "${it.ruleId}:${it.windowStartMs}:${it.evidence.description.hashCode()}" }.toSet()
        val combinedKeySet = combined.map { "${it.ruleId}:${it.windowStartMs}:${it.evidence.description.hashCode()}" }.toSet()

        val missing = fullKeySet - combinedKeySet
        val extra = combinedKeySet - fullKeySet

        assertTrue(missing.isEmpty(), "Incremental misses these findings: $missing")
        assertTrue(extra.isEmpty(), "Incremental has extra findings: $extra")
    }

    @Test
    fun `incremental analysis should match full analysis after segment intensity change`() {
        val sid = StoryboardId.generate()
        val baseSegments = generateMixedSegments(sid, 40)
        val sorted = baseSegments.sortedBy { it.startTimeMs }

        val changedIdx = 15
        val original = sorted[changedIdx]
        val modified = original.copy(
            stimulusIntensity = 5,
            hasReversal = true,
            version = original.version + 1
        )
        val afterChange = sorted.toMutableList().apply { this[changedIdx] = modified }

        val fullFindings = rules.flatMap { it.evaluate(afterChange) }

        val affectedRange = (modified.startTimeMs - config.windowSizeMs)..(modified.endTimeMs + config.windowSizeMs)
        val incrementalFindings = rules.flatMap { it.evaluate(afterChange, affectedRange) }

        val fullFindingsBefore = rules.flatMap { it.evaluate(sorted) }
        val unaffectedBefore = fullFindingsBefore.filter { f ->
            f.windowStartMs == null || f.windowEndMs == null ||
            f.windowEndMs < affectedRange.start - config.windowSizeMs ||
            f.windowStartMs > affectedRange.endInclusive + config.windowSizeMs
        }

        val combined = (unaffectedBefore + incrementalFindings)
            .distinctBy { Triple(it.ruleId, it.windowStartMs, it.evidence.description.hashCode()) }

        val fullKeySet = fullFindings.map { "${it.ruleId}:${it.windowStartMs}:${it.evidence.description.hashCode()}" }.toSet()
        val combinedKeySet = combined.map { "${it.ruleId}:${it.windowStartMs}:${it.evidence.description.hashCode()}" }.toSet()

        val missing = fullKeySet - combinedKeySet
        assertTrue(missing.isEmpty(), "After intensity change, incremental misses: $missing")
    }

    @Test
    fun `incremental analysis should match full analysis for scroll inducement additions`() {
        val sid = StoryboardId.generate()
        val segments = (0 until 20).map { i ->
            makeTestSegment(sid, i, i * 3000L, (i + 1) * 3000L, intensity = 2)
        }.toMutableList()

        val scrollSeg = makeTestSegment(sid, 20, 20 * 3000L, 21 * 3000L, intensity = 2, hasScroll = true)
        segments.add(scrollSeg)

        val fullFindings = rules.flatMap { it.evaluate(segments.sortedBy { it.startTimeMs }) }
        val scrollFindings = fullFindings.filter { it.ruleId == RuleId.SCROLL_INDUCEMENT_PRESENT }

        assertEquals(1, scrollFindings.size)
        scrollFindings.forEach { finding ->
            assertEquals(RiskSeverity.LOW, finding.severity)
            assertTrue(finding.suggestion.isNotBlank())
        }
    }

    @Test
    fun `incremental analysis affected windows should cover sliding window properly`() {
        val changeStart = 60000L
        val changeEnd = 63000L
        val totalDuration = 180000L

        val rule = ExcessiveReversalsRule(config)
        val windows = rule.affectedWindowsByChange(changeStart, changeEnd, totalDuration)

        assertTrue(windows.isNotEmpty(), "Should produce affected windows")
        windows.forEach { window ->
            assertTrue(window.endInclusive - window.start <= config.windowSizeMs + 1000)
        }
    }

    @Test
    fun `full re-analysis after multiple changes should be consistent`() {
        val sid = StoryboardId.generate()
        var segments = generateMixedSegments(sid, 50)

        for (iteration in 0 until 10) {
            val idx = iteration * 5
            val original = segments[idx]
            segments = segments.toMutableList().apply {
                this[idx] = original.copy(
                    stimulusIntensity = 5,
                    hasReversal = true,
                    version = original.version + 1
                )
            }
        }

        val fullFindings = rules.flatMap { it.evaluate(segments.sortedBy { it.startTimeMs }) }

        var incrementalSegments = generateMixedSegments(sid, 50)
        for (iteration in 0 until 10) {
            val idx = iteration * 5
            val original = incrementalSegments[idx]
            incrementalSegments = incrementalSegments.toMutableList().apply {
                this[idx] = original.copy(
                    stimulusIntensity = 5,
                    hasReversal = true,
                    version = original.version + 1
                )
            }
        }

        val fullFromIncremental = rules.flatMap { it.evaluate(incrementalSegments.sortedBy { it.startTimeMs }) }
        assertEquals(fullFindings.size, fullFromIncremental.size)
    }

    private fun generateMixedSegments(sid: StoryboardId, count: Int): List<StoryboardSegment> {
        return (0 until count).map { i ->
            val intensity = when {
                i % 5 == 0 -> 4
                i % 7 == 0 -> 5
                i % 3 == 0 -> 1
                else -> 3
            }
            val hasReversal = i % 11 == 0 && intensity >= 4
            val isKp = i % 10 == 0
            val hasScroll = i % 20 == 0
            StoryboardSegment(
                id = SegmentId.generate(),
                storyboardId = sid,
                order = i,
                startTimeMs = i * 3000L,
                endTimeMs = (i + 1) * 3000L,
                stimulusIntensity = intensity,
                hasReversal = hasReversal,
                isKnowledgePoint = isKp,
                knowledgePointId = if (isKp) "kp_$i" else null,
                hasScrollInducement = hasScroll,
                contentType = if (isKp) ContentType.KNOWLEDGE_POINT else ContentType.CONTENT
            )
        }
    }

    private fun makeTestSegment(
        sid: StoryboardId, order: Int, startMs: Long, endMs: Long,
        intensity: Int = 3, hasReversal: Boolean = false,
        isKp: Boolean = false, kpId: String? = null,
        hasScroll: Boolean = false
    ): StoryboardSegment = StoryboardSegment(
        id = SegmentId.generate(),
        storyboardId = sid,
        order = order,
        startTimeMs = startMs,
        endTimeMs = endMs,
        stimulusIntensity = intensity,
        hasReversal = hasReversal,
        isKnowledgePoint = isKp,
        knowledgePointId = kpId,
        hasScrollInducement = hasScroll
    )
}

class OutOfOrderBufferTest {

    @Test
    fun `buffer should release events in correct version order`() = kotlinx.coroutines.runBlocking {
        val buffer = OutOfOrderEventBuffer()
        val storyboardId = "sb-123"

        val event1 = makeTestEvent(storyboardId, 1)
        val event2 = makeTestEvent(storyboardId, 2)
        val event3 = makeTestEvent(storyboardId, 3)

        val record = MockConsumerRecord(storyboardId, "payload")

        val readyAfter3 = buffer.addAndGetReady(storyboardId, 1, event3, record)
        assertEquals(0, readyAfter3.size, "Should not release event 3 when expecting version 1")

        val readyAfter1 = buffer.addAndGetReady(storyboardId, 1, event1, record)
        assertEquals(1, readyAfter1.size, "Should release event 1")
        assertEquals(1L, readyAfter1[0].event.aggregateVersion)
        buffer.markProcessed(storyboardId, 1)

        val readyAfter2 = buffer.addAndGetReady(storyboardId, 2, event2, record)
        assertEquals(2, readyAfter2.size, "Should release both event 2 and 3")
        assertEquals(2L, readyAfter2[0].event.aggregateVersion)
        assertEquals(3L, readyAfter2[1].event.aggregateVersion)
    }

    @Test
    fun `buffer should detect duplicate events`() = kotlinx.coroutines.runBlocking {
        val buffer = OutOfOrderEventBuffer()
        val storyboardId = "sb-456"
        val record = MockConsumerRecord(storyboardId, "payload")

        val event = makeTestEvent(storyboardId, 1)
        buffer.addAndGetReady(storyboardId, 1, event, record)
        buffer.markProcessed(storyboardId, 1)

        val duplicate = makeTestEvent(storyboardId, 1)
        val ready = buffer.addAndGetReady(storyboardId, 2, duplicate, record)
        assertEquals(0, ready.size, "Duplicate version should be skipped")
    }

    @Test
    fun `buffer should handle sequential events without buffering`() = kotlinx.coroutines.runBlocking {
        val buffer = OutOfOrderEventBuffer()
        val storyboardId = "sb-789"
        val record = MockConsumerRecord(storyboardId, "payload")

        for (version in 1L..10L) {
            val event = makeTestEvent(storyboardId, version)
            val ready = buffer.addAndGetReady(storyboardId, version, event, record)
            assertEquals(1, ready.size, "Sequential event v$version should be immediately ready")
            buffer.markProcessed(storyboardId, version)
        }
    }

    private fun makeTestEvent(storyboardId: String, version: Long): DomainEvent {
        return SegmentAdded(
            eventId = UUID.randomUUID().toString(),
            storyboardId = storyboardId,
            aggregateVersion = version,
            segment = com.calmcut.domain.events.SegmentData(
                order = version.toInt(),
                startTimeMs = version * 3000,
                endTimeMs = (version + 1) * 3000,
                stimulusIntensity = 3
            )
        )
    }
}

class MockConsumerRecord(
    private val key: String,
    private val value: String
) : org.apache.kafka.clients.consumer.ConsumerRecord<String, String>(
    "test-topic", 0, 0L, key, value
)
