package com.calmcut.studio.analysis

import com.calmcut.studio.domain.Segment
import com.calmcut.studio.domain.Timeline
import com.calmcut.studio.worker.TimelineDiff
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * Property test: for a random storyboard subjected to a random single-segment
 * edit (insert / update / delete), the incremental recompute over the affected
 * window must be identical to a full recompute ("增量/全量等价"). This exercises
 * the equivalence guarantee across thousands of randomized scenarios.
 */
class IncrementalEquivalenceTest {

    private val analyzer = RiskAnalyzer(AnalysisSettings())

    private fun randomSegment(id: String, order: Int, rnd: Random) = Segment(
        id = id,
        orderIndex = order,
        durationMs = (1000L..6000L).random(rnd),
        intensity = (1..5).random(rnd),
        strongReversal = rnd.nextInt(3) == 0,
        declineInducement = rnd.nextInt(6) == 0,
        knowledgePoint = if (rnd.nextInt(3) == 0) "KP-${rnd.nextInt(4)}" else null,
    )

    private fun reindex(segs: List<Segment>): List<Segment> =
        segs.mapIndexed { i, s -> s.copy(orderIndex = i) }

    @Test
    fun `incremental equals full over random edits`() {
        val rnd = Random(20260724)
        var mismatches = 0

        repeat(3000) { iter ->
            val n = 1 + rnd.nextInt(40)
            val base = reindex((0 until n).map { randomSegment("s$it", it, rnd) })
            val baseTimeline = Timeline.of(base)
            val prior = analyzer.analyzeFull("sb", 1, baseTimeline)

            // Apply a random edit.
            val op = rnd.nextInt(3)
            val edited: List<Segment> = when (op) {
                0 -> { // update a random existing segment in place
                    if (base.isEmpty()) base
                    else {
                        val idx = rnd.nextInt(base.size)
                        base.toMutableList().also {
                            it[idx] = randomSegment(base[idx].id, idx, rnd)
                        }
                    }
                }
                1 -> { // insert a new segment at a random position
                    val pos = rnd.nextInt(base.size + 1)
                    val mutable = base.toMutableList()
                    mutable.add(pos, randomSegment("new-$iter", pos, rnd))
                    reindex(mutable)
                }
                else -> { // delete a random segment
                    if (base.isEmpty()) base
                    else {
                        val idx = rnd.nextInt(base.size)
                        reindex(base.toMutableList().also { it.removeAt(idx) })
                    }
                }
            }

            val newTimeline = Timeline.of(edited)
            val full = analyzer.analyzeFull("sb", 2, newTimeline)
            val changed = TimelineDiff.changedRange(baseTimeline, newTimeline)
            val incremental = analyzer.analyzeIncremental(prior, 2, newTimeline, changed)

            if (full.canonical() != incremental.canonical()) {
                mismatches++
                if (mismatches <= 3) {
                    println("MISMATCH iter=$iter op=$op changed=$changed")
                    println("  full=${full.canonical().map { it.code to it.hitSegmentIds }}")
                    println("  incr=${incremental.canonical().map { it.code to it.hitSegmentIds }}")
                }
            }
        }
        assertEquals(0, mismatches, "incremental diverged from full in $mismatches cases")
    }

    @Test
    fun `full recompute is deterministic`() {
        val rnd = Random(7)
        val segs = reindex((0 until 25).map { randomSegment("s$it", it, rnd) })
        val a = analyzer.analyzeFull("sb", 1, Timeline.of(segs))
        val b = analyzer.analyzeFull("sb", 1, Timeline.of(segs))
        assertEquals(a.canonical(), b.canonical())
    }
}
