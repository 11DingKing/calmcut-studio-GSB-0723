package com.calmcut.studio.worker

import com.calmcut.studio.analysis.AnalysisResult
import com.calmcut.studio.analysis.RiskAnalyzer
import com.calmcut.studio.db.EventStore
import com.calmcut.studio.projection.StoryboardProjector
import com.calmcut.studio.projection.StoryboardState
import org.slf4j.LoggerFactory

/** Result of rebuilding a storyboard's projection from its event log. */
data class RebuildResult(
    val storyboardId: String,
    val version: Long,
    val eventsReplayed: Int,
    val analysis: AnalysisResult,
)

/**
 * Rebuilds a storyboard's projection and analysis "from zero" by replaying its
 * persisted event log ("从零重建投影"). Used for recovery, drift self-heal, and
 * as an operator-callable API. Every rebuild is recorded in the audit trail so
 * the operation is fully auditable.
 *
 * Because event folding is deterministic and pure, the rebuilt projection is
 * identical to the live one when no drift exists — the property exploited by the
 * drift detector.
 */
class RebuildService(
    private val eventStore: EventStore,
    private val repo: WorkerRepository,
    private val analyzer: RiskAnalyzer,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    suspend fun rebuild(storyboardId: String): RebuildResult {
        return try {
            val log0 = eventStore.readLogSuspending(storyboardId)
            // Fold the ordered log from zero into an authoritative state.
            var state = StoryboardState.empty(storyboardId)
            var analysis: AnalysisResult? = null
            for (event in log0.sortedBy { it.version }) {
                val old = state.timeline()
                state = StoryboardProjector.apply(state, event)
                val now = state.timeline()
                analysis = if (analysis == null || old.size == 0) {
                    analyzer.analyzeFull(storyboardId, state.version, now)
                } else {
                    val changed = TimelineDiff.changedRange(old, now)
                    analyzer.analyzeIncremental(analysis, state.version, now, changed)
                }
            }
            val finalAnalysis = analysis ?: analyzer.analyzeFull(storyboardId, state.version, state.timeline())

            // Replace projection + processed-version atomically, marking a synthetic id.
            repo.resetStoryboard(storyboardId)
            repo.commitApplied(state, finalAnalysis, "rebuild:${storyboardId}:${state.version}")

            repo.recordAudit(
                WorkerRepository.AUDIT_REBUILD, storyboardId,
                "rebuilt to v${state.version} from ${log0.size} events",
                WorkerRepository.OUTCOME_SUCCESS,
            )
            RebuildResult(storyboardId, state.version, log0.size, finalAnalysis)
        } catch (t: Throwable) {
            repo.recordAudit(
                WorkerRepository.AUDIT_REBUILD, storyboardId,
                "rebuild failed: ${t.message}", WorkerRepository.OUTCOME_FAILURE,
            )
            throw t
        }
    }
}
