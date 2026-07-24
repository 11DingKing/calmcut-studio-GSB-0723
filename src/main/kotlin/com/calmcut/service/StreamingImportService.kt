package com.calmcut.service

import com.calmcut.domain.StoryboardId
import com.calmcut.domain.events.ImportJobCancelled
import com.calmcut.domain.events.ImportJobCompleted
import com.calmcut.domain.events.ImportJobCreated
import com.calmcut.domain.events.SegmentData
import com.calmcut.domain.events.SegmentsBatchImported
import com.calmcut.infrastructure.repository.EventLogRepository
import com.calmcut.infrastructure.repository.ImportJobStatus
import com.calmcut.infrastructure.repository.ImportRepository
import com.calmcut.infrastructure.repository.OptimisticLockException
import com.calmcut.infrastructure.repository.StoryboardRepository
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import mu.KotlinLogging
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

private val logger = KotlinLogging.logger {}

class StreamingImportService(
    private val storyboardRepository: StoryboardRepository,
    private val eventLogRepository: EventLogRepository,
    private val importRepository: ImportRepository,
    private val eventProcessor: EventProcessor,
    private val batchSize: Int = 1000,
    private val maxConcurrentJobs: Int = 2
) {
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private val activeJobs = mutableMapOf<String, Job>()
    private val cancellationFlags = mutableMapOf<String, AtomicBoolean>()

    suspend fun startStreamingImport(
        storyboardId: StoryboardId,
        totalSegments: Int,
        segmentFlow: Flow<SegmentData>
    ): String {
        val jobId = importRepository.createJob(
            storyboardId = storyboardId.value,
            totalSegments = totalSegments
        )

        val cancelFlag = AtomicBoolean(false)
        cancellationFlags[jobId] = cancelFlag

        val currentVersion = storyboardRepository.getCurrentVersion(storyboardId) ?: 0L

        val event = ImportJobCreated(
            storyboardId = storyboardId.value,
            aggregateVersion = currentVersion + 1,
            importJobId = jobId,
            totalSegments = totalSegments
        )
        eventLogRepository.append(event)

        val job = scope.launch {
            try {
                processStreamingImport(jobId, storyboardId, totalSegments, segmentFlow, cancelFlag, currentVersion)
            } catch (e: CancellationException) {
                handleCancellation(jobId, storyboardId)
            } catch (e: Exception) {
                logger.error(e) { "Import job $jobId failed" }
                importRepository.updateJobStatus(jobId, ImportJobStatus.FAILED, e.message)
            }
        }

        activeJobs[jobId] = job
        return jobId
    }

    private suspend fun processStreamingImport(
        jobId: String,
        storyboardId: StoryboardId,
        totalSegments: Int,
        segmentFlow: Flow<SegmentData>,
        cancelFlag: AtomicBoolean,
        initialVersion: Long
    ) {
        val buffer = mutableListOf<SegmentData>()
        var batchNumber = 0
        var processedCount = 0

        segmentFlow.collect { segment ->
            if (cancelFlag.get() || importRepository.isCancelled(jobId)) {
                handleCancellation(jobId, storyboardId)
                return@collect
            }

            buffer.add(segment)

            if (buffer.size >= batchSize) {
                val batch = buffer.toList()
                buffer.clear()
                processBatch(jobId, storyboardId, batch, batchNumber++, initialVersion, processedCount)
                processedCount += batch.size
            }
        }

        if (buffer.isNotEmpty()) {
            val batch = buffer.toList()
            buffer.clear()
            processBatch(jobId, storyboardId, batch, batchNumber, initialVersion, processedCount)
            processedCount += batch.size
        }

        if (!cancelFlag.get() && !importRepository.isCancelled(jobId)) {
            val currentVersion = storyboardRepository.getCurrentVersion(storyboardId) ?: initialVersion
            val completeEvent = ImportJobCompleted(
                storyboardId = storyboardId.value,
                aggregateVersion = currentVersion + 1,
                importJobId = jobId,
                totalProcessed = processedCount,
                totalFailed = 0
            )
            eventLogRepository.append(completeEvent)
            eventLogRepository.appendOutbox(completeEvent, "storyboard-events")

            importRepository.updateJobStatus(jobId, ImportJobStatus.COMPLETED)
            logger.info { "Import job $jobId completed: $processedCount segments processed" }

            eventProcessor.processEvent(completeEvent)
        }

        activeJobs.remove(jobId)
        cancellationFlags.remove(jobId)
    }

    private suspend fun processBatch(
        jobId: String,
        storyboardId: StoryboardId,
        batch: List<SegmentData>,
        batchNumber: Int,
        initialVersion: Long,
        processedSoFar: Int
    ) {
        var version = storyboardRepository.getCurrentVersion(storyboardId) ?: initialVersion

        importRepository.insertBatches(jobId, listOf(batch))

        try {
            version = storyboardRepository.incrementVersion(storyboardId, version)

            val event = SegmentsBatchImported(
                storyboardId = storyboardId.value,
                aggregateVersion = version,
                importJobId = jobId,
                segments = batch,
                batchStartVersion = processedSoFar.toLong()
            )

            eventLogRepository.append(event)
            eventLogRepository.appendOutbox(event, "storyboard-events")

            importRepository.markBatchProcessed(jobId, batchNumber, success = true)

            eventProcessor.processEvent(event)
        } catch (e: OptimisticLockException) {
            logger.warn { "Optimistic lock conflict for batch $batchNumber, retrying..." }
            version = storyboardRepository.getCurrentVersion(storyboardId) ?: initialVersion
            processBatch(jobId, storyboardId, batch, batchNumber, initialVersion, processedSoFar)
        } catch (e: Exception) {
            logger.error(e) { "Failed to process batch $batchNumber for job $jobId" }
            importRepository.markBatchProcessed(jobId, batchNumber, success = false, error = e.message)
        }
    }

    private suspend fun handleCancellation(jobId: String, storyboardId: StoryboardId) {
        val checkpoint = importRepository.getCheckpoint(jobId)
        val currentVersion = storyboardRepository.getCurrentVersion(storyboardId) ?: 0L
        val cancelEvent = ImportJobCancelled(
            storyboardId = storyboardId.value,
            aggregateVersion = currentVersion + 1,
            importJobId = jobId,
            checkpoint = checkpoint
        )
        eventLogRepository.append(cancelEvent)
        importRepository.updateJobStatus(jobId, ImportJobStatus.CANCELLED)
        logger.info { "Import job $jobId cancelled at checkpoint $checkpoint" }
    }

    suspend fun cancelImport(jobId: String): Boolean {
        val job = activeJobs[jobId] ?: return false
        cancellationFlags[jobId]?.set(true)
        job.cancel()
        return true
    }

    suspend fun resumeImport(jobId: String, segmentFlow: Flow<SegmentData>): Boolean {
        val jobRecord = importRepository.getJob(jobId) ?: return false
        if (jobRecord.status != ImportJobStatus.PAUSED && jobRecord.status != ImportJobStatus.CANCELLED) {
            return false
        }

        val checkpoint = importRepository.resumeFromCheckpoint(jobId)
        val storyboardId = StoryboardId(jobRecord.storyboardId)
        val cancelFlag = AtomicBoolean(false)
        cancellationFlags[jobId] = cancelFlag

        val job = scope.launch {
            try {
                processStreamingImport(
                    jobId = jobId,
                    storyboardId = storyboardId,
                    totalSegments = jobRecord.totalSegments,
                    segmentFlow = segmentFlow,
                    cancelFlag = cancelFlag,
                    initialVersion = storyboardRepository.getCurrentVersion(storyboardId) ?: 0L
                )
            } catch (e: Exception) {
                logger.error(e) { "Failed to resume import job $jobId" }
                importRepository.updateJobStatus(jobId, ImportJobStatus.FAILED, e.message)
            }
        }

        activeJobs[jobId] = job
        return true
    }

    suspend fun getJobStatus(jobId: String) = importRepository.getJob(jobId)

    fun createSegmentFlowFromList(segments: List<SegmentData>): Flow<SegmentData> = flow {
        segments.forEach { emit(it) }
    }
}
