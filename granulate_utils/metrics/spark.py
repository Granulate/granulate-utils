#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#
SPARK_APPS_PATH = "api/v1/applications"

SPARK_JOB_METRICS = {
    metric: f"spark_job_{metric}"
    for metric in (
        "numActiveTasks",
        "numActiveStages",
    )
}
SPARK_JOB_DIFF_METRICS = {
    metric: f"spark_job_diff_{metric}"
    for metric in (
        "numTasks",
        "numCompletedTasks",
        "numSkippedTasks",
        "numFailedTasks",
        "numCompletedStages",
        "numSkippedStages",
        "numFailedStages",
    )
}
SPARK_STAGE_METRICS = {
    metric: f"spark_stage_{metric}"
    for metric in (
        "numActiveTasks",
        "numCompleteTasks",
        "numFailedTasks",
        "executorRunTime",
        "inputBytes",
        "inputRecords",
        "outputBytes",
        "outputRecords",
        "shuffleReadBytes",
        "shuffleReadRecords",
        "shuffleWriteBytes",
        "shuffleWriteRecords",
        "memoryBytesSpilled",
        "diskBytesSpilled",
    )
}
SPARK_RDD_METRICS = {
    metric: f"spark_rdd_{metric}" for metric in ("numPartitions", "numCachedPartitions", "memoryUsed", "diskUsed")
}
SPARK_TASK_SUMMARY_METRICS = {
    metric: f"spark_stage_tasks_summary_{metric}"
    for metric in (
        "executorDeserializeTime",
        "executorDeserializeCpuTime",
        "executorRunTime",
        "executorCpuTime",
        "resultSize",
        "jvmGcTime",
        "resultSerializationTime",
        "gettingResultTime",
        "schedulerDelay",
        "peakExecutionMemory",
        "memoryBytesSpilled",
        "diskBytesSpilled",
    )
}
SPARK_STREAMING_STATISTICS_METRICS = {
    metric: f"spark_streaming_statistics_{metric}"
    for metric in (
        "avgInputRate",
        "avgProcessingTime",
        "avgSchedulingDelay",
        "avgTotalDelay",
        "batchDuration",
        "numActiveBatches",
        "numActiveReceivers",
        "numInactiveReceivers",
        "numProcessedRecords",
        "numReceivedRecords",
        "numReceivers",
        "numRetainedCompletedBatches",
        "numTotalCompletedBatches",
    )
}
SPARK_STRUCTURED_STREAMING_METRICS = {
    "inputRate-total": "spark_structured_streaming_input_rate",
    "latency": "spark_structured_streaming_latency",
    "processingRate-total": "spark_structured_streaming_processing_rate",
    "states-rowsTotal": "spark_structured_streaming_rows_count",
    "states-usedBytes": "spark_structured_streaming_used_bytes",
}
