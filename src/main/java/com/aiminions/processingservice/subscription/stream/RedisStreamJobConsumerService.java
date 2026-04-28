package com.aiminions.processingservice.subscription.stream;

import com.aiminions.processingservice.config.ProcessingProperties;
import com.aiminions.processingservice.jobs.balancedsync.BalancedSyncJobMessage;
import com.aiminions.processingservice.jobs.balancedsync.BalancedSyncPipeline;
import com.aiminions.processingservice.jobs.subtitles.SubtitleJobMessage;
import com.aiminions.processingservice.jobs.subtitles.SubtitlePipeline;
import com.aiminions.processingservice.jobs.transcribe.TranscribeJobMessage;
import com.aiminions.processingservice.jobs.transcribe.TranscribePipeline;
import com.aiminions.processingservice.jobs.workspaceexport.WorkspaceExportJobMessage;
import com.aiminions.processingservice.jobs.workspaceexport.WorkspaceExportPipeline;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@ConditionalOnProperty(name = "app.processing.queue-mode", havingValue = "stream", matchIfMissing = true)
@RequiredArgsConstructor
@Slf4j
public class RedisStreamJobConsumerService {
    private final StringRedisTemplate redis;
    private final ProcessingProperties props;
    private final ObjectMapper objectMapper;
    private final TranscribePipeline transcribePipeline;
    private final SubtitlePipeline subtitlePipeline;
    private final BalancedSyncPipeline balancedSyncPipeline;
    private final WorkspaceExportPipeline workspaceExportPipeline;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private ExecutorService workers;

    @PostConstruct
    void start() {
        if (!props.isConsumeJobs()) {
            log.info("Redis stream consumers disabled via app.processing.consume-jobs=false");
            return;
        }
        log.info("Starting Redis stream consumers");
        running.set(true);
        workers = Executors.newFixedThreadPool(4);
        workers.submit(() -> consumeForever(
                props.getRedisTranscribeJobStream(),
                props.getRedisTranscribeJobStreamGroup(),
                props.getRedisTranscribeJobDlqStream(),
                payload -> runPayload(payload, TranscribeJobMessage.class, transcribePipeline::run)
        ));
        workers.submit(() -> consumeForever(
                props.getRedisSubtitlesJobStream(),
                props.getRedisSubtitlesJobStreamGroup(),
                props.getRedisSubtitlesJobDlqStream(),
                payload -> runPayload(payload, SubtitleJobMessage.class, subtitlePipeline::run)
        ));
        workers.submit(() -> consumeForever(
                props.getRedisBalancedSyncJobStream(),
                props.getRedisBalancedSyncJobStreamGroup(),
                props.getRedisBalancedSyncJobDlqStream(),
                payload -> runPayload(payload, BalancedSyncJobMessage.class, balancedSyncPipeline::run)
        ));
        workers.submit(() -> consumeForever(
                props.getRedisWorkspaceExportJobStream(),
                props.getRedisWorkspaceExportJobStreamGroup(),
                props.getRedisWorkspaceExportJobDlqStream(),
                payload -> runPayload(payload, WorkspaceExportJobMessage.class, workspaceExportPipeline::run)
        ));
    }

    @PreDestroy
    void stop() {
        running.set(false);
        if (workers != null) {
            workers.shutdownNow();
        }
    }

    private <T> void runPayload(String payload, Class<T> clazz, java.util.function.Consumer<T> handler) {
        try {
            T msg = objectMapper.readValue(payload, clazz);
            handler.accept(msg);
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }

    private void consumeForever(String stream, String group, String dlqStream, java.util.function.Consumer<String> handler) {
        String consumerName = "worker-" + Thread.currentThread().threadId();
        ensureGroup(stream, group);
        reclaimPending(stream, group, consumerName);
        while (running.get()) {
            try {
                List<MapRecord<String, Object, Object>> records = redis.opsForStream().read(
                        org.springframework.data.redis.connection.stream.Consumer.from(group, consumerName),
                        StreamReadOptions.empty()
                                .block(Duration.ofMillis(Math.max(250, props.getRedisStreamReadBlockMs())))
                                .count(10),
                        StreamOffset.create(stream, ReadOffset.lastConsumed())
                );
                if (records == null || records.isEmpty()) {
                    continue;
                }
                for (MapRecord<String, Object, Object> record : records) {
                    log.debug(
                            "[redis-stream][consume] stream={} group={} consumer={} recordId={} mapKeys={}",
                            stream,
                            group,
                            consumerName,
                            record.getId() != null ? record.getId().getValue() : "null",
                            record.getValue() != null ? record.getValue().keySet() : List.of());
                    processRecord(stream, group, dlqStream, record, handler);
                }
            } catch (Exception ex) {
                if (looksLikeNoGroup(ex)) {
                    log.warn("Missing consumer group for stream={}, group={}; recreating", stream, group);
                    ensureGroup(stream, group);
                    continue;
                }
                log.error("Stream consumer loop error stream={} group={}", stream, group, ex);
            }
        }
    }

    private void processRecord(
            String stream,
            String group,
            String dlqStream,
            MapRecord<String, Object, Object> record,
            java.util.function.Consumer<String> handler
    ) {
        Map<Object, Object> map = record.getValue();
        String payload = asString(map.get("payload"));
        String jobId = asString(map.get("jobId"));
        String jobType = asString(map.get("jobType"));
        int attempts = parseInt(asString(map.get("attempts")));
        String doneKey = "ai-minions:stream:processed:" + jobType + ":" + jobId;
        log.info(
                "[redis-stream][record] stream={} group={} recordId={} jobType={} jobId={} attempts={} dlqStream={}",
                stream,
                group,
                record.getId() != null ? record.getId().getValue() : "null",
                jobType,
                jobId,
                attempts,
                dlqStream);
        if (!jobId.isBlank() && Boolean.FALSE.equals(redis.opsForValue().setIfAbsent(doneKey, "in-progress", Duration.ofHours(2)))) {
            log.info(
                    "[redis-stream][dedupe] stream={} group={} recordId={} jobType={} jobId={} action=ack_duplicate",
                    stream,
                    group,
                    record.getId() != null ? record.getId().getValue() : "null",
                    jobType,
                    jobId);
            redis.opsForStream().acknowledge(stream, group, record.getId());
            return;
        }
        try {
            handler.accept(payload);
            if (!jobId.isBlank()) {
                redis.opsForValue().set(doneKey, record.getId().getValue(), Duration.ofDays(7));
            }
            redis.opsForStream().acknowledge(stream, group, record.getId());
            log.info(
                    "[redis-stream][ack] stream={} group={} recordId={} jobType={} jobId={} attempts={} result=success",
                    stream,
                    group,
                    record.getId() != null ? record.getId().getValue() : "null",
                    jobType,
                    jobId,
                    attempts);
        } catch (Exception ex) {
            if (!jobId.isBlank()) {
                redis.delete(doneKey);
            }
            int nextAttempts = attempts + 1;
            if (nextAttempts >= Math.max(1, props.getRedisStreamMaxAttempts())) {
                log.error(
                        "[redis-stream][dlq] stream={} group={} recordId={} jobType={} jobId={} attempts={} maxAttempts={} error={}",
                        stream,
                        group,
                        record.getId() != null ? record.getId().getValue() : "null",
                        jobType,
                        jobId,
                        nextAttempts,
                        props.getRedisStreamMaxAttempts(),
                        safe(ex.getMessage()));
                redis.opsForStream().add(StreamRecords.newRecord().in(dlqStream).ofMap(Map.of(
                        "jobId", jobId,
                        "jobType", asString(map.get("jobType")),
                        "payload", payload,
                        "attempts", String.valueOf(nextAttempts),
                        "lastError", safe(ex.getMessage()),
                        "sourceRecordId", record.getId().getValue()
                )));
            } else {
                log.warn(
                        "[redis-stream][retry] stream={} group={} recordId={} jobType={} jobId={} nextAttempts={} error={}",
                        stream,
                        group,
                        record.getId() != null ? record.getId().getValue() : "null",
                        jobType,
                        jobId,
                        nextAttempts,
                        safe(ex.getMessage()));
                redis.opsForStream().add(StreamRecords.newRecord().in(stream).ofMap(Map.of(
                        "jobId", jobId,
                        "jobType", asString(map.get("jobType")),
                        "payload", payload,
                        "attempts", String.valueOf(nextAttempts)
                )));
            }
            redis.opsForStream().acknowledge(stream, group, record.getId());
        }
    }

    private void ensureGroup(String stream, String group) {
        try {
            redis.opsForStream().createGroup(stream, ReadOffset.latest(), group);
            log.info("Created stream group stream={} group={}", stream, group);
        } catch (Exception ignored) {
            // If the stream does not yet exist, bootstrap it then create the group.
            try {
                redis.opsForStream().add(StreamRecords.newRecord().in(stream).ofMap(Map.of(
                        "_bootstrap", "1"
                )));
                redis.opsForStream().createGroup(stream, ReadOffset.latest(), group);
                log.info("Bootstrapped stream and created group stream={} group={}", stream, group);
            } catch (Exception second) {
                // Group may already exist, or Redis may be unavailable.
                if (!looksLikeBusyGroup(second)) {
                    log.warn("Could not ensure stream group stream={} group={}: {}", stream, group, second.toString());
                }
            }
        }
    }

    private void reclaimPending(String stream, String group, String consumerName) {
        try {
            PendingMessages pending = redis.opsForStream().pending(stream, org.springframework.data.redis.connection.stream.Consumer.from(group, consumerName), Range.unbounded(), 50L);
            if (pending == null || pending.isEmpty()) {
                return;
            }
            List<RecordId> ids = pending.stream().map(PendingMessage::getId).toList();
            if (!ids.isEmpty()) {
                redis.opsForStream().claim(stream, group, consumerName, Duration.ofMillis(Math.max(1000, props.getRedisStreamClaimIdleMs())), ids.toArray(new RecordId[0]));
            }
        } catch (Exception ex) {
            log.warn("Pending reclaim failed stream={} group={}: {}", stream, group, ex.toString());
        }
    }

    private static int parseInt(String raw) {
        try {
            return raw == null ? 0 : Integer.parseInt(raw);
        } catch (Exception ignored) {
            return 0;
        }
    }

    private static String safe(String s) {
        if (s == null || s.isBlank()) {
            return "unknown_error";
        }
        return s.length() > 500 ? s.substring(0, 500) : s;
    }

    private static String asString(Object value) {
        return value == null ? "" : String.valueOf(value);
    }

    private static boolean looksLikeNoGroup(Exception ex) {
        if (ex == null || ex.getMessage() == null) return false;
        String msg = ex.getMessage().toLowerCase();
        return msg.contains("nogroup");
    }

    private static boolean looksLikeBusyGroup(Exception ex) {
        if (ex == null || ex.getMessage() == null) return false;
        String msg = ex.getMessage().toLowerCase();
        return msg.contains("busygroup");
    }
}
