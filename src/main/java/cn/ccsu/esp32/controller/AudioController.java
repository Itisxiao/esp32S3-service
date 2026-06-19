package cn.ccsu.esp32.controller;

import cn.ccsu.esp32.handler.AudioWebSocketHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.http.HttpMethod;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.ResponseExtractor;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.io.BufferedInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 音频控制器 - 提供向ESP32-S3推送音频的REST接口
 * @author 潇洒哥queen
 * @date 2026/6/17
 */
@Slf4j
@RestController
@RequestMapping("/api/audio")
public class AudioController {

    @Resource
    private AudioWebSocketHandler audioWebSocketHandler;

    private final RestTemplate restTemplate = new RestTemplate();

    // ==================== 常量配置 ====================

    /** 每次发送的音频数据块大小 (bytes) */
    private static final int CHUNK_SIZE = 1600;

    /** 每块之间的发送间隔 (ms)，模拟实时播放速率 */
    private static final long CHUNK_INTERVAL_MS = 50;

    /**
     * 发送队列容量（块数）。
     * 队列满时生产者(下载线程)会阻塞等待，实现背压控制；
     * 队列空时消费者(定时发送)会跳过本轮，等待数据就绪。
     */
    private static final int QUEUE_CAPACITY = 20;

    /** 下载线程池超时 (ms)，超过此时间队列仍满则放弃本次推送 */
    private static final long ENQUEUE_TIMEOUT_MS = 5000;

    // ==================== 线程池 & 状态 ====================

    /** 固定速率定时发送线程：每 50ms 从队列 poll 一块并发送 */
    private final ScheduledExecutorService sendScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "audio-sender");
        t.setDaemon(true);
        return t;
    });

    /** 下载/分块线程池：异步下载音频并填充队列 */
    private final ExecutorService downloadExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "audio-loader");
        t.setDaemon(true);
        return t;
    });

    /**
     * 音频块发送队列（有界阻塞队列）。
     * 生产者(下载线程) offer 入队，满则阻塞等待；
     * 消费者(定时线程) poll 出队，空则跳过本轮。
     */
    private final LinkedBlockingQueue<byte[]> chunkQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);

    /** 当前活跃的下载任务 Future，用于取消旧推送 */
    private volatile Future<?> activeDownloadTask = null;

    /** 当前活跃的推送ID，消费者只认这个ID */
    private volatile String activePushId = null;

    /** 统计：已发送块数 */
    private final AtomicInteger totalSent = new AtomicInteger(0);

    /** 统计：队列空跳过次数（underrun 计数） */
    private final AtomicInteger underrunCount = new AtomicInteger(0);

    /** 统计：推送开始时间 */
    private final AtomicLong pushStartTime = new AtomicLong(0);

    /** 当前推送的总块数（用于完成判定） */
    private final AtomicInteger totalChunksExpected = new AtomicInteger(0);

    // ==================== 初始化 ====================

    /**
     * 启动固定速率发送循环。
     * 每 CHUNK_INTERVAL_MS 毫秒从队列取一块发送，队列空则跳过。
     */
    @javax.annotation.PostConstruct
    public void init() {
        sendScheduler.scheduleAtFixedRate(this::sendNextChunk,
                CHUNK_INTERVAL_MS, CHUNK_INTERVAL_MS, TimeUnit.MILLISECONDS);
        log.info("音频发送调度已启动, 固定间隔={}ms, 队列容量={}", CHUNK_INTERVAL_MS, QUEUE_CAPACITY);
    }

    // ==================== REST 接口 ====================

    /**
     * 从MinIO下载音频并推送给所有已连接的ESP32-S3设备。
     * 下载和分块在独立线程异步执行，通过有界队列与发送线程解耦。
     *
     * @param url MinIO音频文件链接
     * @return 推送结果
     */
    @GetMapping("/push")
    public Map<String, Object> pushAudio(@RequestParam String url) {
        Map<String, Object> result = new HashMap<>();
        log.info("收到音频推送请求, url={}", url);

        int onlineCount = audioWebSocketHandler.getOnlineCount();
        if (onlineCount == 0) {
            result.put("success", false);
            result.put("message", "当前没有已连接的ESP32设备");
            return result;
        }

        String pushId = java.util.UUID.randomUUID().toString().substring(0, 8);

        // 取消旧的下载任务并清空队列残留
        cancelPreviousPush(pushId);

        activePushId = pushId;

        totalSent.set(0);
        underrunCount.set(0);
        pushStartTime.set(System.currentTimeMillis());
        totalChunksExpected.set(0);

        // 异步下载 + 填充队列
        activeDownloadTask = downloadExecutor.submit(() -> loadAndEnqueue(url, pushId, onlineCount));

        result.put("success", true);
        result.put("message", "音频推送已启动");
        result.put("pushId", pushId);
        result.put("chunkSize", CHUNK_SIZE);
        result.put("intervalMs", CHUNK_INTERVAL_MS);
        result.put("queueCapacity", QUEUE_CAPACITY);
        result.put("deviceCount", onlineCount);
        return result;
    }

    // ==================== 核心逻辑 ====================

    /**
     * 取消上一次推送：中断下载线程 + 清空队列 + 重置状态。
     * 新推送调用前必须先执行此方法，确保旧的消费者不会再误触 finishPush。
     */
    private void cancelPreviousPush(String newPushId) {
        // 1. 切换 activePushId，让消费者 sendNextChunk 不再为旧推送调用 finishPush
        String oldPushId = activePushId;
        if (oldPushId != null) {
            log.info("[push:{}] 检测到旧推送[{}]，取消并清空队列", newPushId, oldPushId);
        }

        // 2. 中断旧的下载任务
        Future<?> oldTask = activeDownloadTask;
        if (oldTask != null && !oldTask.isDone()) {
            oldTask.cancel(true);
            log.debug("[push:{}] 已中断旧下载线程", newPushId);
        }

        // 3. 清空队列中残留的旧音频块
        int discarded = chunkQueue.size();
        chunkQueue.clear();
        if (discarded > 0) {
            log.info("[push:{}] 清空队列残留{}块", newPushId, discarded);
        }
    }

    /**
     * 【生产者】流式下载音频 → 分块 → 填充队列。
     * 使用 InputStream 边下载边分块入队，避免将整个文件加载到内存。
     * 队列满时 offer 超时等待（背压），超时过久则放弃本次推送。
     */
    private void loadAndEnqueue(String url, String pushId, int deviceCount) {
        try {
            log.info("[push:{}] 开始流式下载, url={}", pushId, url);

            // 使用 RestTemplate execute 获取 InputStream，边读边分块入队
            Integer totalChunks = restTemplate.execute(url, HttpMethod.GET, null, (ResponseExtractor<Integer>) response -> {
                long contentLength = response.getHeaders().getContentLength();
                int estimatedChunks = contentLength > 0
                        ? (int) ((contentLength + CHUNK_SIZE - 1) / CHUNK_SIZE)
                        : -1;
                totalChunksExpected.set(Math.max(estimatedChunks, 0));

                log.info("[push:{}] 响应头已到达, Content-Length={} bytes, 预估{}块, 开始流式读取",
                        pushId, contentLength, estimatedChunks);

                try (InputStream raw = response.getBody();
                     BufferedInputStream bis = new BufferedInputStream(raw, CHUNK_SIZE * 4)) {

                    int chunkIndex = 0;
                    byte[] buffer = new byte[CHUNK_SIZE];

                    while (!Thread.currentThread().isInterrupted()) {
                        int bytesRead = readFully(bis, buffer);
                        if (bytesRead <= 0) {
                            break;
                        }

                        // 如果已被新推送取代，立即停止
                        if (!pushId.equals(activePushId)) {
                            log.info("[push:{}] 已被新推送取代，停止入队 (已入队{}块)", pushId, chunkIndex);
                            break;
                        }

                        // 如果实际读到的不足 CHUNK_SIZE，拷贝精确长度
                        byte[] chunk;
                        if (bytesRead < CHUNK_SIZE) {
                            chunk = new byte[bytesRead];
                            System.arraycopy(buffer, 0, chunk, 0, bytesRead);
                        } else {
                            chunk = new byte[CHUNK_SIZE];
                            System.arraycopy(buffer, 0, chunk, 0, CHUNK_SIZE);
                        }

                        // 背压：队列满时阻塞等待，直到超时
                        boolean offered;
                        try {
                            offered = chunkQueue.offer(chunk, ENQUEUE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            log.info("[push:{}] 入队被中断（可能已被新推送取消）, 已入队{}块", pushId, chunkIndex);
                            break;
                        }
                        if (!offered) {
                            log.warn("[push:{}] 队列持续满超过{}ms，放弃剩余块", pushId, ENQUEUE_TIMEOUT_MS);
                            break;
                        }

                        chunkIndex++;

                        // 周期性打印队列状态
                        if (chunkIndex % 50 == 0) {
                            log.debug("[push:{}] 已入队{}块, 队列深度={}/{}",
                                    pushId, chunkIndex, chunkQueue.size(), QUEUE_CAPACITY);
                        }
                    }

                    // 如果之前无法预估总块数，现在补设
                    if (estimatedChunks <= 0) {
                        totalChunksExpected.set(chunkIndex);
                    }

                    log.info("[push:{}] 流式读取完毕, 共入队{}块", pushId, chunkIndex);
                    return chunkIndex;
                }
            });

            log.info("[push:{}] 下载+入队完成, 共{}块, 等待发送线程消费完毕", pushId, totalChunks);

        } catch (Exception e) {
            if (Thread.currentThread().isInterrupted() || !pushId.equals(activePushId)) {
                log.info("[push:{}] 下载任务被取消（被新推送取代）", pushId);
            } else {
                log.error("[push:{}] 音频流式加载失败", pushId, e);
            }
        }
    }

    /**
     * 从 InputStream 尽量读满 buffer。
     * 返回实际读取的字节数，-1 表示已到流末尾。
     */
    private int readFully(InputStream in, byte[] buffer) throws java.io.IOException {
        int totalRead = 0;
        while (totalRead < buffer.length) {
            int n = in.read(buffer, totalRead, buffer.length - totalRead);
            if (n == -1) {
                return totalRead > 0 ? totalRead : -1;
            }
            totalRead += n;
        }
        return totalRead;
    }

    /**
     * 【消费者】定时发送线程的核心方法，每 50ms 调用一次。
     * 从队列 poll 一块音频数据并发送给所有设备。
     * 队列空时跳过（计入 underrun），不阻塞定时线程。
     * 只处理与当前 activePushId 匹配的推送，避免旧推送干扰新推送。
     */
    private void sendNextChunk() {
        String pushId = activePushId;

        // 队列为空时直接跳过
        byte[] chunk = chunkQueue.poll();
        if (chunk == null) {
            // 如果正在推送但队列空，计为 underrun
            if (pushId != null) {
                underrunCount.incrementAndGet();
                // 检查是否所有块都已发完
                if (totalSent.get() >= totalChunksExpected.get() && totalChunksExpected.get() > 0) {
                    finishPush(pushId);
                }
            }
            return;
        }

        try {
            int sentTo = audioWebSocketHandler.sendAudioToAll(chunk);
            int sent = totalSent.incrementAndGet();

            if (sentTo == 0 && pushId != null) {
                log.warn("[push:{}] 无可用设备, 已发送{}/{}块",
                        pushId, sent, totalChunksExpected.get());
                chunkQueue.clear();
                finishPush(pushId);
                return;
            }

            // 所有块发送完毕
            if (pushId != null && sent >= totalChunksExpected.get() && totalChunksExpected.get() > 0) {
                finishPush(pushId);
            }
        } catch (Exception e) {
            log.error("[push:{}] 发送音频块失败", pushId, e);
        }
    }

    /**
     * 推送完成，打印统计信息并重置状态。
     * 只有当 pushId 仍然等于 activePushId 时才执行，防止旧推送干扰新推送。
     */
    private void finishPush(String pushId) {
        // 原子性地检查并清除：只有当前活跃推送才允许 finish
        if (pushId == null || !pushId.equals(activePushId)) {
            return;
        }
        // 用 synchronized 确保只有一个线程能完成 finish
        synchronized (this) {
            if (!pushId.equals(activePushId)) {
                return;
            }
            activePushId = null;
            activeDownloadTask = null;
        }

        long elapsed = System.currentTimeMillis() - pushStartTime.get();
        int expected = totalChunksExpected.get();
        int sent = totalSent.get();
        int underruns = underrunCount.get();
        long theoretical = (long) expected * CHUNK_INTERVAL_MS;

        log.info("[push:{}] 推送完成, 发送{}/{}块, 耗时{}ms (理论{}ms), underrun次数={}, 队列剩余={}",
                pushId, sent, expected, elapsed, theoretical, underruns, chunkQueue.size());

        if (underruns > 0) {
            log.warn("[push:{}] 出现{}次underrun，考虑增大队列容量或检查网络延迟", pushId, underruns);
        }
    }

    // ==================== 生命周期 ====================

    @PreDestroy
    public void shutdown() {
        log.info("正在关闭音频推送服务...");
        sendScheduler.shutdown();
        downloadExecutor.shutdown();
        try {
            sendScheduler.awaitTermination(3, TimeUnit.SECONDS);
            downloadExecutor.awaitTermination(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            sendScheduler.shutdownNow();
            downloadExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.info("音频推送服务已关闭");
    }
}
