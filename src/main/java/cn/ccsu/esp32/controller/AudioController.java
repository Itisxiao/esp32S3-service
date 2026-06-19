package cn.ccsu.esp32.controller;

import cn.ccsu.esp32.handler.AudioWebSocketHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
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

    /** 是否正在推送中，防止重复触发 */
    private final AtomicBoolean pushing = new AtomicBoolean(false);

    /** 统计：已发送块数 */
    private final AtomicInteger totalSent = new AtomicInteger(0);

    /** 统计：队列空跳过次数（underrun 计数） */
    private final AtomicInteger underrunCount = new AtomicInteger(0);

    /** 统计：推送开始时间 */
    private final AtomicLong pushStartTime = new AtomicLong(0);

    /** 当前推送的总块数（用于完成判定） */
    private final AtomicInteger totalChunksExpected = new AtomicInteger(0);

    /** 当前推送的会话标识（用于日志） */
    private volatile String currentPushId = null;

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

        // 防止并发推送
        if (!pushing.compareAndSet(false, true)) {
            result.put("success", false);
            result.put("message", "已有推送任务正在进行中，请等待完成");
            return result;
        }

        String pushId = java.util.UUID.randomUUID().toString().substring(0, 8);
        currentPushId = pushId;

        // 异步下载 + 填充队列
        downloadExecutor.submit(() -> loadAndEnqueue(url, pushId, onlineCount));

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
     * 【生产者】下载音频 → 分块 → 填充队列。
     * 队列满时 offer 超时等待（背压），超时过久则放弃本次推送。
     */
    private void loadAndEnqueue(String url, String pushId, int deviceCount) {
        try {
            // 1. 下载音频
            byte[] audioData = restTemplate.getForObject(url, byte[].class);
            if (audioData == null || audioData.length == 0) {
                log.warn("[push:{}] 音频文件为空或下载失败", pushId);
                pushing.set(false);
                return;
            }

            int totalChunks = (audioData.length + CHUNK_SIZE - 1) / CHUNK_SIZE;
            totalChunksExpected.set(totalChunks);
            totalSent.set(0);
            underrunCount.set(0);
            pushStartTime.set(System.currentTimeMillis());

            log.info("[push:{}] 下载完成, 大小={} bytes, 共{}块, 开始填充队列(容量={})",
                    pushId, audioData.length, totalChunks, QUEUE_CAPACITY);

            // 2. 分块入队
            for (int i = 0; i < totalChunks; i++) {
                int offset = i * CHUNK_SIZE;
                int length = Math.min(CHUNK_SIZE, audioData.length - offset);
                byte[] chunk = new byte[length];
                System.arraycopy(audioData, offset, chunk, 0, length);

                // 背压：队列满时阻塞等待，直到超时
                boolean offered = chunkQueue.offer(chunk, ENQUEUE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                if (!offered) {
                    log.warn("[push:{}] 队列持续满超过{}ms，放弃剩余{}块",
                            pushId, ENQUEUE_TIMEOUT_MS, totalChunks - i);
                    break;
                }

                // 周期性打印队列状态
                if ((i + 1) % 50 == 0) {
                    log.debug("[push:{}] 已入队{}/{}块, 队列深度={}/{}",
                            pushId, i + 1, totalChunks, chunkQueue.size(), QUEUE_CAPACITY);
                }
            }

            log.info("[push:{}] 全部块已入队, 共{}块, 等待发送线程消费完毕", pushId, totalChunks);

        } catch (Exception e) {
            log.error("[push:{}] 音频加载失败", pushId, e);
            pushing.set(false);
        }
    }

    /**
     * 【消费者】定时发送线程的核心方法，每 50ms 调用一次。
     * 从队列 poll 一块音频数据并发送给所有设备。
     * 队列空时跳过（计入 underrun），不阻塞定时线程。
     */
    private void sendNextChunk() {
        // 队列为空时直接跳过
        byte[] chunk = chunkQueue.poll();
        if (chunk == null) {
            // 如果不在推送中，静默跳过；如果正在推送但队列空，计为 underrun
            if (pushing.get()) {
                underrunCount.incrementAndGet();
                // 检查是否所有块都已发完
                if (totalSent.get() >= totalChunksExpected.get()) {
                    finishPush();
                }
            }
            return;
        }

        try {
            int sentTo = audioWebSocketHandler.sendAudioToAll(chunk);
            int sent = totalSent.incrementAndGet();

            if (sentTo == 0 && pushing.get()) {
                log.warn("[push:{}] 无可用设备, 已发送{}/{}块",
                        currentPushId, sent, totalChunksExpected.get());
                // 清空队列，结束推送
                chunkQueue.clear();
                finishPush();
                return;
            }

            // 所有块发送完毕
            if (sent >= totalChunksExpected.get()) {
                finishPush();
            }
        } catch (Exception e) {
            log.error("[push:{}] 发送音频块失败", currentPushId, e);
        }
    }

    /**
     * 推送完成，打印统计信息并重置状态。
     */
    private void finishPush() {
        if (!pushing.compareAndSet(true, false)) {
            return; // 防止重复触发
        }
        long elapsed = System.currentTimeMillis() - pushStartTime.get();
        int expected = totalChunksExpected.get();
        int sent = totalSent.get();
        int underruns = underrunCount.get();
        long theoretical = (long) expected * CHUNK_INTERVAL_MS;

        log.info("[push:{}] 推送完成, 发送{}/{}块, 耗时{}ms (理论{}ms), underrun次数={}, 队列剩余={}",
                currentPushId, sent, expected, elapsed, theoretical, underruns, chunkQueue.size());

        if (underruns > 0) {
            log.warn("[push:{}] 出现{}次underrun，考虑增大队列容量或检查网络延迟", currentPushId, underruns);
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
