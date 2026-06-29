package cn.ccsu.esp32.handler;

import cn.ccsu.esp32.service.AudioService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.BinaryMessage;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.AbstractWebSocketHandler;

import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.*;

/**
 * 音频WebSocket处理器 - 处理ESP32-S3的WebSocket连接和音频数据
 *
 * 协议说明:
 * - ESP32发送二进制消息为音频PCM数据
 * - ESP32发送文本消息为控制指令:
 *   {"action":"start"} - 开始录音
 *   {"action":"stop"}  - 停止录音
 *
 * @author 潇洒哥queen
 * @date 2026/6/15
 */
@Slf4j
@Component
public class AudioWebSocketHandler extends AbstractWebSocketHandler {

    @Resource
    private AudioService audioService;

    /** 在线会话集合 */
    private final Map<String, WebSocketSession> sessions = new ConcurrentHashMap<>();

    /** 每个设备独立的发送器（sessionId → DeviceSender） */
    private final Map<String, DeviceSender> deviceSenders = new ConcurrentHashMap<>();

    /** 设备独立发送线程池 */
    private final ExecutorService deviceSendExecutor = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "device-sender");
        t.setDaemon(true);
        return t;
    });

    // ==================== 设备独立发送器 ====================

    /**
     * 每个设备独立的发送器：拥有自己的发送队列和发送线程。
     * 设备间发送互不阻塞，某个设备网络卡顿不会影响其他设备的 50ms 定时节奏。
     */
    private static class DeviceSender {
        private static final int SEND_QUEUE_CAPACITY = 10;

        private final String sessionId;
        private final WebSocketSession session;
        private final LinkedBlockingQueue<WebSocketMessage<?>> sendQueue = new LinkedBlockingQueue<>(SEND_QUEUE_CAPACITY);
        private volatile boolean running = true;

        DeviceSender(String sessionId, WebSocketSession session) {
            this.sessionId = sessionId;
            this.session = session;
        }

        /**
         * 非阻塞入队：队列满时丢弃最旧消息（offer 失败则 poll 头部再重试）。
         * 确保调用线程（50ms 定时器）绝不阻塞。
         */
        void enqueue(WebSocketMessage<?> message) {
            if (!running) return;
            if (!sendQueue.offer(message)) {
                sendQueue.poll(); // 丢弃最旧的一条消息
                sendQueue.offer(message);
            }
        }

        /**
         * 发送循环：从队列 poll 并通过 WebSocket 发送给设备。
         * 队列空时短暂等待，不忙等。
         */
        void sendLoop() {
            while (running && session.isOpen()) {
                try {
                    WebSocketMessage<?> message = sendQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (message != null) {
                        session.sendMessage(message);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    if (running) {
                        log.error("[device:{}] 发送WebSocket消息失败", sessionId, e);
                    }
                    break;
                }
            }
            log.debug("[device:{}] 发送循环结束", sessionId);
        }

        void stop() {
            running = false;
            sendQueue.clear();
        }
    }

    // ==================== WebSocket 生命周期 ====================

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        String sessionId = session.getId();
        sessions.put(sessionId, session);

        // 为该设备创建独立发送器并启动发送线程
        DeviceSender sender = new DeviceSender(sessionId, session);
        deviceSenders.put(sessionId, sender);
        deviceSendExecutor.submit(sender::sendLoop);

        log.info("ESP32设备已连接, sessionId={}, 当前在线设备数={}", sessionId, sessions.size());
    }

    @Override
    protected void handleBinaryMessage(WebSocketSession session, BinaryMessage message) {
        ByteBuffer payload = message.getPayload();
        byte[] audioData = new byte[payload.remaining()];
        payload.get(audioData);
        String sessionId = session.getId();
        audioService.writeAudioData(sessionId, audioData);
        log.debug("收到音频数据, sessionId={}, 数据大小={} bytes", sessionId, audioData.length);
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        String payload = message.getPayload();
        String sessionId = session.getId();
        log.info("收到控制指令, sessionId={}, 内容={}", sessionId, payload);

        try {
            if (payload.contains("\"start\"")) {
                log.info("执行开始录音, sessionId={}", sessionId);
                audioService.startRecording(sessionId);
                sendTextToSession(sessionId, "{\"status\":\"recording\"}");
            } else if (payload.contains("\"stop\"")) {
                log.info("执行停止录音, sessionId={}", sessionId);
                String filePath = audioService.stopRecording(sessionId);
                log.info("停止录音完成, sessionId={}, filePath={}", sessionId, filePath);
                String response = filePath != null
                        ? "{\"status\":\"stopped\",\"file\":\"" + filePath + "\"}"
                        : "{\"status\":\"stopped\",\"file\":null}";
                sendTextToSession(sessionId, response);
            } else {
                log.warn("未识别的控制指令, sessionId={}, payload={}", sessionId, payload);
            }
        } catch (Exception e) {
            log.error("处理控制指令异常, sessionId={}, payload={}", sessionId, payload, e);
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        String sessionId = session.getId();
        sessions.remove(sessionId);

        // 停止并移除该设备的独立发送器
        DeviceSender sender = deviceSenders.remove(sessionId);
        if (sender != null) {
            sender.stop();
        }

        String filePath = audioService.stopRecording(sessionId);
        log.info("ESP32设备已断开, sessionId={}, 状态={}, 录音文件={}, 当前在线设备数={}",
                sessionId, status, filePath, sessions.size());
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) {
        String sessionId = session.getId();
        log.error("WebSocket传输错误, sessionId={}", sessionId, exception);
        sessions.remove(sessionId);

        DeviceSender sender = deviceSenders.remove(sessionId);
        if (sender != null) {
            sender.stop();
        }

        audioService.stopRecording(sessionId);
    }

    // ==================== 公开方法 ====================

    /**
     * 获取当前在线设备数量
     */
    public int getOnlineCount() {
        return sessions.size();
    }

    /**
     * 向所有已连接的ESP32设备发送音频数据（非阻塞 fan-out）。
     * 将数据块放入每个设备的独立发送队列后立即返回，不等待实际发送完成。
     *
     * @param audioData 音频二进制数据
     * @return 成功入队的设备数量
     */
    public int sendAudioToAll(byte[] audioData) {
        int count = 0;
        for (Map.Entry<String, DeviceSender> entry : deviceSenders.entrySet()) {
            DeviceSender sender = entry.getValue();
            if (sender.running && sender.session.isOpen()) {
                sender.enqueue(new BinaryMessage(audioData));
                count++;
            }
        }
        return count;
    }

    /**
     * 向所有已连接的ESP32设备发送文本消息
     * @param text 文本内容
     */
    public void sendTextToAll(String text) {
        for (Map.Entry<String, DeviceSender> entry : deviceSenders.entrySet()) {
            DeviceSender sender = entry.getValue();
            if (sender.running && sender.session.isOpen()) {
                sender.enqueue(new TextMessage(text));
            }
        }
    }

    /**
     * 向指定会话发送文本消息，统一进入设备发送队列，避免并发写同一个 WebSocketSession。
     */
    private void sendTextToSession(String sessionId, String text) {
        DeviceSender sender = deviceSenders.get(sessionId);
        if (sender == null || !sender.running || !sender.session.isOpen()) {
            log.warn("发送文本消息失败，会话不可用, sessionId={}", sessionId);
            return;
        }
        sender.enqueue(new TextMessage(text));
    }

    /**
     * 获取所有在线会话ID
     */
    public java.util.Set<String> getOnlineSessionIds() {
        return sessions.keySet();
    }

    @PreDestroy
    public void shutdown() {
        log.info("正在关闭所有设备发送器...");
        deviceSenders.values().forEach(DeviceSender::stop);
        deviceSenders.clear();
        deviceSendExecutor.shutdownNow();
    }
}
