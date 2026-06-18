package cn.ccsu.esp32.handler;

import cn.ccsu.esp32.service.AudioService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.BinaryMessage;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.AbstractWebSocketHandler;

import javax.annotation.Resource;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        String sessionId = session.getId();
        sessions.put(sessionId, session);
        log.info("ESP32设备已连接, sessionId={}, 当前在线设备数={}", sessionId, sessions.size());
    }

    @Override
    protected void handleBinaryMessage(WebSocketSession session, BinaryMessage message) {
        byte[] audioData = message.getPayload().array();
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
                session.sendMessage(new TextMessage("{\"status\":\"recording\"}"));
            } else if (payload.contains("\"stop\"")) {
                log.info("执行停止录音, sessionId={}", sessionId);
                String filePath = audioService.stopRecording(sessionId);
                log.info("停止录音完成, sessionId={}, filePath={}", sessionId, filePath);
                String response = filePath != null
                        ? "{\"status\":\"stopped\",\"file\":\"" + filePath + "\"}"
                        : "{\"status\":\"stopped\",\"file\":null}";
                session.sendMessage(new TextMessage(response));
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
        String filePath = audioService.stopRecording(sessionId);
        log.info("ESP32设备已断开, sessionId={}, 状态={}, 录音文件={}, 当前在线设备数={}",
                sessionId, status, filePath, sessions.size());
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) {
        String sessionId = session.getId();
        log.error("WebSocket传输错误, sessionId={}", sessionId, exception);
        sessions.remove(sessionId);
        audioService.stopRecording(sessionId);
    }

    /**
     * 获取当前在线设备数量
     */
    public int getOnlineCount() {
        return sessions.size();
    }

    /**
     * 向所有已连接的ESP32设备发送音频数据
     * @param audioData 音频二进制数据
     * @return 成功发送的设备数量
     */
    public int sendAudioToAll(byte[] audioData) {
        int count = 0;
        BinaryMessage binaryMessage = new BinaryMessage(audioData);
        for (Map.Entry<String, WebSocketSession> entry : sessions.entrySet()) {
            WebSocketSession session = entry.getValue();
            if (session.isOpen()) {
                try {
                    session.sendMessage(binaryMessage);
                    count++;
                    log.debug("发送音频数据到设备, sessionId={}, 数据大小={} bytes", entry.getKey(), audioData.length);
                } catch (Exception e) {
                    log.error("发送音频数据失败, sessionId={}", entry.getKey(), e);
                }
            }
        }
        return count;
    }

    /**
     * 向所有已连接的ESP32设备发送文本消息
     * @param text 文本内容
     */
    public void sendTextToAll(String text) {
        TextMessage textMessage = new TextMessage(text);
        for (Map.Entry<String, WebSocketSession> entry : sessions.entrySet()) {
            WebSocketSession session = entry.getValue();
            if (session.isOpen()) {
                try {
                    session.sendMessage(textMessage);
                } catch (Exception e) {
                    log.error("发送文本消息失败, sessionId={}", entry.getKey(), e);
                }
            }
        }
    }

    /**
     * 获取所有在线会话ID
     */
    public java.util.Set<String> getOnlineSessionIds() {
        return sessions.keySet();
    }
}
