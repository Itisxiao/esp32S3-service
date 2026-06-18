package cn.ccsu.esp32.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 音频数据服务 - 接收并保存ESP32-S3传来的音频数据
 * @author 潇洒哥queen
 * @date 2026/6/15
 */
@Slf4j
@Service
public class AudioService {

    /** 音频保存目录 */
    private static final String AUDIO_DIR = "audio_recordings";

    /** 每个会话对应的文件输出流 */
    private final Map<String, FileOutputStream> sessionStreams = new ConcurrentHashMap<>();

    /** 每个会话对应的文件名 */
    private final Map<String, String> sessionFileNames = new ConcurrentHashMap<>();

    /** 已显式停止录音的会话集合，防止残余数据触发自动重启 */
    private final Set<String> stoppedSessions = ConcurrentHashMap.newKeySet();

    /** 正在录音的会话集合 */
    private final Set<String> recordingSessions = ConcurrentHashMap.newKeySet();

    /**
     * 开始录音 - 为会话创建音频文件
     * @param sessionId WebSocket会话ID
     */
    public void startRecording(String sessionId) {
        stoppedSessions.remove(sessionId);
        recordingSessions.add(sessionId);
        try {
            File dir = new File(AUDIO_DIR);
            if (!dir.exists()) {
                dir.mkdirs();
            }
            String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
            String fileName = "esp32_audio_" + sessionId.substring(0, 6) + "_" + timestamp + ".pcm";
            File file = new File(dir, fileName);
            FileOutputStream fos = new FileOutputStream(file);
            sessionStreams.put(sessionId, fos);
            sessionFileNames.put(sessionId, file.getAbsolutePath());
            log.info("开始录音, sessionId={}, 文件={}", sessionId, file.getAbsolutePath());
        } catch (IOException e) {
            log.error("创建音频文件失败, sessionId={}", sessionId, e);
        }
    }

    /**
     * 写入音频数据
     * @param sessionId WebSocket会话ID
     * @param audioData 音频二进制数据
     */
    public void writeAudioData(String sessionId, byte[] audioData) {
        if (!recordingSessions.contains(sessionId)) {
            log.debug("会话未在录音中，丢弃音频数据, sessionId={}, 数据大小={} bytes", sessionId, audioData.length);
            return;
        }
        FileOutputStream fos = sessionStreams.get(sessionId);
        if (fos != null) {
            try {
                fos.write(audioData);
                fos.flush();
            } catch (IOException e) {
                log.error("写入音频数据失败, sessionId={}", sessionId, e);
            }
        } else {
            log.warn("录音流已关闭但会话仍在录音中, sessionId={}", sessionId);
        }
    }

    /**
     * 停止录音 - 关闭文件输出流
     * @param sessionId WebSocket会话ID
     * @return 录音文件路径，如果没有则返回null
     */
    public String stopRecording(String sessionId) {
        recordingSessions.remove(sessionId);
        stoppedSessions.add(sessionId);
        String filePath = sessionFileNames.remove(sessionId);
        FileOutputStream fos = sessionStreams.remove(sessionId);
        if (fos != null) {
            try {
                fos.close();
                log.info("停止录音, sessionId={}, 文件={}", sessionId, filePath);
            } catch (IOException e) {
                log.error("关闭音频文件失败, sessionId={}", sessionId, e);
            }
        }
        return filePath;
    }

    /**
     * 获取当前录音文件路径
     */
    public String getRecordingPath(String sessionId) {
        return sessionFileNames.get(sessionId);
    }
}
