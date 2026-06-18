package cn.ccsu.esp32.controller;

import cn.ccsu.esp32.handler.AudioWebSocketHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;

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

    /** 每次发送的音频数据块大小 (bytes) */
    private static final int CHUNK_SIZE = 1600;

    /** 每块之间的发送间隔 (ms)，模拟实时播放速率 */
    private static final long CHUNK_INTERVAL_MS = 50;

    /**
     * 从MinIO下载音频并推送给所有已连接的ESP32-S3设备
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

        try {
            // 从MinIO下载音频数据
            byte[] audioData = restTemplate.getForObject(url, byte[].class);
            if (audioData == null || audioData.length == 0) {
                result.put("success", false);
                result.put("message", "音频文件为空或下载失败");
                return result;
            }

            log.info("音频下载完成, 大小={} bytes, 开始分块推送给{}台设备", audioData.length, onlineCount);

            // 分块发送音频数据，避免一次性发送过大导致缓冲区溢出
            int totalChunks = (audioData.length + CHUNK_SIZE - 1) / CHUNK_SIZE;
            int sentChunks = 0;

            for (int offset = 0; offset < audioData.length; offset += CHUNK_SIZE) {
                int length = Math.min(CHUNK_SIZE, audioData.length - offset);
                byte[] chunk = new byte[length];
                System.arraycopy(audioData, offset, chunk, 0, length);

                int sentTo = audioWebSocketHandler.sendAudioToAll(chunk);
                sentChunks++;

                if (sentTo == 0) {
                    log.warn("发送中断，无可用设备, 已发送{}/{}块", sentChunks, totalChunks);
                    break;
                }

                // 控制发送速率
                Thread.sleep(CHUNK_INTERVAL_MS);
            }

            log.info("音频推送完成, 共发送{}/{}块", sentChunks, totalChunks);

            result.put("success", true);
            result.put("message", "音频推送完成");
            result.put("audioSize", audioData.length);
            result.put("totalChunks", totalChunks);
            result.put("sentChunks", sentChunks);
            result.put("deviceCount", onlineCount);

        } catch (Exception e) {
            log.error("音频推送失败", e);
            result.put("success", false);
            result.put("message", "推送失败: " + e.getMessage());
        }

        return result;
    }
}
