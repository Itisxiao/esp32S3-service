package cn.ccsu.esp32.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpMethod;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.ResponseExtractor;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * TTS文本转语音控制器 - 调用本地edge-tts服务将文本转为音频文件并保存
 * @author 潇洒哥queen
 * @date 2026/6/22
 */
@Slf4j
@RestController
@RequestMapping("/api/tts")
public class TtsController {

    @Value("${tts.server-url}")
    private String ttsServerUrl;

    private final RestTemplate restTemplate = new RestTemplate();

    /** 音频保存目录 */
    private static final String TTS_AUDIO_DIR = "tts_audio";

    /** 流式读写缓冲区大小 8KB */
    private static final int BUFFER_SIZE = 8192;

    /**
     * 文本转语音接口
     * 调用本地edge-tts服务将文本转为音频，保存为MP3文件并返回文件信息
     *
     * @param text   需要转换的文本内容
     * @param voice  音色，默认 zh-CN-XiaoxiaoNeural
     * @param rate   语速调整，默认 +0%，如 +20%, -10%
     * @param volume 音量调整，默认 +0%，如 +50%
     * @return 文件保存信息（路径、大小等）
     */
    @GetMapping("/speech")
    public Map<String, Object> textToSpeech(
            @RequestParam String text,
            @RequestParam(defaultValue = "zh-CN-XiaoxiaoNeural") String voice,
            @RequestParam(defaultValue = "+0%") String rate,
            @RequestParam(defaultValue = "+0%") String volume
    ) {
        Map<String, Object> result = new HashMap<>();
        log.info("收到TTS请求, text长度={}, voice={}, rate={}, volume={}",
                text.length(), voice, rate, volume);

        try {
            // 1. 构建GET请求URL，拼接查询参数
            String ttsUrl = UriComponentsBuilder.fromHttpUrl(ttsServerUrl + "/api/tts")
                    .queryParam("text", text)
                    .queryParam("voice", voice)
                    .queryParam("rate", rate)
                    .queryParam("volume", volume)
                    .build()
                    .toUriString();

            log.info("调用edge-tts服务: {}", ttsUrl);

            // 2. 准备目标文件
            File dir = new File(TTS_AUDIO_DIR);
            if (!dir.exists()) {
                dir.mkdirs();
            }
            String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
            String fileName = "tts_" + timestamp + "_" + voice + ".mp3";
            File audioFile = new File(dir, fileName);

            // 3. 流式下载：边接收边写文件，音频数据不会完整驻留内存
            long bytesWritten = restTemplate.execute(ttsUrl, HttpMethod.GET, null,
                    (ResponseExtractor<Long>) response -> {
                        long contentLength = response.getHeaders().getContentLength();
                        log.info("TTS服务响应已到达, Content-Length={} bytes, 开始流式写入", contentLength);

                        try (InputStream in = new BufferedInputStream(response.getBody(), BUFFER_SIZE);
                             FileOutputStream fos = new FileOutputStream(audioFile);
                             BufferedOutputStream bos = new BufferedOutputStream(fos, BUFFER_SIZE)) {

                            byte[] buffer = new byte[BUFFER_SIZE];
                            long total = 0;
                            int bytesRead;
                            while ((bytesRead = in.read(buffer)) != -1) {
                                bos.write(buffer, 0, bytesRead);
                                total += bytesRead;
                            }
                            bos.flush();
                            log.info("流式写入完成, 共{} bytes", total);
                            return total;
                        }
                    });

            if (bytesWritten <= 0) {
                result.put("success", false);
                result.put("message", "TTS服务返回了空音频");
                return result;
            }

            String absolutePath = audioFile.getAbsolutePath();
            log.info("音频文件已保存: {}, 大小={} bytes", absolutePath, bytesWritten);

            // 4. 返回结果
            result.put("success", true);
            result.put("message", "TTS转换成功");
            result.put("filePath", absolutePath);
            result.put("fileName", fileName);
            result.put("fileSizeBytes", bytesWritten);
            result.put("format", "mp3");
            result.put("voice", voice);
            result.put("rate", rate);
            result.put("volume", volume);
            result.put("textLength", text.length());

        } catch (Exception e) {
            log.error("TTS转换失败", e);
            result.put("success", false);
            result.put("message", "TTS转换失败: " + e.getMessage());
        }

        return result;
    }
}
