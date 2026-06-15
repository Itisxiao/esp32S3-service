package cn.ccsu.esp32.config;

import cn.ccsu.esp32.handler.AudioWebSocketHandler;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

import javax.annotation.Resource;

/**
 * WebSocket配置类
 * @author 潇洒哥queen
 * @date 2026/6/15
 */
@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    @Resource
    private AudioWebSocketHandler audioWebSocketHandler;

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        // /ws/audio 为ESP32-S3连接的WebSocket端点，允许跨域
        registry.addHandler(audioWebSocketHandler, "/ws/audio")
                .setAllowedOrigins("*");
    }
}
