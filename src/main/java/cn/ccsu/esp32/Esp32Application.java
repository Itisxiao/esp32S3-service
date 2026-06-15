package cn.ccsu.esp32;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author 潇洒哥queen
 * @date 2026/6/15 14:10
 */
@SpringBootApplication(
        scanBasePackages = {"cn.ccsu.esp32"}
)
public class Esp32Application {
    public static void main(String[] args) {
        SpringApplication.run(Esp32Application.class);
    }
}
