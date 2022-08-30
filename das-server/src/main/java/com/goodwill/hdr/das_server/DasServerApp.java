package com.goodwill.hdr.das_server;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.io.IOException;

@SpringBootApplication
public class DasServerApp {
    public static void main(String[] args) {
        SpringApplication.run(DasServerApp.class,args);
    }


    @Bean(destroyMethod = "close")
    public Connection getConnection() {
        try {
            return ConnectionFactory.createConnection();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
