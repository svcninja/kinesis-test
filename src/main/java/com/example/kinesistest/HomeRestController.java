package com.example.kinesistest;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;

@RestController
public class HomeRestController {
   
    @GetMapping("/hello")
    public String getGreeting() {
//        personRepository.insertPerson();
        KinesisCollector.builder().appName("kinesis-test").streamName("test").credentialsProvider(new DefaultAWSCredentialsProviderChain()).build().start();
        return "Hello Spring World!";
    }

}