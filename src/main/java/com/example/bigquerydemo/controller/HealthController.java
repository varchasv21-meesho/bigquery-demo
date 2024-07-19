package com.example.bigquerydemo.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/health")
public class HealthController {
        @RequestMapping(value = "/ping", produces = "text/plain", method = RequestMethod.GET)
        public String ping() {
            return "pong";
        }
}
