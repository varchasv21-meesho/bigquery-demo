package com.example.bigquerydemo.controller;

import com.example.bigquerydemo.service.BigQueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/bigquery")
public class BigQueryController {

    @Autowired
    private BigQueryService bigQueryService;


    @RequestMapping(value = "/createDataset", method = RequestMethod.POST)
    public String createDataset(@RequestParam String dataset) {
        bigQueryService.createDataset(dataset);
        return "Dataset created successfully";
    }

    @RequestMapping(value = "/createTable", method = RequestMethod.POST)
    public String createTable(@RequestParam String dataset, @RequestParam String table, @RequestBody Map<String, String> schema) {
        bigQueryService.createTable(dataset, table, schema);
        return "Table created successfully";
    }

    @RequestMapping(value = "/insert", method = RequestMethod.POST)
    public String insertData(@RequestParam String dataset, @RequestParam String table, @RequestBody Map<String, Object> data) {
        bigQueryService.insertData(dataset, table, data);
        return "Data inserted successfully";
    }

    @RequestMapping(value = "/insertList", method = RequestMethod.POST)
    public String insertListData(@RequestParam String dataset, @RequestParam String table, @RequestBody List<Map<String, Object>> data) {
        return bigQueryService.insertListData(dataset, table, data);
    }

    @RequestMapping(value = "/batchInsert", method = RequestMethod.POST)
    public String batchInsertData(@RequestParam String dataset, @RequestParam String table, @RequestParam String filePath) throws InterruptedException {
        return bigQueryService.batchInsertData(dataset, table, filePath);
    }

    @RequestMapping(value = "/insertLocal", method = RequestMethod.POST)
    public String insertLocalData(@RequestParam String dataset, @RequestParam String table, @RequestParam String filePath) {
        return bigQueryService.insertDataFromLocalFile(dataset, table, filePath);
    }
}
