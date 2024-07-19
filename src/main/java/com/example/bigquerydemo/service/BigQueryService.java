package com.example.bigquerydemo.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.*;
import org.springframework.stereotype.Service;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class BigQueryService {

    private final BigQuery bigQuery;
    private final ObjectMapper jacksonObjectMapper;

    public BigQueryService(ObjectMapper jacksonObjectMapper) {
        bigQuery = BigQueryOptions.getDefaultInstance().getService();
        this.jacksonObjectMapper = jacksonObjectMapper;
    }

    public void createDataset(String datasetName) {
        DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetName).build();
        bigQuery.create(datasetInfo);
    }

    public void createTable(String datasetName, String tableName, Map<String, String> schema) {

        TableId tableId = TableId.of(datasetName, tableName);
        Schema schemaObj = Schema.of(schema.entrySet().stream()
                .map(entry -> Field.of(entry.getKey(), LegacySQLTypeName.valueOf(entry.getValue())))
                .collect(Collectors.toList()));
        TableDefinition tableDefinition = StandardTableDefinition.of(schemaObj);
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        bigQuery.create(tableInfo);
    }

    public void insertData(String datasetName, String tableName, Map<String, Object> data) {
        try {
            TableId tableId = TableId.of(datasetName, tableName);
            InsertAllRequest.Builder builder = InsertAllRequest.newBuilder(tableId);
            InsertAllRequest request = builder.addRow(data).build();
            InsertAllResponse response = bigQuery.insertAll(request);
            if (response.hasErrors()) {
                response.getInsertErrors().forEach((key, value) -> {
                    System.out.println("Error occurred while inserting data: " + key + " : " + value);
                });
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String insertListData(String datasetName, String tableName, List<Map<String, Object>> data) {
        try {
            TableId tableId = TableId.of(datasetName, tableName);
            InsertAllRequest.Builder builder = InsertAllRequest.newBuilder(tableId);
            data.forEach(builder::addRow);
            InsertAllRequest request = builder.build();
            InsertAllResponse response = bigQuery.insertAll(request);
            if (response.hasErrors()) {
                response.getInsertErrors().forEach((key, value) -> {
                    System.out.println("Error occurred while inserting data: " + key + " : " + value);
                });
                return "Error occurred while inserting data";
            }
            return "Data inserted successfully";
        } catch (BigQueryException e) {
            throw new BigQueryException(e.getCode(),"Error occurred while inserting data: " + e.getMessage());
        }
    }

    public String batchInsertData(String datasetName, String tableName, String filePath) {

        try {
            LoadJobConfiguration loadConfig = LoadJobConfiguration.newBuilder(TableId.of(datasetName, tableName), filePath)
                    .setFormatOptions(FormatOptions.json())
                    .build();
            Job job = bigQuery.create(JobInfo.of(JobId.of(), loadConfig));
            // Wait for the job to complete
            job = job.waitFor();
            if (job.isDone() && job.getStatus().getError() == null) {
                return "Data inserted successfully";
            } else if (job.getStatus().getError() != null) {
                // If the job completed with errors, return the error message
                return "Error occurred while inserting data: " + job.getStatus().getError().toString();
            } else {
                // If the job did not complete, handle it accordingly
                return "Job did not complete successfully.";
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to insert data: " + e.getMessage(), e);
        }
    }

    public String insertDataFromLocalFile(String datasetName, String tableName, String localFilePath) {
        try {
            // Read the local JSON file
            String jsonContent = new String(Files.readAllBytes(Paths.get(localFilePath)));
            List<Map<String, Object>> dataList = jacksonObjectMapper.readValue(jsonContent, new TypeReference<List<Map<String, Object>>>() {});

            // Use the existing insertListData method to insert the data
            return insertListData(datasetName, tableName, dataList);
        } catch (Exception e) {
            throw new RuntimeException("Failed to insert data from local file: " + e.getMessage(), e);
        }
    }
}
