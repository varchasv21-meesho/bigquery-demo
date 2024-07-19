package com.example.bigquerydemo.db.dao;

import com.example.bigquerydemo.data.mysql.UserActivity;
import com.example.bigquerydemo.db.repository.UserActivityRepository;
import com.example.bigquerydemo.service.BigQueryService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class UserActivityDao {
    @Autowired
    private UserActivityRepository userActivityRepository;
    @Autowired
    private BigQueryService bigQueryService;
    @Autowired
    private ObjectMapper objectMapper;
    public void save(UserActivity userActivity){
        userActivityRepository.save(userActivity);
    }

    public UserActivity findById(Long id){
        return userActivityRepository.findById(id).orElse(null);
    }

    public void saveAll(Iterable<UserActivity> userActivities){
        userActivityRepository.saveAll(userActivities);
    }

    public void saveUserActivityToBigQuery(UserActivity userActivity) {
        Map<String, Object> data = objectMapper.convertValue(userActivity, new TypeReference<>() {});
        bigQueryService.insertData("my_new_dataset", "user_activity", data);
    }

    public void saveUserActivitiesToBigQuery(List<UserActivity> userActivities) {
        List<Map<String, Object>> data = objectMapper.convertValue(userActivities, new TypeReference<>() {});
        bigQueryService.insertListData("my_new_dataset", "user_activity", data);
    }

    public List<UserActivity> findAll() {
        return userActivityRepository.findAll();
    }
}
