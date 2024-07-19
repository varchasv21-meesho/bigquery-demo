package com.example.bigquerydemo.db.repository;

import com.example.bigquerydemo.data.mysql.UserActivity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface UserActivityRepository extends JpaRepository<UserActivity, Long> {
}
