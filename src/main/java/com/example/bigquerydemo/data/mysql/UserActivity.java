package com.example.bigquerydemo.data.mysql;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Entity
@Table(name = "user_activity")
@AllArgsConstructor
@NoArgsConstructor
public class UserActivity {

    @Id
    @Column(name = "id")
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "user_id")
    private Long userId;

    @Column(name = "activity_date")
    private String activityDate;

    @Column(name = "activity_type")
    private String activityType;

    @Column(name = "duration_minutes")
    private Integer durationMinutes;
}
