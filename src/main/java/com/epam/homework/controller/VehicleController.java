package com.epam.homework.controller;

import com.epam.homework.model.VehicleDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.lang.NonNull;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.stream.Stream;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/vehicle")
public class VehicleController {

    private static final double MIN_LATITUDE = -90d;
    private static final double MAX_LATITUDE = 90d;
    private static final double MIN_LONGITUDE = -180d;
    private static final double MAX_LONGITUDE = 180d;

    @Value("${kafka-topics.input.name}")
    private String inputTopic;

    private final KafkaTemplate<String, VehicleDTO> kafkaTemplate;

    @PostMapping
    public void accept(
            @RequestParam @NonNull String id,
            @RequestParam @NonNull Double latitude,
            @RequestParam @NonNull Double longitude
    ) {
        //validation
        boolean validationFailed = Stream.of(id.isBlank(),
                        latitude < MIN_LATITUDE, latitude > MAX_LATITUDE,
                        longitude < MIN_LONGITUDE, longitude > MAX_LONGITUDE)
                .anyMatch(Boolean::booleanValue);

        if (validationFailed) {
            throw new RuntimeException("Validation failed");
        }
        log.info("Accepted vehicle with: id={}, latitude={}, longitude={}", id, latitude, longitude);

        VehicleDTO vehicle = new VehicleDTO(id, latitude, longitude);
        kafkaTemplate.send(inputTopic, id, vehicle);
    }
}
