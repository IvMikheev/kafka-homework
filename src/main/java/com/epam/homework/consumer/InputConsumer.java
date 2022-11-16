package com.epam.homework.consumer;

import com.epam.homework.model.VehicleDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.util.SloppyMath;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class InputConsumer {

    @Value("${kafka-topics.output.name}")
    private String outputTopic;

    private final KafkaTemplate<String, Double> kafkaTemplate;
    private final Map<String, Pair<Double, Double>> vehicleIdToCoordinates = new HashMap<>();
    private final Map<String, Double> vehicleIdToTotalDistance = new HashMap<>();

    @KafkaListener(topics = "${kafka-topics.input.name}", groupId = "input")
    public void firstConsumer(VehicleDTO vehicle) {
        log.info("First consumer consumed vehicle with id={}, latitude={}, longitude={}",
                vehicle.getId(), vehicle.getLatitude(), vehicle.getLongitude()
        );

        double newDistance = calculateDistance(vehicle.getId(), vehicle.getLatitude(), vehicle.getLongitude());
        storeData(vehicle.getId(), Pair.of(vehicle.getLatitude(), vehicle.getLongitude()), newDistance);
        kafkaTemplate.send(outputTopic, vehicle.getId(), newDistance);
    }

    @KafkaListener(topics = "${kafka-topics.input.name}", groupId = "input")
    public void secondConsumer(VehicleDTO vehicle) {
        log.info("Second consumer consumed vehicle with id={}, latitude={}, longitude={}",
                vehicle.getId(), vehicle.getLatitude(), vehicle.getLongitude()
        );

        double newDistance = calculateDistance(vehicle.getId(), vehicle.getLatitude(), vehicle.getLongitude());
        storeData(vehicle.getId(), Pair.of(vehicle.getLatitude(), vehicle.getLongitude()), newDistance);
        kafkaTemplate.send(outputTopic, vehicle.getId(), newDistance);
    }

    @KafkaListener(topics = "${kafka-topics.input.name}", groupId = "input")
    public void thirdConsumer(VehicleDTO vehicle) {
        log.info("Third consumer consumed vehicle with id={}, latitude={}, longitude={}",
                vehicle.getId(), vehicle.getLatitude(), vehicle.getLongitude()
        );

        double newDistance = calculateDistance(vehicle.getId(), vehicle.getLatitude(), vehicle.getLongitude());
        storeData(vehicle.getId(), Pair.of(vehicle.getLatitude(), vehicle.getLongitude()), newDistance);
        kafkaTemplate.send(outputTopic, vehicle.getId(), newDistance);
    }

    private double calculateDistance(String vehicleId, double newLatitude, double newLongitude) {
        Pair<Double, Double> oldCoordinates = vehicleIdToCoordinates.get(vehicleId);

        if (oldCoordinates == null) {
            return 0d;
        }

        double newDistance = SloppyMath.haversinMeters(
                oldCoordinates.getLeft(),
                oldCoordinates.getRight(),
                newLatitude,
                newLongitude
        );

        double oldDistance = vehicleIdToTotalDistance.get(vehicleId);

        return BigDecimal
                .valueOf(newDistance)
                .add(BigDecimal.valueOf(oldDistance))
                .setScale(2, RoundingMode.HALF_UP)
                .doubleValue();
    }

    private void storeData(String vehicleId, Pair<Double, Double> coordinates, double totalDistance) {
        vehicleIdToCoordinates.put(vehicleId, coordinates);
        vehicleIdToTotalDistance.put(vehicleId, totalDistance);
    }
}
