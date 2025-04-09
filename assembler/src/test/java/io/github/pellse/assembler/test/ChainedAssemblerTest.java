package io.github.pellse.assembler.test;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.LocalDateTime;
import java.util.List;

import static io.github.pellse.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.assembler.Rule.rule;
import static io.github.pellse.assembler.RuleMapper.oneToMany;
import static io.github.pellse.assembler.RuleMapper.oneToOne;
import static java.time.LocalDateTime.now;

record AugmentedVitals(Patient patient, HR heartRate, List<BP> bloodPressures, String diagnosis) {
    AugmentedVitals(Vitals vitals, Diagnosis diagnosis) {
        this(vitals.patient(), vitals.heartRate(), vitals.bloodPressures(), diagnosis.diagnosis());
    }
}

record Vitals(HR heartRate, Patient patient, List<BP> bloodPressures) {
}

record Patient(
        Integer id,
        String name,
        String healthCardNumber) {
}

record HR(
        Long id,
        int patientId,
        int heartRateValue,
        LocalDateTime time) {
}

record BP(
        String id,
        int patientId,
        int systolic,
        int diastolic,
        LocalDateTime time) {
}

record Diagnosis(Integer patientId, String diagnosis) {
}

public class ChainedAssemblerTest {

    Patient patient1 = new Patient(1, "John Deere", "123456789");
    Patient patient2 = new Patient(2, "Jane Doe", "287334321");
    Patient patient3 = new Patient(3, "Frank Smith Doe", "387654325");

    HR hr1 = new HR(1L, 1, 60, now());
    HR hr2 = new HR(2L, 2, 70, now());
    HR hr3 = new HR(3L, 3, 80, now());

    BP bp1_1 = new BP("1", 1, 120, 80, now());
    BP bp1_2 = new BP("2", 1, 130, 85, now());

    BP bp2_1 = new BP("3", 2, 140, 90, now());
    BP bp2_2 = new BP("4", 2, 150, 95, now());

    BP bp3_1 = new BP("5", 3, 160, 80, now());
    BP bp3_2 = new BP("6", 3, 170, 85, now());

    Vitals vitals1 = new Vitals(hr1, patient1, List.of(bp1_1, bp1_2));
    Vitals vitals2 = new Vitals(hr2, patient2, List.of(bp2_1, bp2_2));
    Vitals vitals3 = new Vitals(hr3, patient3, List.of(bp3_1, bp3_2));

    Diagnosis d1 = new Diagnosis(1, "Healthy");
    Diagnosis d2 = new Diagnosis(2, "Sick");
    Diagnosis d3 = new Diagnosis(3, "Critical");

    AugmentedVitals av1 = new AugmentedVitals(vitals1, d1);
    AugmentedVitals av2 = new AugmentedVitals(vitals2, d2);
    AugmentedVitals av3 = new AugmentedVitals(vitals3, d3);

    Flux<HR> heartRateFlux = Flux.just(hr1, hr2, hr3)
            .repeat(2);

    List<AugmentedVitals> expectedAugmentedVitals = List.of(av1, av2, av3, av1, av2, av3, av1, av2, av3);

    @Test
    public void testChainedAssemblers() {

        var vitalsAssembler = assemblerOf(Vitals.class)
                .withCorrelationIdResolver(HR::patientId)
                .withRules(
                        rule(Patient::id, oneToOne(this::getPatients)),
                        rule(BP::patientId, oneToMany(BP::id, this::getBPs)),
                        Vitals::new)
                .build();

        var augmentedVitalsAssembler = assemblerOf(AugmentedVitals.class)
                .withCorrelationIdResolver(Vitals::patient, Patient::id)
                .withRules(
                        rule(Diagnosis::patientId, oneToOne(this::getDiagnoses)),
                        AugmentedVitals::new)
                .build();

        StepVerifier.create(heartRateFlux
                        .window(3)
                        .flatMapSequential(vitalsAssembler::assemble)
                        .transform(augmentedVitalsAssembler::assemble))
                .expectSubscription()
                .expectNextSequence(expectedAugmentedVitals)
                .verifyComplete();
    }

    private Flux<Patient> getPatients(List<HR> heartRates) {
        return Flux.just(patient1, patient2, patient3);
    }

    private Flux<BP> getBPs(List<HR> heartRates) {
        return Flux.just(bp1_1, bp1_2, bp2_1, bp2_2, bp3_1, bp3_2);
    }

    private Flux<Diagnosis> getDiagnoses(List<Vitals> vitalsList) {
        return Flux.just(d1, d2, d3);
    }
}
