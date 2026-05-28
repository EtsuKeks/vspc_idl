package ru.sberbank.bigdata.sl.rt.core;

import ru.sberbank.bigdata.sl.rt.coordinator.Coordinator;

/**
 * Класс-мок для создания сущности координатора в rtfw-idl тестах
 */
public class CoordinatorFactoryMock extends Factory<Coordinator> {

    private static Coordinator COORDINATOR_MOCK;

    public static void setCoordinatorMock(Coordinator coordinatorMock) {
        COORDINATOR_MOCK = coordinatorMock;
    }

    @Override
    public Coordinator create(Transformation transformation) {
        return COORDINATOR_MOCK;
    }

    @Override
    void register() {
        App.registerFactory(Coordinator.class, new CoordinatorFactoryMock());
    }

}