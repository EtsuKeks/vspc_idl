package ru.sberbank.bigdata.sl.rt.core;

import ru.sberbank.bigdata.sl.rt.coordinator.Coordinator;

/** Local stand: shadows the real CoordinatorFactory (CTL client) with a stub. */
public class CoordinatorFactory extends Factory<Coordinator> {
    @Override
    public Coordinator create(Transformation transformation) {
        return StubCoordinator.INSTANCE;
    }

    @Override
    void register() {
        App.registerFactory(Coordinator.class, new CoordinatorFactory());
    }
}
