package ru.sberbank.bigdata.sl.rt.core;

/**
 * Mock для CoreImpl с переопределением методов по инициализации логера
 * Его "подхватывает" Factory и регистрирует как фабрику для создания Core
 */
public class TestCoreFactoryMock extends Factory<Core> {

    @Override
    public Core create(Transformation transformation) {
        return new CoreImpl() {

            @Override
            public void initializeLoggerFactory(AppContext ctx) {
            }
        };
    }

    @Override
    void register() {
        App.registerFactory(Core.class, new TestCoreFactoryMock());
    }

}