/**
 * Слой совместимости с ESSA 3.0 поверх RT-FW 2.16.1: декораторы ChangesCalculator/MergeCalculator вендора,
 * вызываемые из блоков VSPC-COMPAT в shadow-копии ProcessingStage (L5_INIT_ENRICH, L5_CALC_DIFF, L5_CALC_MERGE).
 * Реестр поправок: id — что — правило 3.0 (id стоит в комментарии у кода поправки).
 * E = essa-common-lib/src/main/java/ru/sberbank/bigdata/essa/common, O = vspc_idl_old/src/main/java/ru/sberbank/bigdata/dffess/vspc,
 * V = rt-fw-publisher-2.16.1/ru/sberbank/bigdata/sl/rt/publisher.
 * <ul>
 * <li>REG_ORD_LONG — div_.reg_ord: cast(int), затем bigint (transformations.DivTransformation) — O/tables/div/DivTransformer.java:52</li>
 * <li>DIVH_INIT_VALIDFROM — div_h INIT: ctl_validfrom := start_dt — O/impl/loader/DataLoaderHistMix.java:108-110</li>
 * <li>DV-05 — «сегодня» берёт каждый шаг: start_dt и закрытия D-2 — дата трансформации (VspcLoadDate), зона — часы публикации, VANISH и stg.chg_div_h — current_date() запроса — O/constant/VspcConstant.java:19-21, E/slave/move/HistMovingGenerator.java:31</li>
 * <li>PERIOD_ZONE_MONTH — зона перезаписи: p1month &gt;= месяц (LocalDate.now() публикации - 1 мес) — E/slave/move/HistMovingGenerator.java:30-32</li>
 * <li>PERIOD_OPEN_ONLY — вход сравнивается только с открытыми версиями — O/impl/loader/DataLoaderHistMix.java:65-66</li>
 * <li>PERIOD_EQUALS_RETENTION — версия с тем же хэшем 3.0 ног не даёт — E/slave/loading/DataLoaderHistSnp.java:82-86</li>
 * <li>PERIOD_CONSEC_DAY — повтор за день: (R,I)+(U,D), старая версия удаляется молча — O/impl/loader/DataLoaderHistMix.java:84-89</li>
 * <li>PERIOD_SILENT_DROP — удаления части 1 (в т.ч. закрытые версии 999912) молча — E/slave/loading/DataLoaderHistSnp.java:117-119, O/impl/loader/DataLoaderHistMix.java:84-88</li>
 * <li>PERIOD_NEW_KEY_1900 — ключ без открытой версии: start_dt = 1900-01-01 — E/slave/loading/DataLoaderHistDiff.java:83</li>
 * <li>PERIOD_NULL_KEY — NULL-ключ вне окна DV-03 (группа ниже зоны) — один ключ мимо join вендора (не null-safe), merge null-safe — E/slave/loading/SnpDataLoader.java:181-204</li>
 * <li>PERIOD_CLOSED_KEEP, DV-04 — закрытые секции зоны в merge не участвуют, переносятся как есть — O/impl/loader/DataLoaderHistMix.java:66-69,94</li>
 * <li>DV-03 — окно 3.0: bk с &gt;1 строкой в snp (у period — открытой) или во входе, rn по desc(ctl_loading), asc(record_status), мимо вендора — E/slave/loading/SnpDataLoader.java:181-204</li>
 * <li>DV-77 — ничья окна DV-03: дальше физический порядок плана, без правила «при равенстве», как 3.0 — E/slave/loading/SnpDataLoader.java:181</li>
 * <li>HUB_NULL_KEY_30 — хаб: любая строка с NULL-pk — группа окна DV-03 — E/slave/loading/DataLoaderHubDiff.java:75-80,127-144</li>
 * <li>DV-79 — PERIOD: группа &gt;1 открытой без входа (и NULL) — окно DV-03, OLD rn1 остаётся — O/impl/loader/DataLoaderHistMix.java:66,74-89</li>
 * <li>DV-81 — PERIOD: NULL-ключ со строкой во входе — окно DV-03 null-safe (одна версия) — E/slave/loading/SnpDataLoader.java:181</li>
 * <li>DV-85 — div_type: NEW окна DV-03 (возвращенец парой) при закрытой истории — start_dt = max(закрытый end_dt) + 1 — E/slave/loading/DataLoaderHistSnp.java:90-96</li>
 * <li>DV-80 — PERIOD, часть 1: N закрытий одного PK (группа ниже зоны) — одна строка на PK вместо N×N (V/strategy/process/merge/GeneralMergeCalculator.java:79-82) — E/slave/loading/DataLoaderHistSnp.java:103-108,124</li>
 * <li>DV-07 — пустой INCR при группе окна DV-03 в snp — строка-маркер мимо раннего выхода вендора (V/PublisherImpl.java:230-235) — E/slave/check/EmptyChecker.java:13-16</li>
 * <li>DIVTYPE_EMPTY_INCR — div_type: пустой INCR закрывает все открытые версии (строка-маркер) — E/slave/loading/DataLoaderHistSnp.java:70</li>
 * <li>DIVTYPE_RETURNER_START — возвращенец: start_dt = max(закрытый end_dt) + 1 — E/slave/loading/DataLoaderHistSnp.java:88-96</li>
 * <li>DIVTYPE_VANISH — исчезнувший ключ закрывается current_date()-1 — E/slave/loading/DataLoaderHistSnp.java:103-108</li>
 * <li>HUB_HASH_ESSA30 — равенство версий по хэшу 3.0 — E/slave/loading/SnpDataLoader.java:116-133</li>
 * <li>HUB_UNDO_VALIDFROM — хабы: (U,U) хранит ctl_validfrom прежней версии — E/slave/loading/DataLoaderHubDiff.java:127-135</li>
 * <li>DV-08 — повтор INCR с тем же ctl_loading: diff L дописывается (guard.PublishPreconditions.keepLegs), открытая версия с L без ног — холостая пара (R,U)+(U,U) — E/slave/move/MovingTableService.java:44, O/impl/loader/DataLoaderHistMix.java:92</li>
 * <li>GUARD_SCHEMA_TYPES, DV-20 — тип входа != типу цели: ANSI-cast или отказ до записи (вендор дал бы NULL) — E/slave/app/HiveWriter.java:10</li>
 * <li>GUARD_UNKNOWN_CALCULATOR — калькулятор не того класса/режима или merge без обёрнутого diff: отказ — в 3.0 нет</li>
 * <li>GUARD_RTFW_VERSION — версии RT-FW на classpath = закреплённым — в 3.0 нет</li>
 * <li>DV-35 — guard.StartupSelfCheck: ProcessingStage загружен из того же jar, что CompatHooks, иначе отказ — в 3.0 нет (pom.xml:155-161)</li>
 * <li>DV-66, DV-16 — init_diff_flag=true на INIT, starting_business_date, max_increment_depth_days &lt; 1: 3.0 их не исполнял — отказ при старте — в 3.0 нет</li>
 * </ul>
 */
package ru.sberbank.bigdata.dffess.vspc.compat;
