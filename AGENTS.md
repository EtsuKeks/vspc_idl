# AGENTS.md — память проекта `usl_vspc_idl` (миграция ESSA 3.0 → RT-FW 3.16.1)

**Файл восстановлен 03.11.2026** (агент FA-15, зона «документация и состояние проекта»).
Побайтно восстановить нельзя: исходник утрачен (нет ни в корне, ни в `temp/`, ни в `.worktrees/` —
каталог `.worktrees/` отсутствует). Ниже — **пересобранная** память кампании по артефактам:
`temp/synthetic_20261103/`, `temp/audit3_20261102/`, `temp/fix_20261101/CLOSED_DEVIATIONS.md`,
пакет `/Users/22304126/Desktop/ess/vspc_verify/synth_v2/`.
**Запрещённые формулировки** (§2) действуют и в этом файле.

---

## 0. Соответствие ссылок старых отчётов

Отчёты кампании ссылаются на `AGENTS.md` по старой нумерации (§1…§8). Соответствие:

| старая ссылка | где теперь | содержание |
|---|---|---|
| `AGENTS.md §1` п.1 | §2.5, §3.1 (строка 1) | `ANALYZE` → `COMPUTE STATISTICS NOSCAN` |
| `AGENTS.md §1` п.2 | §3.1 (строка 2) | агрегат `max(end_dt)` только для `div_type` |
| `AGENTS.md §1` п.3 | §2.5, §4 (п. 4) | бэкап s2t не уезжает в fat-jar; `vendor/` не редактировался |
| `AGENTS.md §1` (FX6) | §3.1 (строка 3), §4 | `databaseCode` = `usl_vspc_idl_in_snp` — прав, оставляем |
| `AGENTS.md §1` (поставка) | §4 (п. 5) | HEAD 4.0 не собирается |
| `AGENTS.md §4`, §4.3, §4.4, §4.6 | §2.6, §3.3, §5 | D-4/D-7 — не отклонения; `mvn compile` без `clean`; `vendor/` |
| `AGENTS.md §5.1` | §2.8 | режим «один агент — один факт», отчёт — файл |
| `AGENTS.md §6` (команды) | §6 | команды сборки/проверок |
| `AGENTS.md §8` (п.1–3) | §4 (пп. 4–5) | что осталось за владельцем: коммиты, сборка, поставка |

---

## 1. Что это за проект

Витрина `usl_vspc_idl` переводится с **ESSA 3.0** на **RT-FW 3.16.1** («4.0»).
Цель кампании — доказать **паритет содержания** 3.0 ↔ 4.0 (16 целевых таблиц: 8 `snp` + 8 `diff`)
и подготовить **синтетику**, на которой этот паритет проверяется на DEV на краевых случаях.

| что | путь | статус |
|---|---|---|
| 4.0 (текущая работа) | `/Users/22304126/Desktop/ess/vspc_idl` | git, ветка `release/4.0`, HEAD `960d608` |
| 3.0 (эталон) | `/Users/22304126/Desktop/ess/vspc_idl_old` | **READ-ONLY. НИКОГДА НЕ ПРАВИТЬ** (даже `chmod`/датой) |
| пакет синтетики | `/Users/22304126/Desktop/ess/vspc_verify/synth_v2` | вне git (репозиторий-сверху `/Users/22304126`) |
| отчёты кампании | `temp/synthetic_20261103/reports/` | 49 файлов на 03.11.2026 |
| аудит №3 (волны 1–5) | `temp/audit3_20261102/` | `brief_*.md`, `INDEX.md`, `KNOWN_DEVIATIONS.md`, `READINESS_REPORT.md`, `REPORT_FOR_LEAD.md`, `WAVE*_SUMMARY.md`, `reports/` |
| реестр отклонений | `temp/fix_20261101/CLOSED_DEVIATIONS.md` | канон ОС-1…ОС-12 |

**Ключевые файлы 4.0:** `src/main/java/ru/sberbank/bigdata/dffess/vspc/common/BaseTransformation.java`,
`…/vspc/transformations/*.java` (9 трансформаций), `src/main/resources/sql/ddl/ddl.usl.vspc_idl.v021.model.9.47.0_fw_3.9.6.sql`,
`src/main/resources/meta/metadata_usl_vspc_idl_v021_t01_fw_3.16.1.json`, `vars_all.yml`,
`src/main/resources/hdfs/rtfw/StartLauncher.sh`, `src/main/resources/ctl/ctl_wait4replica_…yaml`.

---

## 2. Жёсткие правила (нарушение = откат работы)

1. **3.0 — только чтение.** Никаких записей в `vspc_idl_old`.
2. **Запрещённые формулировки в отчётах** (0 совпадений): «принятый риск», «осознанное решение»,
   «допустимо», «не блокер потому что…», «и так сойдёт». Вместо них — конкретика: что, где (`file:line`),
   чем измеряется, что делать.
3. **Коммиты делает владелец сам.** Ничего не коммитить, не пушить, не менять ветки.
4. **Стенда нет.** Выводы — статические, по коду и SQL. Измерения не выдумывать.
   Если нужны замеры — описать, как их снять, но не писать «измерено: N».
5. **Правки в коде 4.0 — задача агента, ревью — владельца.** Если расхождение выглядит как
   *уточнение уже согласованной карточки ОС* — не править код, а зафиксировать вопрос владельцу.
6. **Каждое утверждение — с `file:line`** по обеим версиям (3.0 и 4.0), если речь о паритете.
7. **`vendor/` в 4.0 не трогать** — это эталон RT-FW 2.16.1, 0 расхождений с pristine.
8. **Правило «создай файл отчёта сразу»:** отчёт создаётся пустым каркасом в первые 2–3 проверки,
   потом наполняется. Лимит — 15 минут wall-clock; по истечении — дописать найденное и
   отметить «время вышло» в §5. **Не заканчивать без файла отчёта.**

---

## 3. Что сделано (кратко, со ссылками)

### 3.1 Продукт (код 4.0)

| # | сделано | где |
|---|---|---|
| 1 | `ANALYZE` → `COMPUTE STATISTICS NOSCAN` | 4.0 `common/BaseTransformation.java:105` |
| 2 | Агрегат `max(end_dt)` — только для `div_type` | 4.0 `…/rt/publisher/strategy/process/changes/HistoricalPeriodWithRewriteChangesCalculator.java:345-390` |
| 3 | `databaseCode` = `usl_vspc_idl_in_snp` (оставляем; FX6 прав) | `metadata_…_fw_3.16.1.json` |
| 4 | Кампания фиксеров FX1–FX26 — **не зафиксирована владельцем** | `git status`: 17 `M`, 3 `D`, 1 `??` (`src/.../bigdata/sl/`, 5 shadow-классов) |

### 3.2 Синтетика (`synth_v2`)

| # | сделано | где |
|---|---|---|
| 5 | Схема `default` (G-1/R-1), `MANIFEST.tsv` | `synth_v2/` |
| 6 | **84 сценария** (24 базовых SC-00…SC-34 + 60 из модулей `g01…g10`), self-check **S1–S8**, EXIT=0 | `synth_v2/MANIFEST.tsv`, `gen_synth.py --self-check` |
| 7 | Разведение ID-пространств по модулям (`id` 2000201+, `fulldivcode` 9999999201+); 52 номера SC-40…135 объявлены **резервными** | `synth_v2/scenarios/*.py`, `scenarios/invariants.py` |
| 8 | Механизм `Scenario.extra_ddl` → `default_ref.ref_expected` создаётся (G-28) и **проверяется** чеком `CHK-REF-01/02` | `scenarios/model.py`, `gen_synth.py`, `checks/c12_ref_expected.sql` |
| 9 | **88 `check_id`** в `checks/` (SQL + python/shell); 0 «всегда зелёных», 0 unknown при классификации ожиданий | `synth_v2/checks/` |
| 10 | Автоматизация §1.1 протокола: `c11_ddl_order.py` (DDL 3.0↔4.0 побайтово + DESCRIBE 32 снимков) | `synth_v2/checks/c11_ddl_order.py` |
| 11 | Пускачи: `r1_apply.sh`, `r2_compare.sh`, `r3_preflight_clean.sh`, `r5_e2e_check.sh`, **`r6_script_checks.sh`** (исполняет скриптные чеки) | `synth_v2/runners/` |
| 12 | Ранбуд GATE-0…GATE-13, шаги 3.1–3.10, 4.1–4.8, 5.x, 6.1–6.3, приложения A–C | `synth_v2/RUNDOWN.md` (зона FA-10 — не править) |
| 13 | Проверка сверщика: DDL → §2.1 → §2.2 → §3, 16/16 таблиц, потерь 0 | `temp/synthetic_20261103/_verify_columns.py` (EXIT=0) |
| 14 | **Исходники ESSA 3.0 лежат локально** — `/Users/22304126/Desktop/ess/essa/sources/essa_common` (клон `essa-common-lib` 2.2.6) ⇒ паритет верифицируется **по исходникам**, а не по байткоду | `essa/sources/essa_common/essa-common-lib/src/main/java/…` |

Отчёты по волнам: `temp/synthetic_20261103/reports/` — `D1_GENERATION_report.md`, `D2_EXECUTION_report.md`,
`D3_INVARIANT_REGISTRY.md` (78 инвариантов, 10 доменов), `W2_*` (10), `W3_*` (10), `W4_*` (5),
`W5_*` (5), `W6_01`, `W6_02`, `FA_01`…`FA_15` (тотальный аудит 03.11.2026).

---

## 4. Что открыто

| # | вопрос | детали |
|---|---|---|
| 1 | **Имена колонок `*_sid` в коде 4.0 против `*_sk` в DDL/метаданных — разобрано, дефекта нет** (зона S7) | в 4.0 константы `*_SK` → `*_SID` (93 шт., `dffess/vspc/constant/ColumnsName.java`) — это **контракт RT-FW 3.16**: `SOURCE_SK_SUFFIX="_sid"` / `TARGET_SK_SUFFIX="_sk"` (`BaseTransformation.java:30-31`), а `addSk` (`:157-175+`) переименовывает `*_sid` → `*_sk` перед публикацией; вызывается во всех 9 трансформациях. Сопоставление с целевой схемой — по имени (`PublisherUtils.castToTargetStructure`, `:374-387`), снап наполняется переносом HDFS (`PublisherDao.moveToTarget`, `:571-584`) ⇒ в таблице `*_sk` с правильными значениями, как в DDL и в 3.0. **Контроль на DEV:** `COUNT(*) FROM default.snap40_incr1_addr WHERE addr_sk IS NULL` ⇒ 0 |
| 2 | **Профиль D-9: ×3 executor'ов + режим издателя** | ×3 `maxExecutors` — **минимум, а не улучшение**: в 3.0 своё подключение на преобразование (3×40), в 4.0 — одно на весь граф (40 на всё). AQE-флаги **не работают** в режиме `SALTING` (по умолчанию) — они читаются только в ветке `SPARK_REBALANCE`; в Spark 3.5.1 оба флага уже `true` по умолчанию, поэтому добавление их в `vars_all.yml` Spark не меняет, но **после** смены режима они обязательны (иначе тихий откат на `SALTING`). Разбор режимов: `reports/S6_publisher_modes_report.md` |
| 3 | **HEAD 4.0 не собирается** | нет 7 классов издателя (`VspcPublisher`, `HubDiffStrategy`, `HistSnpStrategy`, `HistDiffStrategy`, `HistMixStrategy`, `LinkHistDiffStrategy`, `HistFrameworkBase`); `VspcPublisher` есть только в `temp/backup_rewire_20261026/publish/`; 5 shadow-классов лежат **untracked** (`src/main/java/ru/sberbank/bigdata/sl/rt/publisher/strategy/**`) ⇒ `git commit -am` их не возьмёт, jar соберётся и потеряет 5 классов издателя. `mvn compile` без `clean` даёт ложный «BUILD SUCCESS» на устаревшем `target/classes` ⇒ префлайт обязан использовать `clean` |
| 4 | **`s2t_vspc_idl_diff.xlsx`** — отложен решением владельца 03.11.2026 | файл пересобран: потеряно 48 частей пакета (12 drawings + 12 drawing-rels + 11 printerSettings + 13 иных); данные 11 из 12 листов совпадают, отличается 1 ячейка `S2T!10 / T-src` (правка 4.0 содержательно верна, но не зарегистрирована). К resources вернуться позже |
| 5 | **Привязка «сценарий → чек»** — сделана на 5 сценариев из 84 | `CHK-BIND-01/01R` (`checks/c14_scenario_bind.py`) + колонка `invariant` в `MANIFEST.tsv` (84/84 заполнено); 79 сценариев пока без наблюдаемого чека — либо чеки по доменам, либо закрыть нельзя (обосновано в `reports/S2_checks_coverage_report.md`) |
| 6 | **Архивирование** отладочных `hue_debug.js`/`hue_probe*`, `probe/`, старого поколения `tNN`-скриптов | не выполнено, решение за владельцем |
| 7 | **ОС-8** (`sk_greenfield`) — **закрыт решением владельца 03.11.2026**: параметр не трогаем | в 4.0 читается вне режима `ON_GF` (`BaseTransformation.java:74-75`); на штатном CTL (`prior_value: ""`) расхождения нет; риск — только при непустом `sk_greenfield` на ПРОМе |
| 8 | **`debug_sk_off` = `D`/`ON_GF`/`OFF_TB` не проходят CTL-валидатор RT-FW 3.16** (`ApplicationParameters.java:137` — только `true`/`false`/`""`) ⇒ ветки `SkMode.DISABLE`/`ON_GF`/`OFF_TB` через CTL **недостижимы** | зона P01; связано с ОС-8 (режим `ON_GF`) |
| 9 | **`spark_parameters_init` не имеет потребителя** ⇒ `maxExecutors=300` на RDT/PROM **не доезжает** до стенда (CTL-INIT читает `spark_parameters`) | зона P06, `vars_all.yml` |
| 10 | **Черновики витрины в STGTMP не удаляются, имя таблицы = `hashCode()`** ⇒ +8 таблиц за прогон, накопление на ПРОМе | `BaseTransformation.java:190,234-237` (зона P07) |
| 11 | **fat-jar не защищён от `*.bak*`/`*.orig`/`*.rej`** в `src/main/resources` (дефолтные exclude Maven их не ловят) ⇒ добавить `<resources>` c `<exclude>` | зона P07, `pom.xml` (зона P06) |
| 12 | **8 чеков `c07_ctl.sql` не доходят до сравнения**: файл двусторонний (`ver_l`/`ver_r`) ⇒ пускач кладёт его в `raw/both/`, а сверка берёт `raw/30 ∩ raw/40` ⇒ «множества `p1month` СОВПАДАЮТ» не проверяется никем | зона P11, `runners/r2_compare.sh:791-796,901-902` |

---

## 5. Грабли (наблюдения, стоившие времени)

1. **Транскрипты дочерних сессий.** Ответ дочерней сессии приходит в родительскую как `task_output`;
   полный лог — в `~/.minimax/v2/sessions/YYYY/MM/DD/HH-MM-SS-mmm-session_<b64>/messages.jsonl`.
   **Каталог может лежать в предыдущем дне** (сессия началась в 23:59, а каталог — в 00:00 следующего
   дня, и наоборот): искать и в предыдущем, и в текущем дне. **Имя каталога — base64 от полного
   `session_id` с префиксом `mvs_`:** `bXZzXzljOTc5…` → `mvs_9c979c09f3c94daf949916657e6d2745`.
   `session_id` берётся из ответа `task` (не из имени каталога).
2. **Self-check ловит дубли PK, но не дубли бизнес-ключа.** `gen_synth.py` проверяет уникальность
   первичного ключа (`m_ → id`, `d_ → (id, ctl_validfrom)`, `successors → (id, idx)`,
   `rfzonetime → code`) и не проверяет `fulldivcode`. Именно поэтому пересечение ID-пространств
   между модулями (`id 2000091…2000096`) осталось тихим: SC-91/92/93 теряли строки, а канарейка
   SC-90 выживала. Добавить проверку уникальности `fulldivcode` в `m`.
3. **`AGENTS.md` / `HANDOVER.md` / `NEW_AGENT_PROMPT.md` не в git** ⇒ могут исчезнуть (как и случилось).
   Восстановимы из артефактов сессии: `temp/audit3_20261102/_restore_from_artifact.py`
   (снимает префикс `N→` из вывода `read`) + `reports/tool-outputs`.
4. **`temp/` и `vendor/` — в `.gitignore`** (`.gitignore` в корне `vspc_idl`: `target/`, `temp/`, `vendor/`).
   Значит **артефакты в `temp/` не переживут `git clean` и не попадут в сборку** — хранить отчёты
   копией вне репозитория либо держать в `temp/` и не полагаться на git для восстановления.
5. **`mvn compile` без `clean` даёт ложный зелёный свет** — `target/classes` непуст, Maven печатает
   «Nothing to compile — all classes are up to date».
6. **`--schema` по умолчанию в генераторе — `usl_vspc_idl_syn`** (`gen_synth.py:650`), хотя пакет
   лежит в `default`. Пакет надо перегенерировать **явно** `--schema default`, иначе получится
   «сравнение на пустых таблицах» (ложный успех паритета).
7. **`parse_layout` в `scenarios/common.py` не дедуплицирует блоки** («живой DESCRIBE» + «CANONICAL
   LAYOUT» в одном `layouts/*.txt`): 101 уникальное имя → 202 строки в DDL. Сравнение с архивной
   версией (`temp/vspc_verify_20261030/v1_out/gen_test2/gen_synth.py:62-80`) показывает, что дедуп
   там был.
8. **Заведённые ожидания надо сверять с фактом, а не с памятью.** Раньше в сводках писали
   «78 сценариев» после 84, «76 инвариантов» после 78, «751 DROP» после 567.

---

## 6. Команды (проверенные)

```bash
# сборка 4.0 (в РАБОЧЕМ дереве ожидается BUILD SUCCESS; из git-рефа — нет)
cd /Users/22304126/Desktop/ess/vspc_idl && /Applications/apache-maven-3.9.6/bin/mvn -o -B clean compile -DskipTests

# самопроверка пакета синтетики (ожидается «самопроверка: OK», EXIT=0)
cd /Users/22304126/Desktop/ess/vspc_verify/synth_v2 && python3 gen_synth.py --base-date 2026-10-30 --schema default --out /tmp/fin --self-check

# проверка колонок сверщика (ожидается EXIT=0)
cd /Users/22304126/Desktop/ess/vspc_idl/temp/synthetic_20261103 && python3 _verify_columns.py

# новый чек схемы/порядка колонок
cd /Users/22304126/Desktop/ess/vspc_verify/synth_v2 && python3 checks/c11_ddl_order.py --selftest
```

---

## 7. Как продолжать

1. **Сначала прочитать `temp/synthetic_20261103/FINAL_AUDIT_BRIEF.md`** (задачи по зонам) и
   `temp/synthetic_20261103/FINAL_AUDIT_SUMMARY.md` (сводка), затем
   `temp/synthetic_20261103/reports/FA_*.md` (тотальный аудит 03.11.2026, 15 зон) и `S1…S7` (финальная фаза).
2. **Не открывать заново то, что закрыто**: ОС-1…ОС-12 (`temp/fix_20261101/CLOSED_DEVIATIONS.md`),
   реестр инвариантов (`reports/D3_INVARIANT_REGISTRY.md`, 78 шт.), сводки волн 1–5, W6 и FA/S-отчёты.
3. **Сначала статика, потом стенд.** Стенда нет ⇒ выводы по коду/SQL, замеры — только как «как снять».
4. **Правку в 4.0 — с `file:line` по обеим версиям и пометкой, к какому `INV-*` это относится.**
5. **Отчёт — файл в `temp/synthetic_20261103/reports/`**, создавать сразу, формат — из брифа (§4).
6. **Фактические числа** брать из артефактов, а не из прошлых сводок:
   сценарии — `synth_v2/MANIFEST.tsv` (84), `check_id` — `synth_v2/checks/*.sql` (**88**),
   целевые таблицы — `src/main/resources/sql/ddl/…sql` (16), карточки ОС — `CLOSED_DEVIATIONS.md` (12),
   инварианты — `reports/D3_INVARIANT_REGISTRY.md` (78), самопроверка пакета — **S1…S8**.

---

## 8. Решения владельца (03.11.2026) — не переоткрывать без нового факта

| # | решение | где зафиксировано |
|---|---|---|
| 1 | **ОС-8 `sk_greenfield`: параметр не трогаем** (оставить поведение 4.0) | `CLOSED_DEVIATIONS.md` §0-бис, карточка ОС-8 (подтверждение 03.11.2026) |
| 2 | **`s2t_vspc_idl_diff.xlsx` — отложен**, к ресурсам вернуться позже | `CLOSED_DEVIATIONS.md` §6.4 (приписка от 03.11.2026) |
| 3 | **`service_instance_code` — довести без вопросов владельцу**: канал 3.0 (CTL-имя потока между `"__"` → `toLowerCase`) воспроизведён в 4.0; при пустом — не выставляется | `CLOSED_DEVIATIONS.md` карточка З-7 (уточнение 03.11.2026); отчёт `reports/S4_instance_code_report.md` |
| 4 | **`CHK-DIFF-09`, `CHK-HIST-06`, `CHK-HIST-12`, SC-73 и покрытие — решаем сами** (без вопроса владельцу) | `reports/S1…S3` |
| 5 | **Правило издателя (AQE/`SPARK_REBALANCE`) — переключено 03.11.2026** (режим задан в коде: `dffess/vspc/common/PublisherMode.java`, 8 точек публикации; AQE-флаги закреплены в `vars_all.yml`); разбор режимов — `reports/S6_publisher_modes_report.md`, реализация — `reports/S8_publisher_mode_switch_report.md` | `reports/S6_publisher_modes_report.md` |
| 6 | **Привязку 79 сценариев «по ключу» не делаем** (решение владельца 03.11.2026). Механизм проверки — как есть: сгенерировать данные → посчитать в 3.0 и 4.0 → сверить содержимое 16 таблиц. В отчёте о прогоне фиксируется одна оговорка: сверка не показывает, что данные каждого сценария долели (обе версии получают одинаковые данные) | `reports/S2_checks_coverage_report.md` |

**Грабли кампании (03.11.2026), которых не было в памяти раньше:**

* **Исходники ESSA 3.0 лежат локально**: `/Users/22304126/Desktop/ess/essa/sources/essa_common`
  (клон `essa-common-lib` 2.2.6). Раньше считалось, что первоисточник недоступен и паритет
  верифицируется только по байткоду ⇒ на деле `NEW_CONDITION`, `SkClientWrapper`, `CtlAdapterImpl`,
  `DataLoaderHistMix` читаются **как исходники**. Проверено: `NEW_CONDITION` в 3.0 = «нет открытой
  версии» ⇒ **совпадает** с `noOpenVersion` в 4.0 ⇒ расхождения по `start_dt` нет
  (`reports/S5_new_condition_report.md`).
* **Имена колонок в коде 4.0 — `*_sid`, в DDL и метаданных — `*_sk`** (93 константы). Это не опечатка,
  а переименование целиком; вендор пишет DataFrame как есть ⇒ что попадёт в таблицу, требует
  проверки на DEV (см. §4, п.1).
