# vspc_idl

Витрина `usl_vspc_idl`. Релиз 3.0 работал на ESSA, релиз 4.0 (этот репозиторий) — на RT-FW 3.16.1.
Требование переезда: потребитель не замечает разницы, строки `snp` и `diff` выходят как у ESSA.

## Код (`src/main/java/.../vspc`)

- `MainClass` — вход: создаёт трансформации и задаёт зависимости, порядок запуска решает RT-FW.
- `transformations/` — расчёт таблиц, по классу на таблицу. Каждый читает источник, получает ключи ССК (`addSk`) и
  публикует (`publishChecked`).
- `common/` — общий код трансформаций; `constant/` — имена колонок и таблиц.
- `compat/` — слой совместимости с ESSA. Вместе с `common` не привязан к витрине и переносится в другие.

## Слой совместимости

RT-FW считает изменения по своим правилам. У его публикатора нет точек расширения, поэтому в проекте лежит копия
класса `ProcessingStage` с тремя вставками `COMPAT`; они оборачивают штатные калькуляторы RT-FW. Что закрыто:

- неизменившаяся строка не даёт новой версии — `PeriodRewriteCompatCalculator`;
- `start_dt` нового и вернувшегося ключа как в ESSA — там же, `LoaderCache`;
- пропавшие из источника ключи `div_type` закрываются — `DivTypeTransformation`;
- переписывается не весь снимок, а открытые версии, текущий и прошлый месяц — `PeriodRewriteCompatCalculator`;
- дубликаты в снимке и во входе сводятся к одной строке на ключ — `CompatSupport.onePerKey`;
- будущие версии затираются при слиянии — `MergeCompatCalculator`;
- несовпадение типа колонки — отказ, а не тихий `NULL` — `PublishPreconditions.alignTypes`;
- `ctl_validfrom` как в ESSA — `CompatHooks.enrichInit`, `HubCompatCalculator`.

При неподходящей версии RT-FW или настройках загрузка падает на старте (`StartupSelfCheck`).

## Ресурсы (`src/main/resources`)

- `meta/` — метаданные продукта; в 4.0 по ним работает RT-FW. Против 3.0 заполнены `lineage` и
  `archiveProviderEntities`, исправлено имя базы источника.
- `hdfs/rtfw/` — запуск: `StartLauncher.sh` стартует лаунчер RT-FW, а не `MainClass` напрямую.
- `ctl/` — потоки `USL_VSPC_IDL` (INCR) и `USL_VSPC_IDL_INIT`; параметры ESSA убраны.
- остальное (`sql`, `usl`, `s2t`, `docs`, `devops`) — без изменений против 3.0.
