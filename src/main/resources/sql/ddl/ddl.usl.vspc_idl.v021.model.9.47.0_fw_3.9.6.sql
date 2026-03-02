------------------------------------------------------------------------------------------------------------------------
-- Published data snapshot
------------------------------------------------------------------------------------------------------------------------

-- Адрес: hub table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_snp.addr;

create external table usl_{{ logical_env_prefix }}vspc_idl_snp.addr (
    addr_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для данного экземпляра сущности &#39;Адрес&#39; (для поля &#39;addr_sk&#39;)',
    addr_sk BIGINT COMMENT 'Суррогатный ключ сущности &#39;Адрес&#39;',
    city_name STRING COMMENT 'Город в адресе (устарело), в варианте представления &#39;Наименование&#39; (DEPRECATED)',
    cntry_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для ссылки cntry_sk',
    cntry_sk BIGINT COMMENT 'Страна в адресе',
    ctl_loading BIGINT COMMENT '(техническое поле) Идентификатор загрузки CTL, в рамках которой создана эта запись',
    ctl_validfrom TIMESTAMP COMMENT '(техническое поле) Временная метка старта успешно завершенного обновления данной таблицы, приведшего к появлению или обновлению этой записи',
    deleted_flag BOOLEAN COMMENT 'Признак логического удаления экземпляра сущности &#39;Адрес&#39;',
    dirty_flag BOOLEAN COMMENT 'Признак записи не из мастер-источника, включенной в интегрированный набор для обеспечения ссылочной целостности (DATA-7)',
    district_name STRING COMMENT 'Район в адресе (устарело), в варианте представления &#39;Наименование&#39; (DEPRECATED)',
    fias_code STRING COMMENT 'Идентификационный код (GUID) ФИАС адреса (устарело) (DEPRECATED)',
    fias_code_addr_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для ссылки fias_code_addr_sk',
    fias_code_addr_sk BIGINT COMMENT 'Адрес по ГАР ФИАС, соответствующий нестандартизированному адресу',
    house_num STRING COMMENT 'Номер дома в адресе (устарело) (DEPRECATED)',
    idx_num STRING COMMENT 'Почтовый индекс в адресе (устарело) (DEPRECATED)',
    locty_name STRING COMMENT 'Населенный пункт в адресе (устарело), в варианте представления &#39;Наименование&#39; (DEPRECATED)',
    okato2_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для ссылки okato2_sk',
    okato2_sk BIGINT COMMENT 'ОКАТО адреса (устарело) (DEPRECATED)',
    reg_addr_name STRING COMMENT 'Наименование региона в адресе (устарело), в варианте представления &#39;Наименование&#39; (DEPRECATED)',
    rus_fed_subj_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для ссылки rus_fed_subj_sk',
    rus_fed_subj_sk BIGINT COMMENT 'Субъект РФ в нестандартизированном адресе',
    sid STRING COMMENT 'Идентификатор сущности &#39;Адрес&#39; в data-продукте-источнике',
    src_txt STRING COMMENT 'Нестандартизированный адрес строкой, в варианте представления &#39;Текст&#39;',
    street_name STRING COMMENT 'Улица в адресе (устарело), в варианте представления &#39;Наименование&#39; (DEPRECATED)',
    type_code STRING COMMENT 'Адрес - код субтипа'
)
COMMENT 'Адрес: hub table'

stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/snp/addr';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_snp.addr;

-- Адрес: satellite table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_snp.addr_h;

create external table usl_{{ logical_env_prefix }}vspc_idl_snp.addr_h (
    addr_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для данного экземпляра сущности &#39;Адрес&#39; (для поля &#39;addr_sk&#39;)',
    addr_sk BIGINT COMMENT 'Суррогатный ключ сущности &#39;Адрес&#39;',
    address_okato2_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для ссылки address_okato2_sk',
    address_okato2_sk BIGINT COMMENT 'ОКАТО адреса',
    address_oktmo2_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для ссылки address_oktmo2_sk',
    address_oktmo2_sk BIGINT COMMENT 'ОКТМО адреса',
    ctl_loading BIGINT COMMENT '(техническое поле) Идентификатор загрузки CTL, в рамках которой создана эта запись',
    ctl_validfrom TIMESTAMP COMMENT '(техническое поле) Временная метка старта успешно завершенного обновления данной таблицы, приведшего к появлению или обновлению этой записи',
    end_dt DATE COMMENT 'Дата окончания действия версии экземпляра по календарю бизнес-истории',
    standard_addr_txt STRING COMMENT 'Адрес строкой после стандартизации (устарело), в варианте представления &#39;Текст&#39; (DEPRECATED)',
    start_dt DATE COMMENT 'Дата начала действия версии экземпляра по календарю бизнес-истории',
    type_code STRING COMMENT 'Адрес - код субтипа'
)
COMMENT 'Адрес: satellite table'
partitioned by (p1month STRING)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/snp/addr_h';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_snp.addr_h;

-- Подразделение территориальной структуры Сбербанка: hub table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_snp.div_;

create external table usl_{{ logical_env_prefix }}vspc_idl_snp.div_ (
    code STRING COMMENT 'Код подразделения территориальной структуры Сбербанка',
    ctl_loading BIGINT COMMENT '(техническое поле) Идентификатор загрузки CTL, в рамках которой создана эта запись',
    ctl_validfrom TIMESTAMP COMMENT '(техническое поле) Временная метка старта успешно завершенного обновления данной таблицы, приведшего к появлению или обновлению этой записи',
    deleted_flag BOOLEAN COMMENT 'Признак логического удаления экземпляра сущности &#39;Подразделение территориальной структуры Сбербанка&#39;',
    dirty_flag BOOLEAN COMMENT 'Признак записи не из мастер-источника, включенной в интегрированный набор для обеспечения ссылочной целостности (DATA-7)',
    div_close_dt DATE COMMENT 'Дата закрытия подразделения территориальной структуры Сбербанка',
    div_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для данного экземпляра сущности &#39;Подразделение территориальной структуры Сбербанка&#39; (для поля &#39;div_sk&#39;)',
    div_open_dt DATE COMMENT 'Дата открытия подразделения территориальной структуры Сбербанка',
    div_sk BIGINT COMMENT 'Суррогатный ключ сущности &#39;Подразделение территориальной структуры Сбербанка&#39;',
    div_time_zone_time_zone_rf_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для ссылки div_time_zone_time_zone_rf_sk',
    div_time_zone_time_zone_rf_sk BIGINT COMMENT 'Часовая зона РФ подразделения территориальной структуры Сбербанка',
    full_reg_num STRING COMMENT 'Полный регистрационный номер подразделения территориальной структуры Сбербанка',
    legal_addr_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для ссылки legal_addr_sk',
    legal_addr_sk BIGINT COMMENT 'Юридический адрес подразделения территориальной структуры Сбербанка',
    parent_div_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для ссылки parent_div_sk',
    parent_div_sk BIGINT COMMENT 'Вышестоящее подразделение территориальной структуры Сбербанка',
    reg_ord BIGINT COMMENT 'Регистрационный номер подразделения территориальной структуры Сбербанка',
    short_div_code STRING COMMENT 'Краткий код подразделения территориальной структуры Сбербанка',
    sid STRING COMMENT 'Идентификатор сущности &#39;Подразделение территориальной структуры Сбербанка&#39; в data-продукте-источнике',
    subbranch_code STRING COMMENT 'Код subbranch подразделения территориальной структуры Сбербанка'
)
COMMENT 'Подразделение территориальной структуры Сбербанка: hub table'

stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/snp/div_';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_snp.div_;

-- Информация о реарганизации подразделения территориальной структуры Сбербанка: link table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_snp.div_div_relation;

create external table usl_{{ logical_env_prefix }}vspc_idl_snp.div_div_relation (
    ctl_loading BIGINT COMMENT '(техническое поле) Идентификатор загрузки CTL, в рамках которой создана эта запись',
    ctl_validfrom TIMESTAMP COMMENT '(техническое поле) Временная метка старта успешно завершенного обновления данной таблицы, приведшего к появлению или обновлению этой записи',
    deleted_flag BOOLEAN COMMENT 'Признак логического удаления экземпляра сущности &#39;Информация о реарганизации подразделения территориальной структуры Сбербанка&#39;',
    div_div_relation_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для данного экземпляра сущности &#39;Информация о реарганизации подразделения территориальной структуры Сбербанка&#39; (для поля &#39;div_div_relation_sk&#39;)',
    div_div_relation_sk BIGINT COMMENT 'Суррогатный ключ сущности &#39;Информация о реарганизации подразделения территориальной структуры Сбербанка&#39;',
    end_dt DATE COMMENT 'Дата окончания действия версии экземпляра по календарю бизнес-истории',
    sid STRING COMMENT 'Идентификатор сущности &#39;Информация о реарганизации подразделения территориальной структуры Сбербанка&#39; в data-продукте-источнике',
    source_div_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для ссылки source_div_sk',
    source_div_sk BIGINT COMMENT 'Подразделение территориальной структуры Сбербанка, передавшее полномочия',
    start_dt DATE COMMENT 'Дата начала действия версии экземпляра по календарю бизнес-истории',
    successor_div_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для ссылки successor_div_sk',
    successor_div_sk BIGINT COMMENT 'Подразделение территориальной структуры Сбербанка, принявшее полномочия'
)
COMMENT 'Информация о реарганизации подразделения территориальной структуры Сбербанка: link table'
partitioned by (p1month STRING)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/snp/div_div_relation';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_snp.div_div_relation;

-- Подразделение территориальной структуры Сбербанка: satellite table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_snp.div_h;

create external table usl_{{ logical_env_prefix }}vspc_idl_snp.div_h (
    ctl_loading BIGINT COMMENT '(техническое поле) Идентификатор загрузки CTL, в рамках которой создана эта запись',
    ctl_validfrom TIMESTAMP COMMENT '(техническое поле) Временная метка старта успешно завершенного обновления данной таблицы, приведшего к появлению или обновлению этой записи',
    disabled_access_equipment_flag BOOLEAN COMMENT 'Признак наличия оборудования для доступа маломобильных групп населения',
    div_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для данного экземпляра сущности &#39;Подразделение территориальной структуры Сбербанка&#39; (для поля &#39;div_sk&#39;)',
    div_name STRING COMMENT 'Наименование подразделения территориальной структуры Сбербанка, в варианте представления &#39;Наименование&#39;',
    div_sk BIGINT COMMENT 'Суррогатный ключ сущности &#39;Подразделение территориальной структуры Сбербанка&#39;',
    end_dt DATE COMMENT 'Дата окончания действия версии экземпляра по календарю бизнес-истории',
    fact_addr_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для ссылки fact_addr_sk',
    fact_addr_sk BIGINT COMMENT 'Фактический адрес подразделения территориальной структуры Сбербанка',
    pay_sys_prtcpnt_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для ссылки pay_sys_prtcpnt_sk',
    pay_sys_prtcpnt_sk BIGINT COMMENT 'Участник платёжной системы Банка России, являющийся подразделением территориальной структуры Сбербанка',
    start_dt DATE COMMENT 'Дата начала действия версии экземпляра по календарю бизнес-истории'
)
COMMENT 'Подразделение территориальной структуры Сбербанка: satellite table'
partitioned by (p1month STRING)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/snp/div_h';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_snp.div_h;

-- Подразделение территориальной структуры Сбербанка: satellite table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_snp.div_type;

create external table usl_{{ logical_env_prefix }}vspc_idl_snp.div_type (
    ctl_loading BIGINT COMMENT '(техническое поле) Идентификатор загрузки CTL, в рамках которой создана эта запись',
    ctl_validfrom TIMESTAMP COMMENT '(техническое поле) Временная метка старта успешно завершенного обновления данной таблицы, приведшего к появлению или обновлению этой записи',
    div_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для данного экземпляра сущности &#39;Подразделение территориальной структуры Сбербанка&#39; (для поля &#39;div_sk&#39;)',
    div_sk BIGINT COMMENT 'Суррогатный ключ сущности &#39;Подразделение территориальной структуры Сбербанка&#39;',
    div_type_terr_div_type_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для ссылки div_type_terr_div_type_sk',
    div_type_terr_div_type_sk BIGINT COMMENT 'Вид подразделения территориальной структуры Сбербанка',
    end_dt DATE COMMENT 'Дата окончания действия версии экземпляра по календарю бизнес-истории',
    start_dt DATE COMMENT 'Дата начала действия версии экземпляра по календарю бизнес-истории'
)
COMMENT 'Подразделение территориальной структуры Сбербанка: satellite table'
partitioned by (p1month STRING)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/snp/div_type';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_snp.div_type;

-- Вид подразделений территориальной структуры Сбербанка: hub table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_snp.terr_div_type;

create external table usl_{{ logical_env_prefix }}vspc_idl_snp.terr_div_type (
    ctl_loading BIGINT COMMENT '(техническое поле) Идентификатор загрузки CTL, в рамках которой создана эта запись',
    ctl_validfrom TIMESTAMP COMMENT '(техническое поле) Временная метка старта успешно завершенного обновления данной таблицы, приведшего к появлению или обновлению этой записи',
    deleted_flag BOOLEAN COMMENT 'Признак логического удаления экземпляра сущности &#39;Вид подразделений территориальной структуры Сбербанка&#39;',
    dirty_flag BOOLEAN COMMENT 'Признак записи не из мастер-источника, включенной в интегрированный набор для обеспечения ссылочной целостности (DATA-7)',
    div_type_code STRING COMMENT 'Код вида подразделений территориальной структуры Сбербанка',
    sid STRING COMMENT 'Идентификатор сущности &#39;Вид подразделений территориальной структуры Сбербанка&#39; в data-продукте-источнике',
    terr_div_type_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для данного экземпляра сущности &#39;Вид подразделений территориальной структуры Сбербанка&#39; (для поля &#39;terr_div_type_sk&#39;)',
    terr_div_type_sk BIGINT COMMENT 'Суррогатный ключ сущности &#39;Вид подразделений территориальной структуры Сбербанка&#39;'
)
COMMENT 'Вид подразделений территориальной структуры Сбербанка: hub table'

stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/snp/terr_div_type';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_snp.terr_div_type;

-- Часовая зона РФ: hub table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_snp.time_zone_rf;

create external table usl_{{ logical_env_prefix }}vspc_idl_snp.time_zone_rf (
    ctl_loading BIGINT COMMENT '(техническое поле) Идентификатор загрузки CTL, в рамках которой создана эта запись',
    ctl_validfrom TIMESTAMP COMMENT '(техническое поле) Временная метка старта успешно завершенного обновления данной таблицы, приведшего к появлению или обновлению этой записи',
    deleted_flag BOOLEAN COMMENT 'Признак логического удаления экземпляра сущности &#39;Часовая зона РФ&#39;',
    dirty_flag BOOLEAN COMMENT 'Признак записи не из мастер-источника, включенной в интегрированный набор для обеспечения ссылочной целостности (DATA-7)',
    sid STRING COMMENT 'Идентификатор сущности &#39;Часовая зона РФ&#39; в data-продукте-источнике',
    time_zone_abbr_code STRING COMMENT 'Аббревиатура часовой зоны РФ',
    time_zone_code STRING COMMENT 'Код часовой зоны РФ',
    time_zone_msk_shift_val STRING COMMENT 'Смещение часовой зоны РФ относительно Москвы',
    time_zone_name STRING COMMENT 'Наименование часовой зоны РФ, в варианте представления &#39;Наименование&#39;',
    time_zone_rf_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для данного экземпляра сущности &#39;Часовая зона РФ&#39; (для поля &#39;time_zone_rf_sk&#39;)',
    time_zone_rf_sk BIGINT COMMENT 'Суррогатный ключ сущности &#39;Часовая зона РФ&#39;',
    time_zone_wrld_shift_val STRING COMMENT 'Смещение часовой зоны РФ относительно общемирового времени'
)
COMMENT 'Часовая зона РФ: hub table'

stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/snp/time_zone_rf';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_snp.time_zone_rf;


------------------------------------------------------------------------------------------------------------------------
-- Published difference data
------------------------------------------------------------------------------------------------------------------------

-- Адрес: hub table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_diff.addr;

create external table usl_{{ logical_env_prefix }}vspc_idl_diff.addr (
    addr_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для данного экземпляра сущности &#39;Адрес&#39; (для поля &#39;addr_sk&#39;)',
    addr_sk BIGINT COMMENT 'Суррогатный ключ сущности &#39;Адрес&#39;',
    city_name STRING COMMENT 'Город в адресе (устарело), в варианте представления &#39;Наименование&#39; (DEPRECATED)',
    cntry_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для ссылки cntry_sk',
    cntry_sk BIGINT COMMENT 'Страна в адресе',
    ctl_action STRING COMMENT '(техническое поле) Операция (I&#x3D;insert, U&#x3D;update, D&#x3D;delete)',
    ctl_validfrom TIMESTAMP COMMENT '(техническое поле) Временная метка старта успешно завершенного обновления данной таблицы, приведшего к появлению или обновлению этой записи',
    deleted_flag BOOLEAN COMMENT 'Признак логического удаления экземпляра сущности &#39;Адрес&#39;',
    dirty_flag BOOLEAN COMMENT 'Признак записи не из мастер-источника, включенной в интегрированный набор для обеспечения ссылочной целостности (DATA-7)',
    district_name STRING COMMENT 'Район в адресе (устарело), в варианте представления &#39;Наименование&#39; (DEPRECATED)',
    fias_code STRING COMMENT 'Идентификационный код (GUID) ФИАС адреса (устарело) (DEPRECATED)',
    fias_code_addr_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для ссылки fias_code_addr_sk',
    fias_code_addr_sk BIGINT COMMENT 'Адрес по ГАР ФИАС, соответствующий нестандартизированному адресу',
    house_num STRING COMMENT 'Номер дома в адресе (устарело) (DEPRECATED)',
    idx_num STRING COMMENT 'Почтовый индекс в адресе (устарело) (DEPRECATED)',
    locty_name STRING COMMENT 'Населенный пункт в адресе (устарело), в варианте представления &#39;Наименование&#39; (DEPRECATED)',
    okato2_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для ссылки okato2_sk',
    okato2_sk BIGINT COMMENT 'ОКАТО адреса (устарело) (DEPRECATED)',
    reg_addr_name STRING COMMENT 'Наименование региона в адресе (устарело), в варианте представления &#39;Наименование&#39; (DEPRECATED)',
    rus_fed_subj_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для ссылки rus_fed_subj_sk',
    rus_fed_subj_sk BIGINT COMMENT 'Субъект РФ в нестандартизированном адресе',
    sid STRING COMMENT 'Идентификатор сущности &#39;Адрес&#39; в data-продукте-источнике',
    src_txt STRING COMMENT 'Нестандартизированный адрес строкой, в варианте представления &#39;Текст&#39;',
    street_name STRING COMMENT 'Улица в адресе (устарело), в варианте представления &#39;Наименование&#39; (DEPRECATED)',
    type_code STRING COMMENT 'Адрес - код субтипа'
)
COMMENT 'Адрес: hub table'
partitioned by (ctl_loading BIGINT,ctl_do STRING)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/diff/addr';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_diff.addr;

-- Адрес: satellite table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_diff.addr_h;

create external table usl_{{ logical_env_prefix }}vspc_idl_diff.addr_h (
    addr_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для данного экземпляра сущности &#39;Адрес&#39; (для поля &#39;addr_sk&#39;)',
    addr_sk BIGINT COMMENT 'Суррогатный ключ сущности &#39;Адрес&#39;',
    address_okato2_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для ссылки address_okato2_sk',
    address_okato2_sk BIGINT COMMENT 'ОКАТО адреса',
    address_oktmo2_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для ссылки address_oktmo2_sk',
    address_oktmo2_sk BIGINT COMMENT 'ОКТМО адреса',
    ctl_action STRING COMMENT '(техническое поле) Операция (I&#x3D;insert, U&#x3D;update, D&#x3D;delete)',
    ctl_validfrom TIMESTAMP COMMENT '(техническое поле) Временная метка старта успешно завершенного обновления данной таблицы, приведшего к появлению или обновлению этой записи',
    end_dt DATE COMMENT 'Дата окончания действия версии экземпляра по календарю бизнес-истории',
    p1month STRING COMMENT 'Поле для секционирования по бизнес-истории (одна секция на 1 month, рассчитывается как последняя дата секции в формате YYYYMM)',
    standard_addr_txt STRING COMMENT 'Адрес строкой после стандартизации (устарело), в варианте представления &#39;Текст&#39; (DEPRECATED)',
    start_dt DATE COMMENT 'Дата начала действия версии экземпляра по календарю бизнес-истории',
    type_code STRING COMMENT 'Адрес - код субтипа'
)
COMMENT 'Адрес: satellite table'
partitioned by (ctl_loading BIGINT,ctl_do STRING)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/diff/addr_h';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_diff.addr_h;

-- Подразделение территориальной структуры Сбербанка: hub table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_diff.div_;

create external table usl_{{ logical_env_prefix }}vspc_idl_diff.div_ (
    code STRING COMMENT 'Код подразделения территориальной структуры Сбербанка',
    ctl_action STRING COMMENT '(техническое поле) Операция (I&#x3D;insert, U&#x3D;update, D&#x3D;delete)',
    ctl_validfrom TIMESTAMP COMMENT '(техническое поле) Временная метка старта успешно завершенного обновления данной таблицы, приведшего к появлению или обновлению этой записи',
    deleted_flag BOOLEAN COMMENT 'Признак логического удаления экземпляра сущности &#39;Подразделение территориальной структуры Сбербанка&#39;',
    dirty_flag BOOLEAN COMMENT 'Признак записи не из мастер-источника, включенной в интегрированный набор для обеспечения ссылочной целостности (DATA-7)',
    div_close_dt DATE COMMENT 'Дата закрытия подразделения территориальной структуры Сбербанка',
    div_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для данного экземпляра сущности &#39;Подразделение территориальной структуры Сбербанка&#39; (для поля &#39;div_sk&#39;)',
    div_open_dt DATE COMMENT 'Дата открытия подразделения территориальной структуры Сбербанка',
    div_sk BIGINT COMMENT 'Суррогатный ключ сущности &#39;Подразделение территориальной структуры Сбербанка&#39;',
    div_time_zone_time_zone_rf_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для ссылки div_time_zone_time_zone_rf_sk',
    div_time_zone_time_zone_rf_sk BIGINT COMMENT 'Часовая зона РФ подразделения территориальной структуры Сбербанка',
    full_reg_num STRING COMMENT 'Полный регистрационный номер подразделения территориальной структуры Сбербанка',
    legal_addr_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для ссылки legal_addr_sk',
    legal_addr_sk BIGINT COMMENT 'Юридический адрес подразделения территориальной структуры Сбербанка',
    parent_div_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для ссылки parent_div_sk',
    parent_div_sk BIGINT COMMENT 'Вышестоящее подразделение территориальной структуры Сбербанка',
    reg_ord BIGINT COMMENT 'Регистрационный номер подразделения территориальной структуры Сбербанка',
    short_div_code STRING COMMENT 'Краткий код подразделения территориальной структуры Сбербанка',
    sid STRING COMMENT 'Идентификатор сущности &#39;Подразделение территориальной структуры Сбербанка&#39; в data-продукте-источнике',
    subbranch_code STRING COMMENT 'Код subbranch подразделения территориальной структуры Сбербанка'
)
COMMENT 'Подразделение территориальной структуры Сбербанка: hub table'
partitioned by (ctl_loading BIGINT,ctl_do STRING)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/diff/div_';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_diff.div_;

-- Информация о реарганизации подразделения территориальной структуры Сбербанка: link table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_diff.div_div_relation;

create external table usl_{{ logical_env_prefix }}vspc_idl_diff.div_div_relation (
    ctl_action STRING COMMENT '(техническое поле) Операция (I&#x3D;insert, U&#x3D;update, D&#x3D;delete)',
    ctl_validfrom TIMESTAMP COMMENT '(техническое поле) Временная метка старта успешно завершенного обновления данной таблицы, приведшего к появлению или обновлению этой записи',
    deleted_flag BOOLEAN COMMENT 'Признак логического удаления экземпляра сущности &#39;Информация о реарганизации подразделения территориальной структуры Сбербанка&#39;',
    div_div_relation_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для данного экземпляра сущности &#39;Информация о реарганизации подразделения территориальной структуры Сбербанка&#39; (для поля &#39;div_div_relation_sk&#39;)',
    div_div_relation_sk BIGINT COMMENT 'Суррогатный ключ сущности &#39;Информация о реарганизации подразделения территориальной структуры Сбербанка&#39;',
    end_dt DATE COMMENT 'Дата окончания действия версии экземпляра по календарю бизнес-истории',
    p1month STRING COMMENT 'Поле для секционирования по бизнес-истории (одна секция на 1 month, рассчитывается как последняя дата секции в формате YYYYMM)',
    sid STRING COMMENT 'Идентификатор сущности &#39;Информация о реарганизации подразделения территориальной структуры Сбербанка&#39; в data-продукте-источнике',
    source_div_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для ссылки source_div_sk',
    source_div_sk BIGINT COMMENT 'Подразделение территориальной структуры Сбербанка, передавшее полномочия',
    start_dt DATE COMMENT 'Дата начала действия версии экземпляра по календарю бизнес-истории',
    successor_div_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для ссылки successor_div_sk',
    successor_div_sk BIGINT COMMENT 'Подразделение территориальной структуры Сбербанка, принявшее полномочия'
)
COMMENT 'Информация о реарганизации подразделения территориальной структуры Сбербанка: link table'
partitioned by (ctl_loading BIGINT,ctl_do STRING)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/diff/div_div_relation';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_diff.div_div_relation;

-- Подразделение территориальной структуры Сбербанка: satellite table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_diff.div_h;

create external table usl_{{ logical_env_prefix }}vspc_idl_diff.div_h (
    ctl_action STRING COMMENT '(техническое поле) Операция (I&#x3D;insert, U&#x3D;update, D&#x3D;delete)',
    ctl_validfrom TIMESTAMP COMMENT '(техническое поле) Временная метка старта успешно завершенного обновления данной таблицы, приведшего к появлению или обновлению этой записи',
    disabled_access_equipment_flag BOOLEAN COMMENT 'Признак наличия оборудования для доступа маломобильных групп населения',
    div_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для данного экземпляра сущности &#39;Подразделение территориальной структуры Сбербанка&#39; (для поля &#39;div_sk&#39;)',
    div_name STRING COMMENT 'Наименование подразделения территориальной структуры Сбербанка, в варианте представления &#39;Наименование&#39;',
    div_sk BIGINT COMMENT 'Суррогатный ключ сущности &#39;Подразделение территориальной структуры Сбербанка&#39;',
    end_dt DATE COMMENT 'Дата окончания действия версии экземпляра по календарю бизнес-истории',
    fact_addr_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для ссылки fact_addr_sk',
    fact_addr_sk BIGINT COMMENT 'Фактический адрес подразделения территориальной структуры Сбербанка',
    p1month STRING COMMENT 'Поле для секционирования по бизнес-истории (одна секция на 1 month, рассчитывается как последняя дата секции в формате YYYYMM)',
    pay_sys_prtcpnt_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для ссылки pay_sys_prtcpnt_sk',
    pay_sys_prtcpnt_sk BIGINT COMMENT 'Участник платёжной системы Банка России, являющийся подразделением территориальной структуры Сбербанка',
    start_dt DATE COMMENT 'Дата начала действия версии экземпляра по календарю бизнес-истории'
)
COMMENT 'Подразделение территориальной структуры Сбербанка: satellite table'
partitioned by (ctl_loading BIGINT,ctl_do STRING)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/diff/div_h';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_diff.div_h;

-- Подразделение территориальной структуры Сбербанка: satellite table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_diff.div_type;

create external table usl_{{ logical_env_prefix }}vspc_idl_diff.div_type (
    ctl_action STRING COMMENT '(техническое поле) Операция (I&#x3D;insert, U&#x3D;update, D&#x3D;delete)',
    ctl_validfrom TIMESTAMP COMMENT '(техническое поле) Временная метка старта успешно завершенного обновления данной таблицы, приведшего к появлению или обновлению этой записи',
    div_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для данного экземпляра сущности &#39;Подразделение территориальной структуры Сбербанка&#39; (для поля &#39;div_sk&#39;)',
    div_sk BIGINT COMMENT 'Суррогатный ключ сущности &#39;Подразделение территориальной структуры Сбербанка&#39;',
    div_type_terr_div_type_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для ссылки div_type_terr_div_type_sk',
    div_type_terr_div_type_sk BIGINT COMMENT 'Вид подразделения территориальной структуры Сбербанка',
    end_dt DATE COMMENT 'Дата окончания действия версии экземпляра по календарю бизнес-истории',
    p1month STRING COMMENT 'Поле для секционирования по бизнес-истории (одна секция на 1 month, рассчитывается как последняя дата секции в формате YYYYMM)',
    start_dt DATE COMMENT 'Дата начала действия версии экземпляра по календарю бизнес-истории'
)
COMMENT 'Подразделение территориальной структуры Сбербанка: satellite table'
partitioned by (ctl_loading BIGINT,ctl_do STRING)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/diff/div_type';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_diff.div_type;

-- Вид подразделений территориальной структуры Сбербанка: hub table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_diff.terr_div_type;

create external table usl_{{ logical_env_prefix }}vspc_idl_diff.terr_div_type (
    ctl_action STRING COMMENT '(техническое поле) Операция (I&#x3D;insert, U&#x3D;update, D&#x3D;delete)',
    ctl_validfrom TIMESTAMP COMMENT '(техническое поле) Временная метка старта успешно завершенного обновления данной таблицы, приведшего к появлению или обновлению этой записи',
    deleted_flag BOOLEAN COMMENT 'Признак логического удаления экземпляра сущности &#39;Вид подразделений территориальной структуры Сбербанка&#39;',
    dirty_flag BOOLEAN COMMENT 'Признак записи не из мастер-источника, включенной в интегрированный набор для обеспечения ссылочной целостности (DATA-7)',
    div_type_code STRING COMMENT 'Код вида подразделений территориальной структуры Сбербанка',
    sid STRING COMMENT 'Идентификатор сущности &#39;Вид подразделений территориальной структуры Сбербанка&#39; в data-продукте-источнике',
    terr_div_type_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для данного экземпляра сущности &#39;Вид подразделений территориальной структуры Сбербанка&#39; (для поля &#39;terr_div_type_sk&#39;)',
    terr_div_type_sk BIGINT COMMENT 'Суррогатный ключ сущности &#39;Вид подразделений территориальной структуры Сбербанка&#39;'
)
COMMENT 'Вид подразделений территориальной структуры Сбербанка: hub table'
partitioned by (ctl_loading BIGINT,ctl_do STRING)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/diff/terr_div_type';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_diff.terr_div_type;

-- Часовая зона РФ: hub table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_diff.time_zone_rf;

create external table usl_{{ logical_env_prefix }}vspc_idl_diff.time_zone_rf (
    ctl_action STRING COMMENT '(техническое поле) Операция (I&#x3D;insert, U&#x3D;update, D&#x3D;delete)',
    ctl_validfrom TIMESTAMP COMMENT '(техническое поле) Временная метка старта успешно завершенного обновления данной таблицы, приведшего к появлению или обновлению этой записи',
    deleted_flag BOOLEAN COMMENT 'Признак логического удаления экземпляра сущности &#39;Часовая зона РФ&#39;',
    dirty_flag BOOLEAN COMMENT 'Признак записи не из мастер-источника, включенной в интегрированный набор для обеспечения ссылочной целостности (DATA-7)',
    sid STRING COMMENT 'Идентификатор сущности &#39;Часовая зона РФ&#39; в data-продукте-источнике',
    time_zone_abbr_code STRING COMMENT 'Аббревиатура часовой зоны РФ',
    time_zone_code STRING COMMENT 'Код часовой зоны РФ',
    time_zone_msk_shift_val STRING COMMENT 'Смещение часовой зоны РФ относительно Москвы',
    time_zone_name STRING COMMENT 'Наименование часовой зоны РФ, в варианте представления &#39;Наименование&#39;',
    time_zone_rf_dp_code STRING COMMENT 'Код интерпретирующего data-продукта для данного экземпляра сущности &#39;Часовая зона РФ&#39; (для поля &#39;time_zone_rf_sk&#39;)',
    time_zone_rf_sk BIGINT COMMENT 'Суррогатный ключ сущности &#39;Часовая зона РФ&#39;',
    time_zone_wrld_shift_val STRING COMMENT 'Смещение часовой зоны РФ относительно общемирового времени'
)
COMMENT 'Часовая зона РФ: hub table'
partitioned by (ctl_loading BIGINT,ctl_do STRING)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/diff/time_zone_rf';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_diff.time_zone_rf;


------------------------------------------------------------------------------------------------------------------------
-- CTL_LOG and SUCCESS_LOG tables
------------------------------------------------------------------------------------------------------------------------

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_stg.ctl_log;

create external table usl_{{ logical_env_prefix }}vspc_idl_stg.ctl_log(
        provider_product_code STRING COMMENT 'Содержит run-time мнемокод набора данных, design-time мнемокод которого указан в свойстве providerProductAlias файла метаданных data-архитектуры ЕСС.',
        provider_dataset_code STRING COMMENT 'Содержит run-time мнемокод набора данных, design-time мнемокод которого указан в свойстве providerDatasetCode файла метаданных data-архитектуры ЕСС.',
        consumer_product_code STRING COMMENT 'Заполняется константой usl_<dpcode>, где dpcode - краткий код данного data-продукта.',
        consumer_dataset_code STRING COMMENT 'Содержит run-time мнемокод набора данных, design-time мнемокод которого указан в свойстве consumerDatasetCode файла метаданных data-архитектуры ЕСС.',
        provider_loading_id   INTEGER COMMENT 'Идентификатор загрузки, зарегистрировавшей успешное изменение того состояния набора данных - поставщика, которое было использовано для успешного обновления набора данных data-продукта ЕСС - потребителя.',
        consumer_loading_id   INTEGER COMMENT 'Идентификатор загрузки, зарегистрировавшей успешное изменение состояния набора данных - потребителя.',
        upd_provider_loading_id INTEGER COMMENT 'Соответствующий идентификатор обновляющей загрузки набора данных - поставщика (для диагностики).',
        upd_consumer_loading_id INTEGER COMMENT 'Соответствующий идентификатор обновляющей загрузки набора данных - потребителя (для диагностики).',
        consumer_loading_dttm TIMESTAMP COMMENT 'Временная метка создания записи в таблице (для диагностики).',
        version STRING COMMENT 'Версия формата файла ctl_log.')
COMMENT 'Таблица успешных загрузок'
partitioned by (registry_month STRING)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/stg/ctl_log'
    tblproperties ("skip.header.line.count"="1");

msck repair table usl_{{ logical_env_prefix }}vspc_idl_stg.ctl_log;


drop table if exists usl_{{ logical_env_prefix }}vspc_idl_stg.success_log;

create external table usl_{{ logical_env_prefix }}vspc_idl_stg.success_log(
        program_unit STRING COMMENT 'Код ПЕ без префикса root.',
        upd_loading_id INTEGER COMMENT 'Значение идентификатора обновляющей загрузки')
    COMMENT 'Таблица с семафорами успешности завершения ПЕ'

    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/stg/success_log'
    tblproperties ("skip.header.line.count"="1");

msck repair table usl_{{ logical_env_prefix }}vspc_idl_stg.success_log;
