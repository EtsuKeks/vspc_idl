"""G: staging leftovers (audit G1): garbage in the 3.0 staging diff tables and chg_div_h, a stale ctl_log record,
4.0-style *_diff accumulators, an emptied div_type loader cache."""
from model import GONE, copy_rows, div_sk_of
from cases import case


def g11():
    c = case(id="G11", chain="dtgt", title="G1: staging pre-filled with garbage of earlier runs + stale ctl_log",
             summary="Seeded before T1: stg.diff_div_ / stg.diff_div_h get legs of G11_1 under the old loading 1, "
                     "stg.chg_div_h a stale row, stg.ctl_log one record of 2026-06-03, and 4.0-style accumulators "
                     "(all optional: 3.0 drops its stg tables after the move, so in the 3.0 root most of them do not exist) "
                     "stg.div__diff / stg.div_h_diff (created if missing) get the same legs. G11_1 changes at T1 and T3. "
                     "DEV: div__diff 142M rows, div_h_diff 33M, ctl_log one 2026-06-03 record.")
    c.div("G11_1", at={"T1": dict(shortdivcode="02", fullname="G11 T1"), "T3": dict(fullname="G11 T3")})
    S, D = "usl_vspc_idl_stg", "usl_vspc_idl_snp"
    c.sql("T1",
          "/*opt*/" + copy_rows(S + ".diff_div_", D + ".div_", "sid = 'G11_1'", "stg.diff_div_", "ctl_loading=1, ctl_do='R'", ctl_action="'U'"),
          "/*opt*/" + copy_rows(S + ".diff_div_h", D + ".div_h", div_sk_of("G11_1"), "stg.diff_div_h", "ctl_loading=1, ctl_do='R'",
                    ctl_action="'U'", p1month="'999912'"),
          "/*opt*/insert into %s.chg_div_h select * from %s.chg_div_h where fulldivcode = 'G11_1'" % (S, S),
          "/*opt*/insert into %s.ctl_log partition(registry_month='202606') values ('usl_ebx_rep', 'sfsdivision', 'usl_vspc_idl', "
          "'div', 1, 1, 1, 1, timestamp'2026-06-03 10:00:00', '1')" % S,
          "/*opt*/create table if not exists %s.div__diff like usl_vspc_idl_diff.div_" % S,
          "/*opt*/create table if not exists %s.div_h_diff like usl_vspc_idl_diff.div_h" % S,
          "/*opt*/" + copy_rows(S + ".div__diff", D + ".div_", "sid = 'G11_1'", "diff.div_", "ctl_loading=1, ctl_do='R'", ctl_action="'U'"),
          "/*opt*/" + copy_rows(S + ".div_h_diff", D + ".div_h", div_sk_of("G11_1"), "diff.div_h", "ctl_loading=1, ctl_do='R'",
                                ctl_action="'U'", p1month="'999912'"))


def g12():
    c = case(id="G12", chain="dtgt", title="G1: emptied div_type loader cache before a returner",
             summary="G12_1 vanishes at T1 (div_type closed at today-1; 3.0 stores the end in its loader cache), the "
                     "cache is truncated before T3 (DEV: loader_cache_div_type empty), G12_1 returns at T3. 3.0 takes the "
                     "returner start from the cache, 4.0 from the closed versions in snp.")
    c.div("G12_1", at={"T1": GONE, "T3": {}})
    # the 3.0 cache (stg.loader_cache_div_type: cache_div_sk, cache_end_dt) is EXTERNAL (TRUNCATE is refused): overwrite
    # it with nothing; 4.0 has no such table (the statement is skipped there)
    c.sql("T3", "/*opt*/insert overwrite table usl_vspc_idl_stg.loader_cache_div_type select cast(null as bigint), "
                "cast(null as date) where 1 = 0")

for _f in [g11, g12]:
    _f()
