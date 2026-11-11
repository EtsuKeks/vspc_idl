"""Synthetic-data model: timeline, source DDL, case DSL, SQL rendering.

Nothing here knows about individual cases; cases live in cases.py.
Every value that reaches a source table is set by a case through this DSL, so a source row
can always be traced back to the case (and the case id is part of every natural key).
"""
import datetime as _dt

# --------------------------------------------------------------------------------------------
# Timeline. One list of loads per chain. Business date of a load = date part of its fake time.
# loadingId is passed to both versions unchanged, so ctl_loading can be compared exactly.
# --------------------------------------------------------------------------------------------


class Load:
    def __init__(self, label, mode, faketime, loading_id, why, by="both"):
        self.label, self.mode, self.faketime, self.loading_id, self.why = label, mode, faketime, loading_id, why
        self.by = by   # "30": run by 3.0 only; the 4.0 half of the chain starts from the 3.0 state after it (cutover)
        self.date = _dt.date.fromisoformat(faketime[:10])


class Chain(list):
    """A chain of loads run in order on one stand root per version. `modes` = run modes it covers (CHAINS.md)."""

    def __init__(self, purpose, modes, loads, types=None):
        super().__init__(loads)
        self.purpose, self.modes = purpose, modes
        self.types = types or {}   # source column type overrides for this chain: {"m_sfs.shortdivcode": "int"}


def _days(prefix, first_id, spec):
    """[(mode, 'yyyy-mm-dd HH:MM', why[, by])] -> Loads prefix0.. with consecutive loadingIds."""
    return [Load("%s%d" % (prefix, i), x[0], x[1] + ":00", first_id + i, x[2], *x[3:]) for i, x in enumerate(spec)]


CHAINS = {
    "dsrc": Chain("SOURCE-side dirt from the DEV audit (dev_data_anomalies_20261109.txt): dirty values flow from the "
                  "sources through both pipelines.",
                  "INIT; INCR next day; 2nd INCR the same day; INCR on the 1st of the month", _days("A", 11000, [
        ("INIT", "2027-01-13 10:00", "INIT with dirty sources (d_ history back to 2023, spread over months)."),
        ("INCR", "2027-01-14 10:00", "Consecutive day: some dirty keys change one field, others stay unchanged."),
        ("INCR", "2027-01-14 16:00", "Second INCR the same day: unchanged payload again / another field change."),
        ("INCR", "2027-02-01 10:00", "1st of month (closures land in 202701): changes over the dirty history."),
    ])),
    "dtgt": Chain("TARGET-side dirt: states that 3.0/PROD already left in snp/diff/stg (twins, double open versions, "
                  "overlapping intervals, junk legs, p1month layouts, staging garbage) are seeded by SQL after the INIT, "
                  "identically in both roots; the following INCRs must live with them.",
                  "INIT; dirt seeded; INCR next day; 2nd INCR the same day; INCR on the 1st of the month", _days("T", 12000, [
        ("INIT", "2027-01-13 10:00", "Clean INIT of the keys that get dirt."),
        ("INCR", "2027-01-14 10:00", "Dirt seeded into snp/diff/stg before this INCR; some dirty keys change."),
        ("INCR", "2027-01-14 16:00", "Second INCR the same day over the dirty state."),
        ("INCR", "2027-02-01 10:00", "1st of month: rewrite zone over the dirty / mis-partitioned versions; loader cache emptied before."),
    ])),
    "dg1": Chain("CUTOVER over RT-FW staging leftovers (audit G1): 3.0 INIT+INCR, leftovers seeded into stg by a pre-step "
                 "(stand.sh PRESQL_DIR) before U1, then 3.0 or 4.0 continue.",
                 "INIT 3.0; INCR 3.0; INCR both; INCR 1st of month both", _days("U", 13000, [
        ("INIT", "2027-01-13 10:00", "3.0 INIT.", "30"),
        ("INCR", "2027-01-14 10:00", "3.0 INCR over the seeded leftovers.", "30"),
        ("INCR", "2027-01-14 16:00", "Second INCR the same day: 3.0 vs 4.0 (4.0 from the 3.0 root)."),
        ("INCR", "2027-02-01 10:00", "1st of month; loader cache emptied before; returner U12_1."),
    ])),
}

MIN, MAX = "1900-01-01", "9999-12-31"


def d(label, delta=0, chain=None):
    """Business date of a load plus delta days, ISO string. d('L3', -1) = D-1 of L3."""
    for ch in ([chain] if chain else CHAINS):
        for ld in CHAINS[ch]:
            if ld.label == label:
                return (ld.date + _dt.timedelta(days=delta)).isoformat()
    raise KeyError(label)


def p1month(date_iso):
    return "999912" if date_iso == MAX else date_iso[:4] + date_iso[5:7]


# --------------------------------------------------------------------------------------------
# Source tables. Layouts = DESCRIBE of the real DEV tables (FESS_DEV, 2026-10-26; same column
# order and types). Only the columns in READS are ever filled by a case; the rest stay NULL.
# Reference tables (gar_fias/rdm) are minimal: only the three columns both versions read.
# --------------------------------------------------------------------------------------------

_M_SFS = """addinformation:string address_addrfact_addinformation:string address_addrfact_addrarea_areaname:string address_addrfact_addrarea_objtype:string address_addrfact_addrcity_cityname:string address_addrfact_addrcity_objtype:string address_addrfact_addrhouse_housenumbers:string address_addrfact_addrhouse_objtype:string address_addrfact_addrplace_placename:string address_addrfact_addrplace_objtype:string address_addrfact_addrstreet_streetname:string address_addrfact_addrstreet_objtype:string address_addrfact_postcode:string address_addrfact_propertymanagmentsystemid:string address_addrfact_enddateaddress:string address_addrfact_fiascode:string address_addrfact_geographiccoordinates_geographiclatitude:string address_addrfact_geographiccoordinates_geographiclongitude:string address_addrfact_ismainaddress:string address_addrfact_metrostation:string address_addrfact_okatocode:string address_addrfact_oktmocode:string address_addrfact_timezone:string address_addrfact_startdateaddress:string address_addrfact_parkplacecount:string address_country:string address_isidenticaladdresses:string address_legaladdrdoc:string address_locationaddress:string address_placestatus:string address_placetype:string address_ppkoroute:string address_subjectcode:string address_postcode:string bushvsp_codeauthorizedvsp:string bushvsp_codebport:string businessformats_businessformatadd1:string businessformats_businessformatadd2:string businessformats_businessformatmain:string businessstatus:string casnsicode:string changeplandate:string closedate:timestamp contactmanagers_email:string contactmanagers_manager:string contactmanagers_phonemanager:string divisiontype00:string divisionkind:string facilities_ismeetsrequirements:string facilities_facilitystatus:string facilities_ismoneyinsurance:string foreignexchangelaw:string fulldivcode:string fullname:string headcount_groupposcount:string headcount_groupposvacancy:string headcount_positioncode:string headcount_positionname:string headdivision:string holidaymode_holidayschedule:string holidaymode_modetype:string id:string individualname:string modificationattributes_createdby:string modificationattributes_creationdate:string modificationattributes_modificationdate:string modificationattributes_modifiedby:string osbcode:string opendate:timestamp overallcount:string requisitesdivision_bic:string requisitesdivision_correspondentacc:string requisitesdivision_idparticipant:string requisitesdivision_inn:string requisitesdivision_kpp:string requisitesdivision_okpo:string requisitesdivision_registrationdate:string requisitesdivision_registrationnum:string serving_isbusinessservice:string serving_ischildrencorner:string serving_iscustomerreception:string serving_isdisabledequip:string serving_isdisplayexternresources:string serving_isdivisiongated:string serving_isgivebiodata:string serving_ispersonservice:string serving_istargetvsp:string serving_issurdo:string serving_isvisuallyimpaired:string serving_iszone24withselfservicedev:string serving_statevehiclenumber:string serving_reasonnotdisplayextres:string shortdivcode:string statreportcode:string subordination_oldcasnsicode:string subordination_oldstatrepcode:string subordination_subordinationdate:string tbcode:string vspcode:string headdivisioncode:string ctl_loading:string"""

_D_SFS = """addinformation:string address_addrfact_addinformation:string address_addrfact_addrarea_areaname:string address_addrfact_addrarea_objtype:string address_addrfact_addrcity_cityname:string address_addrfact_addrcity_objtype:string address_addrfact_addrhouse_housenumbers:string address_addrfact_addrhouse_objtype:string address_addrfact_addrplace_placename:string address_addrfact_addrplace_objtype:string address_addrfact_addrstreet_streetname:string address_addrfact_addrstreet_objtype:string address_addrfact_postcode:string address_addrfact_propertymanagmentsystemid:string address_addrfact_enddateaddress:timestamp address_addrfact_fiascode:string address_addrfact_geographiccoordinates_geographiclatitude:string address_addrfact_geographiccoordinates_geographiclongitude:string address_addrfact_ismainaddress:string address_addrfact_metrostation:string address_addrfact_okatocode:string address_addrfact_oktmocode:string address_addrfact_timezone:string address_addrfact_startdateaddress:timestamp address_addrfact_parkplacecount:string address_country:string address_isidenticaladdresses:string address_legaladdrdoc:string address_locationaddress:string address_placestatus:string address_placetype:string address_ppkoroute:string address_subjectcode:string address_postcode:string bushvsp_codeauthorizedvsp:string bushvsp_codebport:string businessformats_businessformatadd1:string businessformats_businessformatadd2:string businessformats_businessformatmain:string businessstatus:string casnsicode:string changeplandate:timestamp closedate:timestamp contactmanagers_email:string contactmanagers_manager:string contactmanagers_phonemanager:string divisiontype:string divisionkind:string facilities_ismeetsrequirements:string facilities_facilitystatus:string facilities_ismoneyinsurance:string foreignexchangelaw:string fulldivcode:string fullname:string headcount_groupposcount:string headcount_groupposvacancy:string headcount_positioncode:string headcount_positionname:string headdivision:string holidaymode_holidayschedule:string holidaymode_modetype:string id:string individualname:string modificationattributes_createdby:string modificationattributes_creationdate:timestamp modificationattributes_modificationdate:timestamp modificationattributes_modifiedby:string osbcode:string opendate:timestamp overallcount:string requisitesdivision_bic:string requisitesdivision_correspondentacc:string requisitesdivision_idparticipant:string requisitesdivision_inn:string requisitesdivision_kpp:string requisitesdivision_okpo:string requisitesdivision_registrationdate:timestamp requisitesdivision_registrationnum:string serving_isbusinessservice:string serving_ischildrencorner:string serving_iscustomerreception:string serving_isdisabledequip:string serving_isdisplayexternresources:string serving_isdivisiongated:string serving_isgivebiodata:string serving_ispersonservice:string serving_istargetvsp:string serving_issurdo:string serving_isvisuallyimpaired:string serving_iszone24withselfservicedev:string serving_statevehiclenumber:string serving_reasonnotdisplayextres:string shortdivcode:string statreportcode:string subordination_oldcasnsicode:string subordination_oldstatrepcode:string subordination_subordinationdate:timestamp tbcode:string vspcode:string headdivisioncode:string ctl_do:string ctl_action:string snapshot_:string ctl_validfrom:timestamp ctl_loading:int"""

_SUCC = "id:string idx:int successors_id_successor:string successors_successordivision:string successors_successorsdate:timestamp successors_successorstype:string"
_TZ = "code:int name00:string abbreviation:string timedesignation:string utcformat:string ctl_loading:int"
_REF = "sid:string deleted_flag:boolean dirty_flag:boolean"


def _cols(spec):
    return [tuple(c.split(":")) for c in spec.split()]


# short table alias -> (full name, columns)
SOURCES = {
    "m_sfs": ("usl_vspc_idl_in_snp.ebx_rep_m_sfs_0002_sfsdivision", _cols(_M_SFS)),
    "d_sfs": ("usl_vspc_idl_in_diff.ebx_rep_d_sfs_0002_sfsdivision", _cols(_D_SFS)),
    "succ": ("usl_vspc_idl_in_snp.ebx_rep_m_sfs_0002_sfsdivision_successors", _cols(_SUCC)),
    "tz": ("usl_vspc_idl_in_snp.ebx_rep_m_rfzonetime_0002_rfzonetime", _cols(_TZ)),
    "gar": ("usl_gar_fias_idl_snp.addr", _cols(_REF)),
    "okato": ("usl_rdm_idl_spark_snp.okato2", _cols(_REF)),
    "oktmo": ("usl_rdm_idl_spark_snp.oktmo2", _cols(_REF)),
    "pay": ("usl_rdm_idl_spark_snp.pay_sys_prtcpnt", _cols(_REF)),
}

_CHG3 = "3.0 tables/chgdivh/ChgDivHSql.java:14-87 (INCR), :211-290 (INIT)"
_CHG4 = "4.0 transformations/ChgDivHTransformation.java:38-111 (INCR), :237-316 (INIT)"
_DIVATTR = [
    "fulldivcode", "fullname", "shortdivcode", "closedate", "opendate", "headdivisioncode",
    "requisitesdivision_bic", "requisitesdivision_registrationnum", "address_locationaddress",
    "address_addrfact_fiascode", "address_addrfact_timezone", "serving_isdisabledequip",
    "address_addrfact_postcode", "address_postcode", "address_addrfact_addrarea_areaname",
    "address_addrfact_addrplace_placename", "address_addrfact_addrcity_cityname",
    "address_addrfact_addrhouse_housenumbers", "address_addrfact_addrstreet_streetname", "address_country",
    "address_addrfact_okatocode", "address_addrfact_oktmocode", "address_subjectcode", "address_legaladdrdoc"]

# Which source columns 3.0 / 4.0 actually read (and where). gen.py asserts that every column here
# exists in the DDL above and that cases fill no other column (except ctl/technical ones marked *).
READS = {
    "m_sfs": dict(
        [(c, _CHG3 + "; " + _CHG4) for c in _DIVATTR] + [
            ("divisiontype00", "3.0 DivTypeTransformer.java:39, TerrDivTypeTransformer.java:33; 4.0 DivTypeTransformation.java:52, TerrDivTypeTransformation.java:36"),
            ("id", "3.0 DivDivRelationTransformer.java:35-42; 4.0 DivDivRelationTransformation.java:47-55"),
            ("ctl_loading", "* technical: not read by 3.0; dropped by the 4.0 reader (rt-fw-reader SnpReader USL_TECH_COLS_SNP)"),
        ]),
    "d_sfs": dict(
        [(c, "INIT only: " + _CHG3.replace(":14-87 (INCR), ", ":") + "; 4.0 ChgDivHTransformation.java:237-276") for c in _DIVATTR if c != "address_addrfact_postcode"] + [
            ("address_addrfact_postcode", "INIT inner select only (not projected to chg_div_h): 3.0 ChgDivHSql.java:225; 4.0 :251"),
            ("snapshot_", "filter snapshot_ LIKE '%DIF%': 3.0 ChgDivHSql.java:249; 4.0 :275"),
            ("ctl_do", "filter ctl_do = 'R': 3.0 ChgDivHSql.java:250; 4.0 :276"),
            ("ctl_validfrom", "becomes start_dt of the version: 3.0 ChgDivHSql.java:245; 4.0 :271"),
            ("ctl_action", "* technical: not read"),
            ("ctl_loading", "* technical: not read (row order is by ctl_validfrom)"),
        ]),
    "succ": {
        "id": "3.0 DivDivRelationTransformer.java:35,40; 4.0 DivDivRelationTransformation.java:51",
        "successors_id_successor": "3.0 DivDivRelationTransformer.java:42; 4.0 :52",
        "successors_successorstype": "3.0 DivDivRelationTransformer.java:43; 4.0 :53,56",
        "idx": "* technical: not read (used only to make duplicate successor rows distinct in the source)",
    },
    "tz": {
        "abbreviation": "3.0 TimeZoneTransformer.java:32-41; 4.0 TimeZoneTransformation.java:34-43",
        "code": "3.0 TimeZoneTransformer.java:34 (int, no cast); 4.0 :36 (cast to string)",
        "timedesignation": "3.0 :35; 4.0 :37",
        "name00": "3.0 :36; 4.0 :38",
        "utcformat": "3.0 :39; 4.0 :41",
        "ctl_loading": "* technical: not read",
    },
}
for _r in ("gar", "okato", "oktmo", "pay"):
    READS[_r] = {c: "lookup in chg_div_h preselect: 3.0 ChgDivHSql.java:47-87, :299-338; 4.0 ChgDivHTransformation.java:71-111, :325-364"
                 for c in ("sid", "deleted_flag", "dirty_flag")}


def check_reads():
    for t, cols in READS.items():
        names = [c for c, _ in SOURCES[t][1]]
        for c in cols:
            assert c in names, "READS: %s.%s is not in the source DDL" % (t, c)


# --------------------------------------------------------------------------------------------
# Case DSL
# --------------------------------------------------------------------------------------------

GONE = "__GONE__"   # value of an `at` entry: the row is absent from this load on


class SrcRow:
    """One logical source row whose content may change from load to load.

    base: attributes at the first load where the row exists (start).
    at:   {load_label: dict of changed attributes | GONE}. Changes accumulate; a dict after GONE
          brings the row back (with all earlier changes plus the new ones).
    """

    def __init__(self, case, table, label, base, at=None, start=None):
        self.case, self.table, self.label, self.base = case, table, label, dict(base)
        self.at = dict(at or {})
        self.start = start
        for k in self.base:
            if k not in READS[table]:
                raise ValueError("%s: column %s.%s is read by neither 3.0 nor 4.0" % (case.id, table, k))
        for v in self.at.values():
            if v != GONE:
                for k in v:
                    if k not in READS[table]:
                        raise ValueError("%s: column %s.%s is read by neither 3.0 nor 4.0" % (case.id, table, k))

    def states(self, chain):
        """[(load, dict or None)] for every load of the chain."""
        out, cur, present = [], dict(self.base), self.start is None
        for ld in CHAINS[chain]:
            if self.start == ld.label:
                present = True
            ch = self.at.get(ld.label)
            if ch == GONE:
                present = False
            elif ch is not None:
                cur.update(ch)
                present = True
            out.append((ld, dict(cur) if present else None))
        return out


class Expect:
    def __init__(self, case, load, table, key, rows, note=None):
        self.case, self.load, self.table, self.key, self.rows, self.note = case, load, table, key, rows, note


class Case:
    def __init__(self, id, title, chain="main", rules=(), changes=(), scenario="", tables=(), summary="",
                 deterministic=True, known40="", accepted=""):
        self.id, self.title, self.chain = id, title, chain
        self.rules, self.changes, self.scenario, self.tables = list(rules), list(changes), scenario, list(tables)
        self.summary, self.deterministic = summary, deterministic
        self.known40 = known40   # observed / known behaviour of the 4.0 baseline, for the reader (not checked)
        self.accepted = accepted  # non-empty: a documented, accepted 3.0/4.0 deviation (the comparator says ACCEPTED)
        self.rows, self.expects, self.notes, self.special_keys = [], [], {}, {}
        self.sqls = []   # (load, sql): statements run against the TARGET/stg tables before that load (target-side dirt)

    # ---- source rows ----
    def div(self, key, at=None, start=None, label=None, **attrs):
        """A division row in the snapshot ebx_rep_m_sfs_0002_sfsdivision. key = fulldivcode = id.

        Defaults: no addresses, no time zone, no BIC, no parent, no registration number (so reg_ord is NULL
        everywhere except in C25, which owns that behaviour), type '<case>_T', disabled-equipment flag '0'."""
        base = dict(fulldivcode=key, id=key, fullname="%s name" % key, shortdivcode="01",
                    opendate="2020-01-01 00:00:00",
                    serving_isdisabledequip="0", divisiontype00="%s_T" % self.id, ctl_loading="1")
        base.update(attrs)
        r = SrcRow(self, "m_sfs", label or key, base, at, start)
        self.rows.append(r)
        return r

    def hist(self, div_row, validfrom, ctl_loading, start=None, ctl_do="R", snapshot_="DIF", at=None, **attrs):
        """A history row in the diff table ebx_rep_d_sfs_0002_sfsdivision, derived from the division's base."""
        base = {k: v for k, v in div_row.base.items() if k in READS["d_sfs"]}
        base.update(dict(ctl_do=ctl_do, ctl_action="U", snapshot_=snapshot_, ctl_validfrom=validfrom, ctl_loading=ctl_loading))
        base.update(attrs)
        r = SrcRow(self, "d_sfs", "%s@%s" % (div_row.label, validfrom), base, at, start)
        self.rows.append(r)
        return r

    def succ(self, src_id, succ_id, stype, idx=0, at=None, start=None):
        r = SrcRow(self, "succ", "%s->%s#%d" % (src_id, succ_id, idx),
                   dict(id=src_id, idx=idx, successors_id_successor=succ_id, successors_successorstype=stype), at, start)
        self.rows.append(r)
        return r

    def tz(self, label, at=None, start=None, **attrs):
        base = dict(ctl_loading=1)
        base.update(attrs)
        r = SrcRow(self, "tz", label, base, at, start)
        self.rows.append(r)
        return r

    def ref(self, table, sid, deleted=False, dirty=False, at=None, start=None):
        r = SrcRow(self, table, sid, dict(sid=sid, deleted_flag=deleted, dirty_flag=dirty), at, start)
        self.rows.append(r)
        return r

    # ---- expectations (3.0 truth) ----
    def expect(self, load, table, key, rows, note=None):
        self.expects.append(Expect(self, load, table, key, rows, note))

    def sql(self, load, *statements):
        """Raw SQL run right after the sources of `load` are applied and before the product runs (both versions)."""
        for st in statements:
            assert "\n" not in st.strip() and not st.strip().endswith(";"), "one statement per line, no ';'"
            self.sqls.append((load, st.strip()))

    def note(self, load, text):
        self.notes.setdefault(load, []).append(text)

    def owns(self, sid, why):
        """A natural key that does not contain the case id (e.g. 'KALT', NULL) belongs to this case."""
        self.special_keys[sid] = why


# expected-row helpers ------------------------------------------------------------------------
NOTNULL = "<notnull>"
NULL = None


def ver(start, end, vf, ld, **attrs):
    """Snapshot row of a historical table (div_h, addr_h, div_type, div_div_relation)."""
    r = dict(start_dt=start, end_dt=end, ctl_validfrom=vf, ctl_loading=ld)
    r.update(attrs)
    return r


def leg(do, action, start, end, vf, **attrs):
    """Diff row of a historical table (ctl_loading = the load being checked)."""
    r = dict(ctl_do=do, ctl_action=action, start_dt=start, end_dt=end, ctl_validfrom=vf)
    r.update(attrs)
    return r


def hub(vf, ld, **attrs):
    r = dict(ctl_validfrom=vf, ctl_loading=ld)
    r.update(attrs)
    return r


def hleg(do, action, vf, **attrs):
    r = dict(ctl_do=do, ctl_action=action, ctl_validfrom=vf)
    r.update(attrs)
    return r


def RUN(label):
    return "RUN@" + label


START = "START"


def addr_key(durty=None, src=None, okato=None, oktmo=None):
    """3.0 AddrTransformer.getKey('|') (AddrTransformer.java:248-263): NULL parts become 'NULL'."""
    if durty is None and src is None and okato is None and oktmo is None:
        return None
    return "|".join("NULL" if p is None else p for p in (durty, src, okato, oktmo))


# --------------------------------------------------------------------------------------------
# SQL rendering
# --------------------------------------------------------------------------------------------


def _lit(v, typ):
    if v is None:
        return "NULL"
    if typ == "string":
        s = str(v)
        assert "\\" not in s and "\n" not in s and not s.rstrip().endswith(";"), "unsupported literal %r" % s
        return "'" + s.replace("'", "\\'") + "'"
    if typ == "timestamp":
        s = str(v)
        if len(s) == 10:
            s += " 00:00:00"
        return "timestamp'%s'" % s
    if typ == "boolean":
        return "true" if v else "false"
    if typ == "int":
        return str(int(v))
    raise ValueError(typ)


# target / stg column lists (3.0 DDL order, partition columns excluded) for target-side dirt
TCOLS = {
    "snp.div_": "code ctl_loading ctl_validfrom deleted_flag dirty_flag div_close_dt div_dp_code div_open_dt div_sk "
                "div_time_zone_time_zone_rf_dp_code div_time_zone_time_zone_rf_sk full_reg_num legal_addr_dp_code "
                "legal_addr_sk parent_div_dp_code parent_div_sk reg_ord short_div_code sid subbranch_code",
    "snp.div_h": "ctl_loading ctl_validfrom disabled_access_equipment_flag div_dp_code div_name div_sk end_dt "
                 "fact_addr_dp_code fact_addr_sk pay_sys_prtcpnt_dp_code pay_sys_prtcpnt_sk start_dt",
    "snp.div_type": "ctl_loading ctl_validfrom div_dp_code div_sk div_type_terr_div_type_dp_code div_type_terr_div_type_sk end_dt start_dt",
    "stg.diff_div_": "code ctl_action ctl_validfrom deleted_flag dirty_flag div_close_dt div_dp_code div_open_dt div_sk "
                     "div_time_zone_time_zone_rf_dp_code div_time_zone_time_zone_rf_sk full_reg_num legal_addr_dp_code "
                     "legal_addr_sk parent_div_dp_code parent_div_sk reg_ord short_div_code sid subbranch_code",
    "diff.div_": "code ctl_action ctl_validfrom deleted_flag dirty_flag div_close_dt div_dp_code div_open_dt div_sk "
                 "div_time_zone_time_zone_rf_dp_code div_time_zone_time_zone_rf_sk full_reg_num legal_addr_dp_code "
                 "legal_addr_sk parent_div_dp_code parent_div_sk reg_ord short_div_code sid subbranch_code",
    "diff.div_h": "ctl_action ctl_validfrom disabled_access_equipment_flag div_dp_code div_name div_sk end_dt "
                  "fact_addr_dp_code fact_addr_sk p1month pay_sys_prtcpnt_dp_code pay_sys_prtcpnt_sk start_dt",
    "stg.diff_div_h": "ctl_action ctl_validfrom div_dp_code div_name div_sk end_dt fact_addr_dp_code fact_addr_sk "
                      "pay_sys_prtcpnt_dp_code pay_sys_prtcpnt_sk p1month start_dt",
}


def copy_rows(dst, src, where, cols, partition=None, **override):
    """INSERT INTO <dst> [PARTITION(...)] SELECT <cols of `cols`, some replaced by SQL expressions> FROM <src> WHERE ..."""
    sel = ", ".join("%s AS %s" % (override[c], c) if c in override else c for c in TCOLS[cols].split())
    part = " partition(%s)" % partition if partition else ""
    return "insert into %s%s select %s from %s where %s" % (dst, part, sel, src, where)


def div_sk_of(sid):
    """Rows of one division as the product wrote them (seeded rows carry ctl_loading 900 and are never copied again)."""
    return "div_sk in (select div_sk from usl_vspc_idl_snp.div_ where sid = '%s') and ctl_loading <> 900" % sid


def render_load_sql(chain, load_label, rows, extra=()):
    """Full source state for one load: drop+create every source table, insert all rows present at this load."""
    out = ["-- generated by synth/gen.py -- chain=%s load=%s -- DO NOT EDIT (edit synth/cases.py)" % (chain, load_label)]
    by_table = {}
    for r in rows:
        for ld, st in r.states(chain):
            if ld.label == load_label and st is not None:
                by_table.setdefault(r.table, []).append((r, st))
    for t, (full, cols) in SOURCES.items():
        ov = CHAINS[chain].types
        cols = [(c, ov.get("%s.%s" % (t, c), ty)) for c, ty in cols]
        types = dict(cols)
        out.append("drop table if exists %s;" % full)
        out.append("create table %s (\n  %s\n) stored as parquet;" % (full, ",\n  ".join("`%s` %s" % c for c in cols)))
        items = by_table.get(t, [])
        if not items:
            out.append("-- %s: no rows at %s" % (t, load_label))
            continue
        used = [c for c, _ in cols if any(c in st for _, st in items)]
        out.append("insert into %s (%s) values" % (full, ", ".join(used)))
        items = sorted(items, key=lambda x: (x[0].case.id, x[0].label))
        for i, (r, st) in enumerate(items):
            # one row per line, trailing comment names the case; the stand's splitter ends a statement at a
            # line ending with ';', so the terminating ';' goes on its own line
            out.append("  (%s)%s  -- %s %s" % (", ".join(_lit(st.get(c), types[c]) for c in used),
                                               "," if i < len(items) - 1 else "", r.case.id, r.label))
        out.append(";")
    if extra:
        out.append("-- target-side dirt seeded before this load (identical for 3.0 and 4.0)")
        for cid, st in extra:
            out.append("-- " + cid)
            out.append(st)
            out.append(";")
    return "\n".join(out) + "\n"
