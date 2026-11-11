"""D: dangling references (audit D1-D2): parent outside the data product, time zone outside the tiny dictionary."""
from cases import case


def d11():
    c = case(id="D11", chain="dsrc", title="D1: parent_div_sk points outside the set of divisions",
             summary="D11_1's parent 'D11_OUT' is not a division of the product; at A1 it moves to another outside "
                     "parent, at A3 to an existing one (D11_2). DEV: 29% of the divisions.")
    c.div("D11_1", headdivisioncode="D11_OUT", at={"A1": dict(headdivisioncode="D11_OUT2"), "A3": dict(headdivisioncode="D11_2")})
    c.div("D11_2", headdivisioncode="D11_OUT")


def d21():
    c = case(id="D21", chain="dsrc", title="D2: time zone reference outside the time_zone_rf dictionary",
             summary="Dictionary: one row D21_TZ. D21_1 refers to 'D21_TZX' (not in it), D21_2 to 'D21_TZ'; at A1 D21_1 "
                     "switches to the known zone and D21_2 to an unknown one. DEV: 85% of the references.")
    c.tz("D21_TZ", code=210, abbreviation="D21_TZ", name00="D21 zone", timedesignation="0", utcformat="+3")
    c.div("D21_1", address_addrfact_timezone="D21_TZX", at={"A1": dict(address_addrfact_timezone="D21_TZ")})
    c.div("D21_2", address_addrfact_timezone="D21_TZ", at={"A1": dict(address_addrfact_timezone="D21_TZY")})


for _f in [d11, d21]:
    _f()
