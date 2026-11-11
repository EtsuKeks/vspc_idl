"""C: mass NULL foreign keys (audit C1): reg_ord, pay_sys, fact_addr, successor, cntry/rus_fed_subj, okato/oktmo."""
from cases import case


def c11():
    c = case(id="C11", chain="dsrc", title="C1: a division with every FK empty next to a twin with every FK filled",
             summary="C11_N: no BIC, no fact address, no registration number, legal address without fias/okato/oktmo/"
                     "subject/country, successor unknown. C11_F: everything filled and found in the references. Both "
                     "change their name at A1 and their legal address at A3.")
    c.div("C11_N", address_legaladdrdoc="C11 legal N", at={"A1": dict(fullname="C11 N2"), "A3": dict(address_legaladdrdoc="C11 legal N2")})
    c.div("C11_F", address_legaladdrdoc="C11 legal F", address_locationaddress="C11 fact F", requisitesdivision_bic="C11BIC",
          requisitesdivision_registrationnum="1481/11", address_addrfact_fiascode="c11-fias", address_addrfact_okatocode="c11okato",
          address_addrfact_oktmocode="c11oktmo", address_subjectcode="77", address_country="RU",
          at={"A1": dict(fullname="C11 F2"), "A3": dict(address_legaladdrdoc="C11 legal F2")})
    c.succ("C11_N", "C11_unknown", "правопреемник")
    c.succ("C11_F", "C11_N", "правопреемник")
    for t, sid in (("gar", "c11-fias"), ("okato", "c11okato"), ("oktmo", "c11oktmo"), ("pay", "C11BIC")):
        c.ref(t, sid)


for _f in [c11]:
    _f()
