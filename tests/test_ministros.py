from datetime import date

from dab_wiki.datasets.ministros import Ministros, partido_uf


def test_extrair(paginas):
    ministros = Ministros().gerar(paginas("ministros")).dados
    atuais = {m.orgao: m for m in ministros if m.em_exercicio}
    assert atuais["Fazenda"].nome == "Dario Durigan"
    assert atuais["Fazenda"].posse == date(2026, 3, 19)
    assert atuais["Casa Civil"].tipo == "Órgão com status de ministério"
    haddad = next(m for m in ministros if m.nome == "Fernando Haddad")
    assert (haddad.partido_sigla, haddad.uf_origem, haddad.saida) == ("PT", "SP", date(2026, 3, 19))
    assert any(m.interino for m in ministros)


def test_partido_uf():
    assert partido_uf("PSD/MT") == ("PSD", "MT")
    assert partido_uf("MDB/MS→PSB/SP") == ("PSB", "SP")
    assert partido_uf("Sem partido/PE") == (None, "PE")
    assert partido_uf("Sem partido") == (None, None)
    assert partido_uf("") == (None, None)
