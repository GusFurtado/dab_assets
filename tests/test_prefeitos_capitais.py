from datetime import date

from dab_wiki.datasets.prefeitos_capitais import PrefeitosCapitais


def test_extrair(brutos):
    por_uf = {p.uf: p for p in PrefeitosCapitais().gerar(brutos("prefeitos_capitais")).dados}
    assert len(por_uf) == 26
    assert "DF" not in por_uf
    sp = por_uf["SP"]
    assert (sp.capital, sp.nome, sp.partido_sigla) == ("São Paulo", "Ricardo Nunes", "MDB")
    assert sp.mandato_inicio == date(2021, 5, 16)
    assert sp.mandato_fim == date(2029, 1, 1)
    # "4 de março de 2026indefinido": sem fim previsto
    assert por_uf["AP"].mandato_fim is None
