from dab_wiki.datasets.ufs import UFs, arquivo_commons


def test_extrair(brutos):
    por_sigla = {u.sigla: u for u in UFs().gerar(brutos("ufs")).dados}
    assert len(por_sigla) == 27
    sp = por_sigla["SP"]
    assert (sp.codigo_ibge, sp.capital, sp.regiao) == (35, "São Paulo", "Sudeste")
    # infobox em predefinição dedicada (Predefinição:Info/Estado do Brasil/São Paulo)
    assert "paulista" in sp.gentilicos
    assert por_sigla["AC"].gentilicos == ["acriano", "acreano"]
    assert all(u.gentilicos for u in por_sigla.values())


def test_arquivo_commons():
    url = "http://commons.wikimedia.org/wiki/Special:FilePath/Bandeira%20do%20Acre.svg"
    assert arquivo_commons(url) == "Bandeira do Acre.svg"
