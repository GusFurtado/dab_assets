from datetime import date

from dab_wiki.datasets.municipios import Municipios, aniversario, fundacao_wikidata, gentilicos


def test_extrair(brutos):
    dataset = Municipios()
    por_nome = {m.artigo: m for m in dataset.gerar(brutos("municipios"), validar_conjunto=False).dados}
    santos = por_nome["Santos"]
    assert (santos.codigo_ibge, santos.uf, santos.regiao) == (3548500, "SP", "Sudeste")
    assert santos.gentilicos == ["santista"]
    # Wikidata só tem o ano; a data completa vem da infobox
    assert santos.fundacao == date(1546, 1, 26)
    campinas = por_nome["Campinas"]
    assert (campinas.aniversario_dia, campinas.aniversario_mes) == (14, 7)


def test_lacunas(brutos):
    comandos = Municipios().comandos_lacunas(brutos("municipios"))
    assert comandos
    assert all(c.startswith("Q") and "\tS143\tQ11921" in c for c in comandos)


def test_gentilicos():
    assert gentilicos("acriano ou acreano") == ["acriano", "acreano"]
    assert gentilicos("belenense, belemense ou belenhense (mais usado)") == ["belenense", "belemense", "belenhense"]
    assert gentilicos(None) == []


def test_fundacao_wikidata():
    assert fundacao_wikidata({"fundacao": "1546-01-01T00:00:00Z", "precisao": "9"}) == (None, 1546)
    assert fundacao_wikidata({"fundacao": "1949-01-20T00:00:00Z", "precisao": "11"}) == (date(1949, 1, 20), 1949)
    assert fundacao_wikidata({}) == (None, None)


def test_aniversario():
    assert aniversario("25 de abril") == (25, 4)
    assert aniversario("1.º de janeiro") == (1, 1)
    assert aniversario(None) == (None, None)


def test_gentilicos_compostos():
    assert gentilicos("são franciscano") == ["são franciscano"]
    assert gentilicos("(jorge-teixeirense) ou (pedra branca)") == ["jorge-teixeirense", "pedra branca"]
