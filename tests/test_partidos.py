from datetime import date

from dab_wiki.datasets.partidos import Partidos


def test_extrair(paginas):
    partidos = Partidos().gerar(paginas("partidos")).dados
    ativos = {p.sigla: p for p in partidos if p.ativo}
    assert ativos["PT"].numero == 13
    assert ativos["PT"].data_registro == date(1982, 2, 11)
    patriota = next(p for p in partidos if p.sigla == "PATRIOTA")
    assert not patriota.ativo
    assert patriota.ano_extincao == 2023
    assert "PRD" in patriota.situacao
    assert {p.periodo for p in partidos if not p.ativo} >= {"Império (1822-1889)", "Era Vargas (1930-1937/45)"}
