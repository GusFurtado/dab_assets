from datetime import date

from dab_wiki.datasets.governadores import Governadores, partido, vice_atual


def test_extrair(brutos):
    arquivo = Governadores().gerar(brutos("governadores"))
    por_uf = {g.uf: g for g in arquivo.dados}
    assert len(por_uf) == 27
    assert por_uf["AC"].nome == "Mailza Assis"
    assert por_uf["AC"].mandato_inicio == date(2026, 4, 2)
    assert por_uf["AL"].vice_governador == "Ronaldo Lessa"
    assert por_uf["RJ"].interino
    assert "(interino)" not in por_uf["RJ"].nome


def test_vice_atual():
    assert vice_atual("Doutor Wanderley\n(2022–2023) Ronaldo Lessa\n(2023–presente)") == "Ronaldo Lessa"
    assert vice_atual("Serafim Corrêa") == "Serafim Corrêa"
    assert vice_atual("Nenhum") is None


def test_partido():
    assert partido("Movimento Democrático Brasileiro\nMDB") == ("Movimento Democrático Brasileiro", "MDB")
    assert partido("Republicanos") == ("Republicanos", "Republicanos")
    assert partido("Sem partido") == (None, None)
