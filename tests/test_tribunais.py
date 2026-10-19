from collections import Counter

from dab_wiki.datasets.tribunais import CADEIRAS, Tribunais


def test_extrair(brutos):
    ministros = Tribunais().gerar(brutos("tribunais")).dados
    contagem = Counter(m.tribunal for m in ministros)
    assert set(contagem) == set(CADEIRAS)
    assert all(contagem[t] <= n for t, n in CADEIRAS.items())
    dino = next(m for m in ministros if m.nome == "Flávio Dino")
    assert (dino.tribunal, dino.uf_origem, dino.ano_aposentadoria_compulsoria) == ("STF", "MA", 2043)
    assert dino.indicado_por == "Luiz Inácio Lula da Silva"
    # STM e TCU usam datas no formato dd/mm/aaaa
    assert all(m.posse for m in ministros if m.tribunal in ("STM", "TCU"))
