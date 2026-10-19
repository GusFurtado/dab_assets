from datetime import date

import pytest

from dab_wiki.tabela import artigo, data, datas, limpar, vazio


@pytest.mark.parametrize(
    "texto, esperado",
    [
        ("1.º de janeiro de 2023", date(2023, 1, 1)),
        ("1º de janeiro de 2023", date(2023, 1, 1)),
        ("2 de abril de 2026[nota 2]", date(2026, 4, 2)),
        ("15 de Maio de 2022", date(2022, 5, 15)),
    ],
)
def test_data(texto, esperado):
    assert data(texto) == esperado


def test_data_invalida():
    with pytest.raises(ValueError):
        data("A definir")


def test_datas():
    assert datas("1 de março de 2020 – 5 de abril de 2024") == [date(2020, 3, 1), date(2024, 4, 5)]


def test_limpar():
    assert limpar("Fulano [nota 3] [12]  de Tal ") == "Fulano de Tal"


@pytest.mark.parametrize("texto", ["", "—", "–", "Nenhum", " vago "])
def test_vazio(texto):
    assert vazio(texto)


def test_artigo():
    assert artigo("/wiki/Paulo_Dantas") == "Paulo Dantas"
    assert artigo("/wiki/Ficheiro:Foto.jpg") is None
    assert artigo("/w/index.php?title=X&action=edit&redlink=1") is None
