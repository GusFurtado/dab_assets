import pytest

from dab_wiki.datasets import DATASETS


@pytest.mark.parametrize("nome", DATASETS)
def test_arquivo_publicado_valido(nome):
    DATASETS[nome].validar_arquivo()


@pytest.mark.online
@pytest.mark.parametrize("nome", DATASETS)
def test_pagina_atual(nome):
    """Raspa a revisão atual da Wikipedia (`pytest -m online`)."""

    DATASETS[nome].gerar()
