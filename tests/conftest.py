import pytest

from dab_wiki.__main__ import carregar_fixture
from dab_wiki.datasets import DATASETS


@pytest.fixture
def paginas():
    """Páginas salvas em `tests/fixtures` para um dataset (atualize com `dab-wiki fixture <dataset>`)."""

    def _paginas(nome: str):
        return {t: carregar_fixture(t) for t in DATASETS[nome].paginas}

    return _paginas
