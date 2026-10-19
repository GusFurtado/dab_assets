import pytest

from dab_wiki.__main__ import carregar_fixture


@pytest.fixture
def brutos():
    """Dados baixados salvos em `tests/fixtures/<dataset>.json.gz` (atualize com `dab-wiki fixture <dataset>`)."""

    return carregar_fixture
