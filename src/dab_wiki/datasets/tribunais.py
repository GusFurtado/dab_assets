import re
from collections import Counter
from collections.abc import Callable
from datetime import date
from typing import Literal

import pandas as pd

from dab_wiki.base import UFS, Dataset, Registro, SiglaUF
from dab_wiki.mediawiki import Pagina
from dab_wiki.tabela import Celula, EstruturaAlterada, datas, encontrar_tabela, limpar, normalizar, vazio

Tribunal = Literal["STF", "STJ", "TST", "STM", "TCU"]

# Número de cadeiras de cada tribunal, para detectar tabelas truncadas ou duplicadas.
CADEIRAS: dict[Tribunal, int] = {"STF": 11, "STJ": 33, "TST": 27, "STM": 15, "TCU": 9}


class MinistroTribunal(Registro):
    tribunal: Tribunal
    nome: str
    artigo: str | None
    posse: date | None
    ano_aposentadoria_compulsoria: int | None
    indicado_por: str | None
    vaga: str | None
    uf_origem: SiglaUF | None


def _ano(texto: str) -> int | None:
    m = re.search(r"\b(19|20)\d{2}\b", texto)
    return int(m.group()) if m else None


def _uf(texto: str) -> str | None:
    return UFS.get(limpar(texto).split("\n")[0])


def _opcional(texto: str) -> str | None:
    return None if vazio(texto) else limpar(texto).replace("\n", " ")


def _posse(texto: str) -> date | None:
    encontradas = datas(texto)
    return encontradas[0] if encontradas else None


def _nome(celula: Celula) -> tuple[str, str | None]:
    # "Flávio Dino\n(1968–)" → só o nome
    return re.sub(r"\s*\(\d{4}\s*[–-].*$", "", celula.split("\n")[0]).strip(), celula.link


def stf(pagina: Pagina) -> list[MinistroTribunal]:
    df = encontrar_tabela(pagina, ["Ministro.1", "Estado de origem", "Período", "Indicação presidencial", "Vaga"])
    registros = []
    for _, linha in df.iterrows():
        if "em exercicio" not in normalizar(linha["Período"]):
            continue
        nascimento = re.search(r"\((\d{4})\s*[–-]", linha["Ministro.1"])
        nome, artigo = _nome(linha["Ministro.1"])
        registros.append(
            MinistroTribunal(
                tribunal="STF",
                nome=nome,
                artigo=artigo,
                posse=_posse(linha["Período"]),
                ano_aposentadoria_compulsoria=int(nascimento.group(1)) + 75 if nascimento else None,
                indicado_por=_opcional(linha["Indicação presidencial"]),
                vaga=_opcional(linha["Vaga"]),
                uf_origem=_uf(linha["Estado de origem"]),
            )
        )
    return registros


def _tabela_simples(tribunal: Tribunal, colunas: dict[str, str | None]) -> Callable[[Pagina], list[MinistroTribunal]]:
    """Tribunais com tabela de composição atual: `colunas` mapeia campo → coluna da Wikipedia."""

    def extrair(pagina: Pagina) -> list[MinistroTribunal]:
        df = encontrar_tabela(pagina, [c for c in colunas.values() if c])

        def campo(linha: pd.Series, nome: str) -> Celula:
            return linha[colunas[nome]] if colunas.get(nome) else Celula("")

        registros = []
        for _, linha in df.iterrows():
            if vazio(campo(linha, "nome")):
                continue  # cadeira vaga
            nome, artigo = _nome(campo(linha, "nome"))
            registros.append(
                MinistroTribunal(
                    tribunal=tribunal,
                    nome=nome,
                    artigo=artigo,
                    posse=_posse(campo(linha, "posse")),
                    ano_aposentadoria_compulsoria=_ano(campo(linha, "aposentadoria")),
                    indicado_por=_opcional(campo(linha, "indicado_por")),
                    vaga=_opcional(campo(linha, "vaga")),
                    uf_origem=_uf(campo(linha, "uf_origem")),
                )
            )
        return registros

    return extrair


class Tribunais(Dataset):
    nome = "tribunais"
    descricao = "Composição atual do STF, STJ, TST, STM e TCU."
    _extratores: dict[str, Callable[[Pagina], list[MinistroTribunal]]] = {
        "Lista de ministros do Supremo Tribunal Federal": stf,
        "Superior Tribunal de Justiça": _tabela_simples(
            "STJ",
            {
                "nome": "Nome",
                "posse": "Data da posse",
                "aposentadoria": "Data Limite",
                "indicado_por": "Nomeação por",
                "vaga": "Vaga",
            },
        ),
        "Tribunal Superior do Trabalho": _tabela_simples(
            "TST",
            {
                "nome": "Nome",
                "posse": "Início no TST",
                "indicado_por": "Nomeação",
                "vaga": "Vaga",
                "uf_origem": "Estado de origem",
            },
        ),
        "Superior Tribunal Militar": _tabela_simples(
            "STM",
            {
                "nome": "Nome",
                "posse": "Posse no Tribunal",
                "aposentadoria": "Data limite",
                "indicado_por": "Indicação presidencial",
                "vaga": "Vaga",
            },
        ),
        "Tribunal de Contas da União": _tabela_simples(
            "TCU",
            {
                "nome": "Nome",
                "posse": "Data da posse",
                "aposentadoria": "Data limite",
                "indicado_por": "Nomeação",
                "vaga": "Origem da vaga",
                "uf_origem": "Estado de origem",
            },
        ),
    }
    paginas = list(_extratores)
    modelo = MinistroTribunal
    chave = ("tribunal", "nome")

    def extrair(self, paginas: dict[str, Pagina]) -> list[MinistroTribunal]:
        return [r for titulo, pagina in paginas.items() for r in self._extratores[titulo](pagina)]

    def validar_conjunto(self, registros: list[MinistroTribunal]) -> None:
        contagem = Counter(r.tribunal for r in registros)
        for tribunal, cadeiras in CADEIRAS.items():
            # Até 3 cadeiras vagas é normal; mais que isso sugere tabela quebrada.
            if not cadeiras - 3 <= contagem[tribunal] <= cadeiras:
                raise EstruturaAlterada(f"{tribunal}: {contagem[tribunal]} ministros para {cadeiras} cadeiras")
