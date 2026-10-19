"""Datasets montados a partir de uma consulta ao Wikidata, completados pela infobox do artigo na Wikipedia."""

from typing import Any, ClassVar

from mwparserfromhell.wikicode import Wikicode

from dab_wiki.base import Dataset, Fonte, Registro
from dab_wiki.infobox import infobox, infobox_externa
from dab_wiki.mediawiki import buscar_wikitextos
from dab_wiki.wikidata import consultar


class DatasetWikidata(Dataset):
    """Cada linha da `consulta` SPARQL vira um registro; a variável `?artigo` aponta o artigo na Wikipedia.

    O Wikidata é a fonte preferida. A infobox (`prefixo_infobox`) só preenche o que falta lá, e essas
    lacunas podem ser exportadas para o QuickStatements com `dab-wiki lacunas`.
    """

    consulta: ClassVar[str]
    prefixo_infobox: ClassVar[str] = "Info/"
    # Artigos sempre incluídos na fixture de teste, além das primeiras linhas.
    amostra: ClassVar[list[str]] = []
    tamanho_fixture: ClassVar[int] = 30

    def registro(self, linha: dict[str, str], info: dict[str, Wikicode], revid: int | None) -> Registro:
        raise NotImplementedError

    def lacunas(self, linha: dict[str, str], info: dict[str, Wikicode], revid: int | None) -> list[str]:
        """Comandos do QuickStatements para valores que estão na infobox mas faltam no Wikidata."""

        return []

    def buscar(self) -> dict:
        linhas = consultar(self.consulta)
        titulos = sorted({linha["artigo"] for linha in linhas if "artigo" in linha})
        wikitextos = buscar_wikitextos(titulos)
        # Infoboxes guardadas em predefinições dedicadas: busca a predefinição no lugar do artigo.
        externas = {}
        for titulo, w in wikitextos.items():
            if nome := infobox_externa(w["texto"], self.prefixo_infobox):
                externas[titulo] = nome
        if externas:
            predefinicoes = buscar_wikitextos(sorted(set(externas.values())), secao=None)
            for titulo, nome in externas.items():
                if nome in predefinicoes:
                    wikitextos[titulo] = {**wikitextos[titulo], "texto": predefinicoes[nome]["texto"]}
        return {"wikidata": linhas, "wikitextos": wikitextos}

    def _linhas(self, brutos: dict) -> list[tuple[dict[str, str], dict[str, Wikicode], int | None]]:
        resultado = []
        for linha in brutos["wikidata"]:
            w = brutos["wikitextos"].get(linha.get("artigo", ""))
            info = infobox(w["texto"], self.prefixo_infobox) if w else {}
            resultado.append((linha, info, w["revid"] if w else None))
        return resultado

    def processar(self, brutos: dict) -> list[Registro]:
        return [self.registro(*args) for args in self._linhas(brutos)]

    def comandos_lacunas(self, brutos: dict) -> list[str]:
        return [comando for args in self._linhas(brutos) for comando in self.lacunas(*args)]

    def fontes(self, brutos: dict) -> list[Fonte]:
        return [
            Fonte(titulo="Wikidata", url="https://query.wikidata.org/", consulta=self.consulta.strip()),
            Fonte(
                titulo=f"Wikipedia: infoboxes de {len(brutos['wikitextos'])} artigos",
                url="https://pt.wikipedia.org/",
            ),
        ]

    def reduzir(self, brutos: dict) -> dict:
        linhas = sorted(brutos["wikidata"], key=lambda linha: linha.get("artigo", ""))
        escolhidas = [linha for linha in linhas if linha.get("artigo") in self.amostra]
        escolhidas += [linha for linha in linhas[: self.tamanho_fixture] if linha not in escolhidas]
        artigos = {linha.get("artigo") for linha in escolhidas}
        return {"wikidata": escolhidas, "wikitextos": {t: w for t, w in brutos["wikitextos"].items() if t in artigos}}


def opcional(valor: Any) -> Any:
    return valor if valor not in ("", None) else None


def referencia_wikipedia(revid: int | None) -> str:
    """Referência do QuickStatements: importado da Wikipedia em português (Q11921), na revisão usada."""

    ref = "\tS143\tQ11921"
    if revid:
        ref += f'\tS4656\t"https://pt.wikipedia.org/w/index.php?oldid={revid}"'
    return ref
