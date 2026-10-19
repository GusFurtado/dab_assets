import re
from urllib.parse import unquote

from mwparserfromhell.wikicode import Wikicode

from dab_wiki.base import CODIGOS_UF, REGIOES, Regiao, Registro, SiglaUF
from dab_wiki.datasets._wikidata import DatasetWikidata, opcional, referencia_wikipedia
from dab_wiki.datasets.municipios import gentilicos
from dab_wiki.infobox import texto
from dab_wiki.wikidata import qid

CONSULTA = """
SELECT ?item ?iso ?nome ?artigo ?capital ?area ?coordenadas ?bandeira ?brasao
  (GROUP_CONCAT(DISTINCT ?gentilico; separator="|") AS ?gentilicos) (SAMPLE(?lema_) AS ?lema)
WHERE {
  ?item wdt:P300 ?iso . FILTER(STRSTARTS(?iso, "BR-"))
  OPTIONAL { ?item rdfs:label ?nome FILTER(LANG(?nome) = "pt") }
  OPTIONAL { ?item wdt:P36 ?c . ?c rdfs:label ?capital FILTER(LANG(?capital) = "pt") }
  OPTIONAL { ?item wdt:P2046 ?area }
  OPTIONAL { ?item wdt:P625 ?coordenadas }
  OPTIONAL { ?item wdt:P41 ?bandeira }
  OPTIONAL { ?item wdt:P94 ?brasao }
  OPTIONAL { ?item wdt:P1549 ?gentilico FILTER(LANG(?gentilico) = "pt") }
  OPTIONAL { ?item wdt:P1451 ?lema_ }
  OPTIONAL { ?a schema:about ?item; schema:isPartOf <https://pt.wikipedia.org/>; schema:name ?artigo }
}
GROUP BY ?item ?iso ?nome ?artigo ?capital ?area ?coordenadas ?bandeira ?brasao
"""

_SIGLAS = {sigla: codigo for codigo, sigla in CODIGOS_UF.items()}


class UF(Registro):
    sigla: SiglaUF
    codigo_ibge: int
    nome: str
    regiao: Regiao
    wikidata: str
    artigo: str | None
    capital: str
    area_km2: float
    latitude: float
    longitude: float
    gentilicos: list[str]
    lema: str | None
    bandeira: str | None
    brasao: str | None


def arquivo_commons(url: str | None) -> str | None:
    """`http://commons.wikimedia.org/wiki/Special:FilePath/Bandeira%20do%20Acre.svg` → `Bandeira do Acre.svg`."""

    return unquote(url.rsplit("/", 1)[-1]) if url else None


class UFs(DatasetWikidata):
    nome = "ufs"
    descricao = "As 27 unidades federativas, com capital, área, coordenadas, gentílico, lema, bandeira e brasão."
    consulta = CONSULTA
    prefixo_infobox = "Info/Estado do Brasil"
    amostra = ["Acre", "São Paulo (estado)", "Distrito Federal (Brasil)"]
    modelo = UF
    chave = ("sigla",)

    def registro(self, linha: dict[str, str], info: dict[str, Wikicode], revid: int | None) -> UF:
        sigla = linha["iso"].removeprefix("BR-")
        codigo = _SIGLAS[sigla]
        longitude, latitude = map(float, re.findall(r"-?\d+(?:\.\d+)?", linha["coordenadas"]))
        nomes_gentilicos = [g for g in linha.get("gentilicos", "").split("|") if g]
        return UF(
            sigla=sigla,
            codigo_ibge=codigo,
            nome=linha["nome"],
            regiao=REGIOES[codigo // 10],
            wikidata=qid(linha["item"]),
            artigo=opcional(linha.get("artigo")),
            capital=linha["capital"],
            area_km2=float(linha["area"]),
            latitude=latitude,
            longitude=longitude,
            gentilicos=nomes_gentilicos or gentilicos(texto(info.get("gentílico"))),
            lema=opcional(linha.get("lema")) or texto(info.get("lema")),
            bandeira=arquivo_commons(linha.get("bandeira")),
            brasao=arquivo_commons(linha.get("brasao")),
        )

    def lacunas(self, linha: dict[str, str], info: dict[str, Wikicode], revid: int | None) -> list[str]:
        if linha.get("gentilicos"):
            return []
        ref = referencia_wikipedia(revid)
        return [f'{qid(linha["item"])}\tP1549\tpt:"{g}"{ref}' for g in gentilicos(texto(info.get("gentílico")))]

    def validar_conjunto(self, registros: list[UF]) -> None:
        if len(registros) != 27:
            raise ValueError(f"{len(registros)} UFs: esperado 27")
