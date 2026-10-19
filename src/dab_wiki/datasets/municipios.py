import re
from datetime import date

from mwparserfromhell.wikicode import Wikicode
from pydantic import Field

from dab_wiki.base import CODIGOS_UF, REGIOES, Regiao, Registro, SiglaUF
from dab_wiki.datasets._wikidata import DatasetWikidata, opcional, referencia_wikipedia
from dab_wiki.infobox import dtlink, texto
from dab_wiki.tabela import MESES
from dab_wiki.wikidata import qid

CONSULTA = """
SELECT ?item ?ibge ?nome ?artigo
  (GROUP_CONCAT(DISTINCT ?gentilico; separator="|") AS ?gentilicos)
  (SAMPLE(?fundacao_) AS ?fundacao) (SAMPLE(?precisao_) AS ?precisao) (SAMPLE(?lema_) AS ?lema)
WHERE {
  ?item wdt:P31 wd:Q3184121; wdt:P1585 ?ibge .
  OPTIONAL { ?item rdfs:label ?nome FILTER(LANG(?nome) = "pt") }
  OPTIONAL { ?item wdt:P1549 ?gentilico FILTER(LANG(?gentilico) = "pt") }
  OPTIONAL { ?item p:P571/psv:P571 [wikibase:timeValue ?fundacao_; wikibase:timePrecision ?precisao_] }
  OPTIONAL { ?item wdt:P1451 ?lema_ }
  OPTIONAL { ?a schema:about ?item; schema:isPartOf <https://pt.wikipedia.org/>; schema:name ?artigo }
}
GROUP BY ?item ?ibge ?nome ?artigo
"""


class Municipio(Registro):
    codigo_ibge: int = Field(ge=1_100_000, le=5_399_999)
    nome: str
    uf: SiglaUF
    regiao: Regiao
    wikidata: str
    artigo: str | None
    gentilicos: list[str]
    fundacao: date | None
    ano_fundacao: int | None
    aniversario_dia: int | None = Field(ge=1, le=31)
    aniversario_mes: int | None = Field(ge=1, le=12)
    lema: str | None


def gentilicos(texto_: str | None) -> list[str]:
    """`acriano ou acreano` → ["acriano", "acreano"]."""

    if not texto_:
        return []
    resultado = []
    for parte in re.split(r"(?:^|\s+)ou\s+|\s*[,;/]\s*|\s+e\s+", texto_):
        # Observações entre parênteses ("(mais usado)") saem; se a parte toda estiver entre parênteses, fica o conteúdo.
        limpa = re.sub(r"\([^)]*\)", "", parte).strip() or parte.strip(" ()")
        if len(limpa) > 2 and limpa.lower() not in resultado:
            resultado.append(limpa.lower())
    return resultado


def fundacao_wikidata(linha: dict[str, str]) -> tuple[date | None, int | None]:
    """Data de fundação no Wikidata, respeitando a precisão (11 = dia, 9 = ano)."""

    if "fundacao" not in linha:
        return None, None
    m = re.match(r"([+-]?\d+)-(\d{2})-(\d{2})", linha["fundacao"])
    if not m or m.group(1).startswith("-"):
        return None, None
    ano = int(m.group(1))
    precisao = int(linha.get("precisao", 0))
    if precisao >= 11:
        return date(ano, int(m.group(2)), int(m.group(3))), ano
    return None, ano if precisao >= 9 else None


def aniversario(texto_: str | None) -> tuple[int | None, int | None]:
    m = re.match(r"(\d{1,2})\s*(?:\.?\s*[º°o])?\s+de\s+(\w+)", texto_ or "", re.IGNORECASE)
    if not m or m.group(2).lower() not in MESES:
        return None, None
    return int(m.group(1)), MESES[m.group(2).lower()]


class Municipios(DatasetWikidata):
    nome = "municipios"
    descricao = "Os 5.570 municípios brasileiros com gentílico, fundação, aniversário e lema."
    consulta = CONSULTA
    prefixo_infobox = "Info/Município do Brasil"
    amostra = ["Santos", "Campinas", "Ouro Preto", "Acrelândia", "Bom Jesus (Piauí)"]
    modelo = Municipio
    chave = ("codigo_ibge",)

    def registro(self, linha: dict[str, str], info: dict[str, Wikicode], revid: int | None) -> Municipio:
        codigo = int(linha["ibge"])
        data, ano = fundacao_wikidata(linha)
        if data is None:
            # O Wikidata muitas vezes só tem o ano; a infobox pode ter a data completa.
            data_info, ano_info = dtlink(info.get("fundação"))
            if ano is None or (data_info and data_info.year == ano):
                data, ano = data_info, ano_info
        dia, mes = aniversario(texto(info.get("aniversário")))
        nomes_gentilicos = [g for g in linha.get("gentilicos", "").split("|") if g]
        return Municipio(
            codigo_ibge=codigo,
            nome=linha.get("nome") or linha["artigo"].split(" (")[0],
            uf=CODIGOS_UF[codigo // 100_000],
            regiao=REGIOES[codigo // 1_000_000],
            wikidata=qid(linha["item"]),
            artigo=opcional(linha.get("artigo")),
            gentilicos=nomes_gentilicos or gentilicos(texto(info.get("gentílico"))),
            fundacao=data,
            ano_fundacao=ano,
            aniversario_dia=dia,
            aniversario_mes=mes,
            lema=opcional(linha.get("lema")) or texto(info.get("lema")),
        )

    def lacunas(self, linha: dict[str, str], info: dict[str, Wikicode], revid: int | None) -> list[str]:
        item = qid(linha["item"])
        ref = referencia_wikipedia(revid)
        comandos = []
        if not linha.get("gentilicos"):
            for g in gentilicos(texto(info.get("gentílico"))):
                comandos.append(f'{item}\tP1549\tpt:"{g}"{ref}')
        if "fundacao" not in linha:
            data, ano = dtlink(info.get("fundação"))
            if data:
                comandos.append(f"{item}\tP571\t+{data.isoformat()}T00:00:00Z/11{ref}")
            elif ano:
                comandos.append(f"{item}\tP571\t+{ano}-00-00T00:00:00Z/9{ref}")
        return comandos

    def validar_conjunto(self, registros: list[Municipio]) -> None:
        if not 5560 <= len(registros) <= 5580:
            raise ValueError(f"{len(registros)} municípios: esperado ~5.570")
