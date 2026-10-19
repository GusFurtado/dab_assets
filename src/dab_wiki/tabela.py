"""Extração e limpeza de tabelas do HTML da Wikipedia."""

import io
import re
import unicodedata
from datetime import date
from urllib.parse import unquote

import pandas as pd
from bs4 import BeautifulSoup

from dab_wiki.mediawiki import Pagina

# Elementos que só poluem o texto das células: notas de rodapé, chaves de ordenação e conteúdo oculto.
_RUIDO = [
    "sup.reference",
    "sup.noprint",
    "span.sortkey",
    "span.mw-ref",
    "style",
    "link",
    "[style*='display:none']",
    "[style*='display: none']",
    "figure",
    "span[typeof~='mw:File']",
    "a.mw-file-description",
    "a.image",
]

# Quebras de linha viram este marcador antes do `read_html`, que colapsa espaços, e voltam a ser `\n` depois.
_QUEBRA = "\u2424"
_NAMESPACES = ("Ficheiro:", "Arquivo:", "Imagem:", "Predefinição:", "Categoria:", "Especial:", "Ajuda:", "Wikipédia:")

MESES = {
    "janeiro": 1,
    "fevereiro": 2,
    "março": 3,
    "abril": 4,
    "maio": 5,
    "junho": 6,
    "julho": 7,
    "agosto": 8,
    "setembro": 9,
    "outubro": 10,
    "novembro": 11,
    "dezembro": 12,
}

_DATA = re.compile(r"(\d{1,2})\s*(?:\.?\s*[º°o])?\s+de\s+(" + "|".join(MESES) + r")\s+de\s+(\d{4})", re.IGNORECASE)
_NOTA = re.compile(r"\[\s*(?:nota|n|nt)?\s*[\w\s]{0,6}\]", re.IGNORECASE)
TRACOS = {"", "—", "–", "-", "nenhum", "nenhuma", "vago", "vaga"}


class EstruturaAlterada(Exception):
    """A página não tem mais o formato que o scraper espera."""


class Celula(str):
    """Texto de uma célula, com o link do primeiro artigo citado nela (se houver)."""

    link: str | None

    def __new__(cls, texto: str, link: str | None = None):
        obj = super().__new__(cls, texto)
        obj.link = link
        return obj


def normalizar(texto: str) -> str:
    """Minúsculas, sem acentos e com espaços simples: para comparar cabeçalhos."""

    texto = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode()
    return re.sub(r"\s+", " ", texto).strip().lower()


def limpar(texto) -> str:
    """Remove notas remanescentes (`[nota 2]`, `[1]`) e espaços extras."""

    if texto is None or (isinstance(texto, float) and pd.isna(texto)):
        return ""
    texto = _NOTA.sub("", str(texto)).replace(_QUEBRA, "\n")
    linhas = [re.sub(r"[ \t\xa0]+", " ", linha).strip() for linha in texto.split("\n")]
    return "\n".join(linha for linha in linhas if linha)


def vazio(texto: str) -> bool:
    return normalizar(limpar(texto)) in TRACOS


def artigo(href: str | None) -> str | None:
    """Converte o href de um link interno no título do artigo, ou None para links externos/vermelhos."""

    if not href or "redlink=1" in href or not href.startswith("/wiki/"):
        return None
    titulo = unquote(href.removeprefix("/wiki/").split("#")[0]).replace("_", " ")
    return None if titulo.startswith(_NAMESPACES) else titulo


def data(texto: str) -> date:
    """Converte uma data por extenso (`1.º de janeiro de 2023`) para `date`."""

    m = _DATA.search(limpar(texto).replace("\n", " "))
    if not m:
        raise ValueError(f"Data não reconhecida: {texto!r}")
    dia, mes, ano = m.groups()
    return date(int(ano), MESES[mes.lower()], int(dia))


def datas(texto: str) -> list[date]:
    """Todas as datas por extenso encontradas no texto, em ordem."""

    return [date(int(a), MESES[m.lower()], int(d)) for d, m, a in _DATA.findall(limpar(texto).replace("\n", " "))]


def _wikitables(html: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    for seletor in _RUIDO:
        for el in soup.select(seletor):
            el.decompose()
    for br in soup.find_all("br"):
        br.replace_with(_QUEBRA)
    for bloco in soup.select("li, p, div"):
        bloco.append(_QUEBRA)
    return [str(t) for t in soup.select("table.wikitable")]


def _nome_coluna(col) -> str:
    if isinstance(col, tuple):
        partes = []
        for parte in col:
            parte = limpar(parte)
            if parte and not parte.startswith("Unnamed") and parte not in partes:
                partes.append(parte)
        col = " / ".join(partes)
    return " ".join(limpar(col).split())


def tabelas(pagina: Pagina) -> list[pd.DataFrame]:
    """Todas as tabelas `wikitable` da página, com células do tipo `Celula`."""

    resultado = []
    for html in _wikitables(pagina.html):
        try:
            df = pd.read_html(io.StringIO(html), extract_links="body", flavor="lxml")[0]
        except ValueError:
            continue
        df.columns = [_nome_coluna(c) for c in df.columns]
        df = df.map(lambda c: Celula(limpar(c[0]), artigo(c[1])) if isinstance(c, tuple) else Celula(limpar(c)))
        resultado.append(df)
    return resultado


def encontrar_tabela(pagina: Pagina, colunas: list[str]) -> pd.DataFrame:
    """Primeira tabela que contém todas as colunas pedidas (comparação por prefixo, sem acentos).

    As colunas da tabela retornada são renomeadas para os nomes pedidos.
    """

    for df in tabelas(pagina):
        nomes = {normalizar(c): c for c in df.columns}
        encontradas = {}
        for pedida in colunas:
            alvo = normalizar(pedida)
            achada = next((orig for norm, orig in nomes.items() if norm == alvo), None)
            achada = achada or next((orig for norm, orig in nomes.items() if norm.startswith(alvo)), None)
            if achada is None:
                break
            encontradas[achada] = pedida
        else:
            return df[list(encontradas)].rename(columns=encontradas).reset_index(drop=True)
    raise EstruturaAlterada(f"Nenhuma tabela em '{pagina.titulo}' tem as colunas {colunas}")
