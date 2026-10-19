"""Cliente mínimo da API da MediaWiki da Wikipedia em português."""

import time
from dataclasses import dataclass

import requests

API = "https://pt.wikipedia.org/w/api.php"
USER_AGENT = "DadosAbertosBrasil-bot/3.0 (https://github.com/GusFurtado/dab_assets)"
TENTATIVAS = 6


@dataclass(frozen=True)
class Pagina:
    """HTML renderizado de uma página da Wikipedia em uma revisão específica."""

    titulo: str
    revid: int
    html: str

    @property
    def url(self) -> str:
        return f"https://pt.wikipedia.org/w/index.php?oldid={self.revid}"


_session = requests.Session()
_session.headers["User-Agent"] = USER_AGENT


def _get(params: dict) -> dict:
    params = {"format": "json", "formatversion": 2, "maxlag": 5, **params}
    for tentativa in range(TENTATIVAS):
        r = _session.get(API, params=params, timeout=60)
        if r.status_code == 429 or r.status_code >= 500 or "retry-after" in r.headers:
            espera = int(r.headers.get("retry-after", 2**tentativa))
            time.sleep(min(espera, 60))
            continue
        r.raise_for_status()
        dados = r.json()
        if "error" in dados:
            raise RuntimeError(f"Erro da API da Wikipedia: {dados['error']}")
        return dados
    raise RuntimeError(f"A Wikipedia continuou recusando a requisição após {TENTATIVAS} tentativas: {params}")


def buscar_pagina(titulo: str) -> Pagina:
    """Busca o HTML renderizado da revisão atual de uma página."""

    dados = _get({"action": "parse", "page": titulo, "prop": "text|revid", "redirects": 1})["parse"]
    return Pagina(titulo=dados["title"], revid=dados["revid"], html=dados["text"])


def buscar_wikitextos(titulos: list[str], secao: int | None = 0) -> dict[str, dict]:
    """Wikitexto (por padrão só a seção inicial, onde fica a infobox) de vários artigos, 50 por requisição.

    Retorna `{titulo pedido: {"titulo", "revid", "texto"}}`; artigos inexistentes ficam de fora.
    """

    resultado = {}
    for inicio in range(0, len(titulos), 50):
        lote = titulos[inicio : inicio + 50]
        params = {
            "action": "query",
            "prop": "revisions",
            "rvprop": "content|ids",
            "rvslots": "main",
            "titles": "|".join(lote),
            "redirects": 1,
        }
        if secao is not None:
            params["rvsection"] = secao
        dados = _get(params)["query"]
        # Títulos pedidos podem ser normalizados ou redirecionados antes de chegar à página final.
        origem = {t: t for t in lote}
        for chave in ("normalized", "redirects"):
            for item in dados.get(chave, []):
                for pedido, atual in list(origem.items()):
                    if atual == item["from"]:
                        origem[pedido] = item["to"]
        por_titulo = {p["title"]: p for p in dados["pages"] if "revisions" in p}
        for pedido, final in origem.items():
            if final in por_titulo:
                revisao = por_titulo[final]["revisions"][0]
                resultado[pedido] = {
                    "titulo": final,
                    "revid": revisao["revid"],
                    "texto": revisao["slots"]["main"]["content"],
                }
    return resultado
