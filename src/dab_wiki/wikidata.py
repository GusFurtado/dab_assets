"""Consultas SPARQL ao Wikidata."""

import time

import requests

from dab_wiki.mediawiki import USER_AGENT

ENDPOINT = "https://query.wikidata.org/sparql"
TENTATIVAS = 5


def consultar(sparql: str) -> list[dict[str, str]]:
    """Executa a consulta e devolve as linhas como `{variável: valor}` (variáveis sem valor ficam de fora)."""

    for tentativa in range(TENTATIVAS):
        r = requests.post(
            ENDPOINT,
            data={"query": sparql},
            headers={"User-Agent": USER_AGENT, "Accept": "application/sparql-results+json"},
            timeout=120,
        )
        if r.status_code == 429 or r.status_code >= 500:
            time.sleep(min(int(r.headers.get("retry-after", 2**tentativa * 5)), 60))
            continue
        r.raise_for_status()
        return [{k: v["value"] for k, v in linha.items()} for linha in r.json()["results"]["bindings"]]
    raise RuntimeError(f"O Wikidata continuou recusando a consulta após {TENTATIVAS} tentativas")


def qid(uri: str) -> str:
    """`http://www.wikidata.org/entity/Q175` → `Q175`."""

    return uri.rsplit("/", 1)[-1]
