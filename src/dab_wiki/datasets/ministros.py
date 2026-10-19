import re
from datetime import date
from typing import Literal

from pydantic import model_validator

from dab_wiki.base import UFS, Dataset, Registro, SiglaUF
from dab_wiki.mediawiki import Pagina
from dab_wiki.tabela import data, encontrar_tabela, normalizar, vazio

TipoOrgao = Literal["Ministério", "Órgão com status de ministério", "Secretaria com status de ministério"]

_SEPARADORES: dict[str, TipoOrgao] = {
    "orgaos com status de ministerio": "Órgão com status de ministério",
    "secretarias com status de ministerio": "Secretaria com status de ministério",
}


class Ministro(Registro):
    governo: str
    orgao: str
    orgao_artigo: str | None
    tipo: TipoOrgao
    nome: str
    artigo: str | None
    interino: bool
    partido_sigla: str | None
    uf_origem: SiglaUF | None
    posse: date
    saida: date | None
    em_exercicio: bool

    @model_validator(mode="after")
    def _periodo(self):
        if self.saida and self.saida < self.posse:
            raise ValueError("saida anterior à posse")
        return self


def partido_uf(texto: str) -> tuple[str | None, str | None]:
    """`PSD/MT` → ("PSD", "MT"); `MDB/MS→PSB/SP` → filiação final; `Sem partido/PE` → (None, "PE")."""

    if vazio(texto):
        return None, None
    atual = texto.split("→")[-1].strip()
    sigla, _, uf = atual.rpartition("/") if "/" in atual else (atual, "", "")
    sigla = None if not sigla or "sem partido" in sigla.lower() else sigla.strip()
    uf = uf.strip() if uf.strip() in UFS.values() else None
    return sigla, uf


class Ministros(Dataset):
    nome = "ministros"
    descricao = "Ministros e titulares de órgãos com status de ministério do governo federal atual, com histórico."
    paginas = ["Lista de membros do gabinete de Lula (2023–presente)"]
    modelo = Ministro
    chave = ("orgao", "posse", "nome")

    def extrair(self, paginas: dict[str, Pagina]) -> list[Ministro]:
        pagina = next(iter(paginas.values()))
        governo = re.sub(r"^Lista de membros do gabinete de ", "", pagina.titulo)
        df = encontrar_tabela(pagina, ["Ministério", "Incumbente.1", "Partido", "Posse no dia", "Saída no dia"])
        tipo: TipoOrgao = "Ministério"
        registros = []
        for _, linha in df.iterrows():
            # Linhas que ocupam a tabela inteira separam ministérios de órgãos e secretarias.
            if normalizar(linha["Ministério"]) in _SEPARADORES:
                tipo = _SEPARADORES[normalizar(linha["Ministério"])]
                continue
            nome = linha["Incumbente.1"].replace("\n", " ")
            saida = linha["Saída no dia"]
            sigla, uf = partido_uf(linha["Partido"])
            registros.append(
                Ministro(
                    governo=governo,
                    orgao=linha["Ministério"].replace("\n", " "),
                    orgao_artigo=linha["Ministério"].link,
                    tipo=tipo,
                    nome=re.sub(r"\s*\(interin[oa]\)", "", nome),
                    artigo=linha["Incumbente.1"].link,
                    interino=bool(re.search(r"\(interin[oa]\)", nome)),
                    partido_sigla=sigla,
                    uf_origem=uf,
                    posse=data(linha["Posse no dia"]),
                    saida=None if "exercicio" in normalizar(saida) else data(saida),
                    em_exercicio="exercicio" in normalizar(saida),
                )
            )
        return registros

    def validar_conjunto(self, registros: list[Ministro]) -> None:
        if sum(r.em_exercicio for r in registros) < 20:
            raise ValueError("Menos de 20 ministros em exercício: a tabela provavelmente mudou")
