import re
from datetime import date

from pydantic import model_validator

from dab_wiki.base import UFS, Dataset, Registro, SiglaUF
from dab_wiki.mediawiki import Pagina
from dab_wiki.tabela import EstruturaAlterada, data, encontrar_tabela, limpar, vazio

_VICE = re.compile(r"([^()\n]+?)\s*\((\d{4})\s*[–-]\s*(presente|\d{4})\)")


class Governador(Registro):
    uf: SiglaUF
    nome: str
    artigo: str | None
    interino: bool
    partido: str | None
    partido_sigla: str | None
    mandato_inicio: date
    mandato_fim: date | None
    ano_eleicao: int | None
    vice_governador: str | None

    @model_validator(mode="after")
    def _mandato(self):
        if self.mandato_fim and self.mandato_fim <= self.mandato_inicio:
            raise ValueError("mandato_fim deve ser posterior a mandato_inicio")
        return self


def vice_atual(texto: str) -> str | None:
    """Vice em exercício: o marcado com `(AAAA–presente)` ou, sem períodos, o texto inteiro."""

    if vazio(texto):
        return None
    periodos = _VICE.findall(texto.replace("\n", " "))
    if not periodos:
        return limpar(texto).replace("\n", " ")
    atuais = [nome for nome, _, fim in periodos if fim == "presente"]
    return (atuais or [periodos[-1][0]])[-1].strip()


def partido(texto: str) -> tuple[str | None, str | None]:
    """`Movimento Democrático Brasileiro\\nMDB` → (nome, sigla)."""

    if vazio(texto) or "sem partido" in texto.lower():
        return None, None
    linhas = texto.split("\n")
    if len(linhas) == 1:
        return linhas[0], linhas[0]
    return " ".join(linhas[:-1]), linhas[-1]


class Governadores(Dataset):
    nome = "governadores"
    descricao = "Governadores em exercício nas 27 unidades federativas."
    paginas = ["Lista de governadores das unidades federativas do Brasil"]
    modelo = Governador
    chave = ("uf",)

    def extrair(self, paginas: dict[str, Pagina]) -> list[Governador]:
        df = encontrar_tabela(
            next(iter(paginas.values())),
            ["Unidade federativa", "Governador.1", "Partido.1", "No cargo desde", "Fim previsto", "Eleição", "Vice"],
        )
        registros = {}
        # Linhas repetidas da mesma UF (rowspan de interinos) → vale a última.
        for _, linha in df.iterrows():
            uf_nome = linha["Unidade federativa"].split("\n")[0].split("(")[0].strip()
            if uf_nome not in UFS:
                raise EstruturaAlterada(f"Unidade federativa desconhecida: {uf_nome!r}")
            nome_partido, sigla = partido(linha["Partido.1"])
            anos = re.findall(r"\d{4}", linha["Eleição"])
            fim = linha["Fim previsto"]
            nome = linha["Governador.1"]
            registros[uf_nome] = Governador(
                uf=UFS[uf_nome],
                nome=re.sub(r"\s*\(interin[oa]\)", "", nome),
                interino=bool(re.search(r"\(interin[oa]\)", nome)),
                artigo=linha["Governador.1"].link,
                partido=nome_partido,
                partido_sigla=sigla,
                mandato_inicio=data(linha["No cargo desde"]),
                mandato_fim=None if "definir" in fim.lower() or vazio(fim) else data(fim),
                ano_eleicao=int(anos[-1]) if anos else None,
                vice_governador=vice_atual(linha["Vice"]),
            )
        return list(registros.values())

    def validar_conjunto(self, registros: list[Governador]) -> None:
        faltando = set(UFS.values()) - {r.uf for r in registros}
        if faltando:
            raise ValueError(f"UFs sem governador: {sorted(faltando)}")
