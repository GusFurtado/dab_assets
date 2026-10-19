import re
from datetime import date

from dab_wiki.base import UFS, Dataset, Registro, SiglaUF
from dab_wiki.datasets.governadores import partido
from dab_wiki.mediawiki import Pagina
from dab_wiki.tabela import EstruturaAlterada, datas, encontrar_tabela, limpar, vazio


class PrefeitoCapital(Registro):
    uf: SiglaUF
    capital: str
    capital_artigo: str | None
    nome: str
    artigo: str | None
    partido: str | None
    partido_sigla: str | None
    vice_prefeito: str | None
    mandato_inicio: date | None
    mandato_fim: date | None


class PrefeitosCapitais(Dataset):
    nome = "prefeitos_capitais"
    descricao = "Prefeitos em exercício nas 26 capitais estaduais."
    paginas = ["Lista de prefeitos das capitais do Brasil"]
    modelo = PrefeitoCapital
    chave = ("uf",)

    def extrair(self, paginas: dict[str, Pagina]) -> list[PrefeitoCapital]:
        df = encontrar_tabela(
            next(iter(paginas.values())),
            ["Capital", "Prefeito (a).1", "Partido.1", "Vice-prefeito", "No cargo"],
        )
        siglas = set(UFS.values())
        registros = {}
        for _, linha in df.iterrows():
            m = re.match(r"(.+?),\s*([A-Z]{2})\b", linha["Capital"].split("\n")[0])
            if not m or m.group(2) not in siglas:
                raise EstruturaAlterada(f"Capital não reconhecida: {linha['Capital']!r}")
            capital, uf = m.groups()
            nome_partido, sigla = partido(linha["Partido.1"])
            # Início e fim vêm grudados ("1.º de janeiro de 20251.º de janeiro de 2029"); fim pode ser "indefinido".
            periodo = datas(linha["No cargo"])
            fim_definido = len(periodo) > 1
            registros[uf] = PrefeitoCapital(
                uf=uf,
                capital=capital,
                capital_artigo=linha["Capital"].link,
                nome=linha["Prefeito (a).1"],
                artigo=linha["Prefeito (a).1"].link,
                partido=nome_partido,
                partido_sigla=sigla,
                vice_prefeito=None if vazio(linha["Vice-prefeito"]) else limpar(linha["Vice-prefeito"]),
                mandato_inicio=periodo[0] if periodo else None,
                mandato_fim=periodo[-1] if fim_definido else None,
            )
        return list(registros.values())

    def validar_conjunto(self, registros: list[PrefeitoCapital]) -> None:
        faltando = set(UFS.values()) - {"DF"} - {r.uf for r in registros}
        if faltando:
            raise ValueError(f"Capitais sem prefeito: {sorted(faltando)}")
