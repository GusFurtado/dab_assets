import re
from datetime import date

from dab_wiki.base import Dataset, Registro
from dab_wiki.mediawiki import Pagina
from dab_wiki.tabela import datas, encontrar_tabela, encontrar_tabelas, limpar, vazio

ATIVOS = "Lista de partidos políticos do Brasil"
EXTINTOS = "Lista de partidos políticos brasileiros extintos"


class Partido(Registro):
    nome: str
    artigo: str | None
    sigla: str | None
    numero: int | None
    ativo: bool
    periodo: str
    ano_fundacao: int | None
    data_criacao: date | None
    data_registro: date | None
    ano_extincao: int | None
    situacao: str | None


def _ano(texto: str) -> int | None:
    m = re.search(r"\b1[89]\d{2}\b|\b20\d{2}\b", texto)
    return int(m.group()) if m else None


def _numero(texto: str) -> int | None:
    return int(texto) if texto.strip().isdigit() else None


def _opcional(texto: str) -> str | None:
    return None if vazio(texto) else limpar(texto).replace("\n", " ")


def ativos(pagina: Pagina) -> list[Partido]:
    df = encontrar_tabela(pagina, ["Nome.1", "Sigla", "Número eleitoral", "Data de criação", "Data de registro"])
    registros = []
    for _, linha in df.iterrows():
        criacao = datas(linha["Data de criação"])
        registro = datas(linha["Data de registro"])
        registros.append(
            Partido(
                nome=linha["Nome.1"],
                artigo=linha["Nome.1"].link,
                sigla=_opcional(linha["Sigla"]),
                numero=_numero(linha["Número eleitoral"]),
                ativo=True,
                periodo="Em atividade",
                ano_fundacao=criacao[0].year if criacao else None,
                data_criacao=criacao[0] if criacao else None,
                data_registro=registro[0] if registro else None,
                ano_extincao=None,
                situacao=None,
            )
        )
    return registros


def extintos(pagina: Pagina) -> list[Partido]:
    registros = []
    tabelas = encontrar_tabelas(
        pagina, ["Nome.1", "Fundação", "Dissolução"], opcionais=["Sigla", "Número eleitoral", "Situação", "Notas"]
    )
    for df in tabelas:
        for _, linha in df.iterrows():
            registros.append(
                Partido(
                    nome=linha["Nome.1"],
                    artigo=linha["Nome.1"].link,
                    sigla=_opcional(linha["Sigla"]),
                    numero=_numero(linha["Número eleitoral"]),
                    ativo=False,
                    periodo=df.attrs["secao"],
                    ano_fundacao=_ano(linha["Fundação"]),
                    data_criacao=None,
                    data_registro=None,
                    ano_extincao=_ano(linha["Dissolução"]),
                    situacao=_opcional(linha["Situação"]) or _opcional(linha["Notas"]),
                )
            )
    return registros


class Partidos(Dataset):
    nome = "partidos"
    descricao = "Partidos políticos brasileiros em atividade e extintos, desde o Império."
    paginas = [ATIVOS, EXTINTOS]
    modelo = Partido
    chave = ("periodo", "nome", "ano_fundacao")

    def extrair(self, paginas: dict[str, Pagina]) -> list[Partido]:
        return ativos(paginas[ATIVOS]) + extintos(paginas[EXTINTOS])

    def validar_conjunto(self, registros: list[Partido]) -> None:
        n_ativos = sum(r.ativo for r in registros)
        if not 20 <= n_ativos <= 45:
            raise ValueError(f"{n_ativos} partidos ativos: a tabela provavelmente mudou")
        numeros = [r.numero for r in registros if r.ativo and r.numero]
        if len(numeros) != len(set(numeros)):
            raise ValueError("Número eleitoral repetido entre partidos ativos")
