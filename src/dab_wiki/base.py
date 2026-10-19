"""Estrutura comum dos datasets: modelos, arquivo publicado e comparação de versões."""

import json
from collections import Counter
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field, create_model

from dab_wiki.mediawiki import Pagina, buscar_pagina

SCHEMA_VERSION = 1
LIMITE_RESUMO = 300  # linhas no corpo do PR, que tem limite de tamanho
RAIZ = Path(__file__).resolve().parents[2]
DATA = RAIZ / "data"
SCHEMAS = RAIZ / "schemas"

SiglaUF = Literal[
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA",
    "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO",
]  # fmt: skip

UFS: dict[str, SiglaUF] = {
    "Acre": "AC",
    "Alagoas": "AL",
    "Amapá": "AP",
    "Amazonas": "AM",
    "Bahia": "BA",
    "Ceará": "CE",
    "Distrito Federal": "DF",
    "Espírito Santo": "ES",
    "Goiás": "GO",
    "Maranhão": "MA",
    "Mato Grosso": "MT",
    "Mato Grosso do Sul": "MS",
    "Minas Gerais": "MG",
    "Pará": "PA",
    "Paraíba": "PB",
    "Paraná": "PR",
    "Pernambuco": "PE",
    "Piauí": "PI",
    "Rio de Janeiro": "RJ",
    "Rio Grande do Norte": "RN",
    "Rio Grande do Sul": "RS",
    "Rondônia": "RO",
    "Roraima": "RR",
    "Santa Catarina": "SC",
    "São Paulo": "SP",
    "Sergipe": "SE",
    "Tocantins": "TO",
}


# Códigos IBGE das UFs; o primeiro dígito é a região.
CODIGOS_UF: dict[int, SiglaUF] = {
    11: "RO", 12: "AC", 13: "AM", 14: "RR", 15: "PA", 16: "AP", 17: "TO",
    21: "MA", 22: "PI", 23: "CE", 24: "RN", 25: "PB", 26: "PE", 27: "AL", 28: "SE", 29: "BA",
    31: "MG", 32: "ES", 33: "RJ", 35: "SP",
    41: "PR", 42: "SC", 43: "RS",
    50: "MS", 51: "MT", 52: "GO", 53: "DF",
}  # fmt: skip

Regiao = Literal["Norte", "Nordeste", "Sudeste", "Sul", "Centro-Oeste"]
REGIOES: dict[int, Regiao] = {1: "Norte", 2: "Nordeste", 3: "Sudeste", 4: "Sul", 5: "Centro-Oeste"}


class Registro(BaseModel):
    """Base dos registros publicados: campos estritos e strings sem espaços nas pontas."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True, str_min_length=1, frozen=True)


class Fonte(BaseModel):
    model_config = ConfigDict(extra="forbid")

    titulo: str
    url: str
    revid: int | None = None
    consulta: str | None = None


class Arquivo(BaseModel):
    """Envelope de todo arquivo em `data/`."""

    model_config = ConfigDict(extra="forbid")

    schema_version: int = SCHEMA_VERSION
    dataset: str
    descricao: str
    licenca: str = "CC BY-SA 4.0 (conteúdo derivado da Wikipedia)"
    gerado_em: datetime
    fontes: list[Fonte]
    dados: list[Any]


class Dataset:
    """Um conjunto de dados extraído da Wikipedia (e, quando preciso, do Wikidata).

    Subclasses definem `nome`, `descricao`, `modelo`, `chave` e, no caso mais comum de tabelas em
    páginas da Wikipedia, `paginas` e `extrair`. Fontes diferentes sobrescrevem `buscar`, `processar`
    e `fontes`, trabalhando com "brutos": um dict serializável em JSON com tudo que foi baixado, que
    também é o que fica salvo em `tests/fixtures/`.
    """

    nome: ClassVar[str]
    descricao: ClassVar[str]
    paginas: ClassVar[list[str]] = []
    modelo: ClassVar[type[Registro]]
    chave: ClassVar[tuple[str, ...]]

    def buscar(self) -> dict:
        return {"paginas": {titulo: asdict(buscar_pagina(titulo)) for titulo in self.paginas}}

    def processar(self, brutos: dict) -> list[Registro]:
        return self.extrair({titulo: Pagina(**p) for titulo, p in brutos["paginas"].items()})

    def fontes(self, brutos: dict) -> list[Fonte]:
        paginas = [Pagina(**p) for p in brutos["paginas"].values()]
        return [Fonte(titulo=p.titulo, revid=p.revid, url=p.url) for p in paginas]

    def reduzir(self, brutos: dict) -> dict:
        """Recorte dos brutos salvo como fixture de teste (padrão: tudo)."""

        return brutos

    def extrair(self, paginas: dict[str, Pagina]) -> list[Registro]:
        raise NotImplementedError

    def validar_conjunto(self, registros: list[Registro]) -> None:
        """Checagens sobre o conjunto inteiro (ex.: 27 UFs). Levanta `ValueError`."""

    # ---------------------------------------------------------------- pipeline

    @property
    def caminho(self) -> Path:
        return DATA / f"{self.nome}.json"

    def modelo_arquivo(self) -> type[Arquivo]:
        return create_model(
            f"Arquivo{self.modelo.__name__}",
            __base__=Arquivo,
            dados=(list[self.modelo], Field(min_length=1)),
        )

    def gerar(self, brutos: dict | None = None, validar_conjunto: bool = True) -> Arquivo:
        brutos = brutos or self.buscar()
        registros = self.processar(brutos)
        chaves = Counter(self._chave(r.model_dump(mode="json")) for r in registros)
        duplicadas = sorted(c for c, n in chaves.items() if n > 1)
        if duplicadas:
            raise ValueError(f"Chaves duplicadas em {self.nome}: {duplicadas}")
        if validar_conjunto:
            self.validar_conjunto(registros)
        registros = sorted(registros, key=lambda r: self._chave(r.model_dump(mode="json")))
        return self.modelo_arquivo()(
            dataset=self.nome,
            descricao=self.descricao,
            gerado_em=datetime.now(UTC).replace(microsecond=0),
            fontes=self.fontes(brutos),
            dados=registros,
        )

    def carregar(self) -> dict | None:
        if not self.caminho.exists():
            return None
        conteudo = json.loads(self.caminho.read_text(encoding="utf-8"))
        # Arquivos no formato anterior ao envelope são tratados como inexistentes.
        return conteudo if isinstance(conteudo, dict) and "dados" in conteudo else None

    def validar_arquivo(self) -> None:
        """Valida o arquivo publicado contra o modelo atual."""

        dados = self.carregar()
        if dados is None:
            raise FileNotFoundError(self.caminho)
        arquivo = self.modelo_arquivo().model_validate(dados)
        self.validar_conjunto(arquivo.dados)

    def salvar(self, arquivo: Arquivo) -> bool:
        """Grava o arquivo se os dados mudaram. Retorna se houve gravação."""

        novo = arquivo.model_dump(mode="json")
        antigo = self.carregar()
        if antigo is not None and antigo.get("dados") == novo["dados"] and antigo.get("descricao") == novo["descricao"]:
            return False
        self.caminho.parent.mkdir(parents=True, exist_ok=True)
        self.caminho.write_text(json.dumps(novo, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return True

    def salvar_schema(self) -> Path:
        SCHEMAS.mkdir(parents=True, exist_ok=True)
        caminho = SCHEMAS / f"{self.nome}.schema.json"
        schema = self.modelo_arquivo().model_json_schema()
        caminho.write_text(json.dumps(schema, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return caminho

    # ---------------------------------------------------------------- diff

    def _chave(self, registro: dict) -> str:
        return " / ".join(str(registro[c]) for c in self.chave)

    def comparar(self, antigo: dict | None, novo: Arquivo) -> str:
        """Resumo em Markdown das mudanças entre o arquivo publicado e o novo."""

        novos = {self._chave(r): r for r in novo.model_dump(mode="json")["dados"]}
        antigos = {self._chave(r): r for r in (antigo or {}).get("dados", [])}
        linhas = []
        for chave in sorted(novos.keys() - antigos.keys()):
            linhas.append(f"- **{chave}**: novo registro")
        for chave in sorted(antigos.keys() - novos.keys()):
            linhas.append(f"- **{chave}**: removido")
        for chave in sorted(novos.keys() & antigos.keys()):
            a, n = antigos[chave], novos[chave]
            for campo in n:
                if a.get(campo) != n[campo]:
                    linhas.append(f"- **{chave}** · `{campo}`: {a.get(campo)!r} → {n[campo]!r}")
        alterados = len({linha.split("**")[1] for linha in linhas})
        if len(linhas) > LIMITE_RESUMO:
            linhas = [*linhas[:LIMITE_RESUMO], f"- … e mais {len(linhas) - LIMITE_RESUMO} mudanças (veja o diff)"]
        if antigos and alterados > max(3, len(antigos) // 2):
            aviso = f"> ⚠️ {alterados} de {len(antigos)} registros mudaram: confira vandalismo ou erro de parsing."
            linhas.insert(0, aviso + "\n")
        return "\n".join(linhas)
