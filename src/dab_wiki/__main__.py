"""Linha de comando: `dab-wiki atualizar|validar|schemas|fixture|listar`."""

import argparse
import gzip
import json
import sys
import traceback
from pathlib import Path

from dab_wiki.base import RAIZ
from dab_wiki.datasets import DATASETS
from dab_wiki.mediawiki import Pagina, buscar_pagina

FIXTURES = RAIZ / "tests" / "fixtures"


def fixture_path(titulo: str) -> Path:
    return FIXTURES / (titulo.replace("/", "_").replace(" ", "_") + ".json.gz")


def carregar_fixture(titulo: str) -> Pagina:
    with gzip.open(fixture_path(titulo), "rt", encoding="utf-8") as f:
        return Pagina(**json.load(f))


def atualizar(nomes: list[str], resumo: Path | None) -> int:
    erros = 0
    partes = []
    for nome in nomes:
        dataset = DATASETS[nome]
        try:
            antigo = dataset.carregar()
            arquivo = dataset.gerar()
            mudancas = dataset.comparar(antigo, arquivo)
            if dataset.salvar(arquivo):
                fontes = ", ".join(f"[{f.titulo}]({f.url})" for f in arquivo.fontes)
                partes.append(f"### `{nome}`\n\nFonte: {fontes}\n\n{mudancas or '- (apenas metadados)'}\n")
                print(f"{nome}: atualizado")
            else:
                print(f"{nome}: sem mudanças")
        except Exception as e:  # noqa: BLE001 — um dataset quebrado não impede os outros
            erros += 1
            print(f"{nome}: ERRO\n{traceback.format_exc()}", file=sys.stderr)
            partes.append(f"### `{nome}`: ERRO\n\n```\n{type(e).__name__}: {e}\n```\n")
    if resumo:
        resumo.write_text("\n".join(partes), encoding="utf-8")
    return 1 if erros else 0


def validar(nomes: list[str]) -> int:
    erros = 0
    for nome in nomes:
        try:
            DATASETS[nome].validar_arquivo()
            print(f"{nome}: ok")
        except Exception as e:  # noqa: BLE001
            erros += 1
            print(f"{nome}: INVÁLIDO\n{e}", file=sys.stderr)
    return 1 if erros else 0


def schemas(nomes: list[str]) -> int:
    for nome in nomes:
        print(DATASETS[nome].salvar_schema().relative_to(RAIZ))
    return 0


def fixture(nomes: list[str]) -> int:
    FIXTURES.mkdir(parents=True, exist_ok=True)
    for nome in nomes:
        for titulo in DATASETS[nome].paginas:
            pagina = buscar_pagina(titulo)
            with gzip.open(fixture_path(titulo), "wt", encoding="utf-8") as f:
                json.dump(pagina.__dict__, f, ensure_ascii=False)
            print(f"{fixture_path(titulo).relative_to(RAIZ)} (revid {pagina.revid})")
    return 0


def listar(nomes: list[str]) -> int:
    print(json.dumps(nomes))
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(prog="dab-wiki")
    sub = parser.add_subparsers(dest="comando", required=True)
    for comando, ajuda in [
        ("atualizar", "raspa a Wikipedia e grava data/*.json se os dados mudaram"),
        ("validar", "valida os arquivos em data/ contra os modelos"),
        ("schemas", "gera schemas/*.schema.json a partir dos modelos"),
        ("fixture", "salva o HTML atual das páginas em tests/fixtures/"),
        ("listar", "imprime os nomes dos datasets em JSON (para a matriz do GitHub Actions)"),
    ]:
        p = sub.add_parser(comando, help=ajuda)
        p.add_argument("datasets", nargs="*", choices=[[], *DATASETS], help="padrão: todos")
        if comando == "atualizar":
            p.add_argument("--resumo", type=Path, help="arquivo Markdown com o resumo das mudanças")
    args = parser.parse_args()
    nomes = args.datasets or list(DATASETS)
    if args.comando == "atualizar":
        sys.exit(atualizar(nomes, args.resumo))
    sys.exit({"validar": validar, "schemas": schemas, "fixture": fixture, "listar": listar}[args.comando](nomes))


if __name__ == "__main__":
    main()
