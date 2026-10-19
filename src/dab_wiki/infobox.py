"""Leitura de parâmetros de infoboxes (predefinições `Info/...`) no wikitexto."""

import contextlib
import re
from datetime import date

import mwparserfromhell
from mwparserfromhell.wikicode import Wikicode


def _predefinicao(wikitexto: str, prefixo: str):
    # Recursivo: algumas infoboxes ficam dentro de outras marcações (ex.: Santa Catarina, Tocantins).
    for predefinicao in mwparserfromhell.parse(wikitexto).filter_templates(recursive=True):
        if predefinicao.name.strip().lower().startswith(prefixo.lower()):
            return predefinicao
    return None


def infobox(wikitexto: str, prefixo: str = "Info/") -> dict[str, Wikicode]:
    """Parâmetros da primeira predefinição cujo nome começa com `prefixo`, com nomes normalizados."""

    # A leitura por linhas completa parâmetros que o parser perde quando a marcação está quebrada
    # (ex.: Tocantins); quando o parser lê o parâmetro, ele prevalece por tratar valores multilinha.
    parametros = _infobox_por_linhas(wikitexto, prefixo)
    predefinicao = _predefinicao(wikitexto, prefixo)
    if predefinicao is not None:
        parametros |= {str(p.name).strip().lower(): p.value for p in predefinicao.params}
    return parametros


def _infobox_por_linhas(wikitexto: str, prefixo: str) -> dict[str, Wikicode]:
    """Leitura tolerante para infoboxes com marcação desbalanceada, que o parser não reconhece.

    Considera um parâmetro por linha (`| nome = valor`) até a linha que fecha a predefinição. Valores que
    ocupam mais de uma linha ficam só com a primeira.
    """

    inicio = re.search(r"\{\{\s*" + re.escape(prefixo), wikitexto, re.IGNORECASE)
    if not inicio:
        return {}
    parametros = {}
    profundidade = 1  # chaves abertas: a predefinição termina quando volta a zero
    for linha in wikitexto[inicio.end() :].split("\n")[1:]:
        if profundidade == 1 and linha.strip().startswith("}}"):
            break
        profundidade += linha.count("{{") - linha.count("}}")
        if profundidade < 1:
            break
        m = re.match(r"\s*\|\s*([^=|]+?)\s*=(.*)", linha)
        if m:
            parametros[m.group(1).strip().lower()] = mwparserfromhell.parse(m.group(2))
    return parametros


def infobox_externa(wikitexto: str, prefixo: str = "Info/") -> str | None:
    """Nome da predefinição dedicada quando a infobox vem sem parâmetros (`{{Info/Estado do Brasil/São Paulo}}`)."""

    predefinicao = _predefinicao(wikitexto, prefixo)
    if predefinicao is not None and not predefinicao.params:
        return "Predefinição:" + predefinicao.name.strip()
    return None


def texto(valor: Wikicode | None) -> str | None:
    """Texto plano da primeira linha do valor: sem referências, links, formatação e traduções após `<br>`."""

    if valor is None:
        return None
    valor = mwparserfromhell.parse(str(valor))
    for tag in valor.filter_tags(matches=lambda n: str(n.tag).lower() in ("ref", "small", "sup")):
        with contextlib.suppress(ValueError):  # já removida junto com uma tag externa
            valor.remove(tag)
    # Primeira linha não vazia: o que vem depois de <br> costuma ser tradução ou observação.
    linhas = [linha for linha in re.split(r"<br\s*/?>|\n", str(valor), flags=re.IGNORECASE) if linha.strip()]
    plano = mwparserfromhell.parse(linhas[0] if linhas else "").strip_code(normalize=True, collapse=True)
    plano = re.sub(r"<[^>]*>", "", plano.replace("''", ""))  # tags desbalanceadas que o parser não remove
    plano = re.sub(r"\s+", " ", plano).strip(" \"'“”;,.")
    return plano or None


def dtlink(valor: Wikicode | None) -> tuple[date | None, int | None]:
    """Data de `{{dtlink|dia|mês|ano}}` (ou `{{dtlink|||ano}}`): (data completa ou None, ano ou None)."""

    if valor is None:
        return None, None
    for predefinicao in mwparserfromhell.parse(str(valor)).filter_templates():
        if predefinicao.name.strip().lower() in ("dtlink", "dni", "data", "dtlink2"):
            partes = [str(p.value).strip() for p in predefinicao.params if not p.showkey][:3]
            partes += [""] * (3 - len(partes))
            dia, mes, ano = partes
            ano = int(ano) if ano.isdigit() else None
            if ano and dia.isdigit() and mes.isdigit():
                try:
                    return date(ano, int(mes), int(dia)), ano
                except ValueError:
                    return None, ano
            return None, ano
    m = re.search(r"\b(1[5-9]\d{2}|20\d{2})\b", texto(valor) or "")
    return None, int(m.group()) if m else None
