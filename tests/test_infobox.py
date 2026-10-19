from datetime import date

from dab_wiki.infobox import dtlink, infobox, infobox_externa, texto

SANTOS = """{{Info/Município do Brasil
| fundação = {{nowrap|{{dtlink|26|1|1546|idade}}}}
| gentílico = [[:wikt:santista|santista]]<ref name="x">{{citar web|url=a}}</ref>
| lema = ''Patriam charitatem et libertatem docui''<br /><small>''"À Pátria"''</small>
| aniversário = 25 de abril
}}"""


def test_infobox():
    info = infobox(SANTOS, "Info/Município do Brasil")
    assert texto(info["gentílico"]) == "santista"
    assert texto(info["lema"]) == "Patriam charitatem et libertatem docui"
    assert texto(info["aniversário"]) == "25 de abril"
    assert dtlink(info["fundação"]) == (date(1546, 1, 26), 1546)


def test_dtlink_so_ano():
    info = infobox("{{Info/X|fundação={{dtlink|||1698|idade}} - por bandeirantes<ref>x</ref>}}")
    assert dtlink(info["fundação"]) == (None, 1698)


def test_infobox_quebrada():
    # Marcação desbalanceada: o parser não reconhece a predefinição; a leitura por linhas resolve.
    wikitexto = "{{Info/Estado do Brasil\n |nome = Santa Catarina\n |nota = {{abre\n |gentílico = catarinense\n}}"
    assert texto(infobox(wikitexto, "Info/Estado do Brasil")["gentílico"]) == "catarinense"


def test_texto_quebra_inicial():
    assert texto(infobox("{{Info/X\n|gentílico = <br />sul-mato-grossense\n}}")["gentílico"]) == "sul-mato-grossense"


def test_infobox_externa():
    assert infobox_externa("{{Info/Estado do Brasil/São Paulo}}") == "Predefinição:Info/Estado do Brasil/São Paulo"
    assert infobox_externa(SANTOS) is None
