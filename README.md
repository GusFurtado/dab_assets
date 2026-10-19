# DadosAbertosBrasil-wiki

Tabelas raspadas da Wikipedia em português que alimentam o módulo `wiki` do
**[DadosAbertosBrasil](https://github.com/GusFurtado/DadosAbertosBrasil)**.

Os scrapers rodam toda semana no GitHub Actions. Quando os dados mudam, um PR é aberto com o
resumo das mudanças. Quando a página da Wikipedia muda de formato e o scraper quebra, uma issue
é aberta com o erro.

## Datasets

| Arquivo | Conteúdo | Fonte |
|---|---|---|
| [`governadores.json`](data/governadores.json) | Governadores em exercício nas 27 UFs | [Lista de governadores das unidades federativas do Brasil](https://pt.wikipedia.org/wiki/Lista_de_governadores_das_unidades_federativas_do_Brasil) |
| [`prefeitos_capitais.json`](data/prefeitos_capitais.json) | Prefeitos em exercício nas 26 capitais | [Lista de prefeitos das capitais do Brasil](https://pt.wikipedia.org/wiki/Lista_de_prefeitos_das_capitais_do_Brasil) |
| [`ministros.json`](data/ministros.json) | Gabinete do governo federal atual, com histórico | [Lista de membros do gabinete de Lula (2023–presente)](https://pt.wikipedia.org/wiki/Lista_de_membros_do_gabinete_de_Lula_(2023%E2%80%93presente)) |
| [`tribunais.json`](data/tribunais.json) | Composição atual de STF, STJ, TST, STM e TCU | Páginas de cada tribunal |
| [`partidos.json`](data/partidos.json) | Partidos em atividade e extintos desde o Império | [Lista de partidos políticos do Brasil](https://pt.wikipedia.org/wiki/Lista_de_partidos_pol%C3%ADticos_do_Brasil) e [extintos](https://pt.wikipedia.org/wiki/Lista_de_partidos_pol%C3%ADticos_brasileiros_extintos) |

O formato de cada arquivo está descrito em [`schemas/`](schemas) (JSON Schema).

## Formato

Todo arquivo em `data/` tem o mesmo envelope:

```json
{
  "schema_version": 1,
  "dataset": "governadores",
  "descricao": "Governadores em exercício nas 27 unidades federativas.",
  "licenca": "CC BY-SA 4.0 (conteúdo derivado da Wikipedia)",
  "gerado_em": "2026-10-19T17:06:36Z",
  "fontes": [{"titulo": "...", "revid": 73144165, "url": "https://pt.wikipedia.org/w/index.php?oldid=73144165"}],
  "dados": [{"uf": "AC", "nome": "Mailza Assis", "...": "..."}]
}
```

- `fontes[].revid` aponta para a revisão exata da página usada na raspagem.
- `dados` é uma lista de registros. UFs são sempre identificadas pela sigla.
- Campos `artigo` trazem o título do artigo da Wikipedia sobre a pessoa ou entidade, quando existe.
- O arquivo só é regravado quando `dados` muda.

## Versões

O DadosAbertosBrasil lê os arquivos direto deste repositório via `raw.githubusercontent.com`, então
cada branch é um contrato com uma linha de versões do pacote:

| Branch | Usado por | Situação |
|---|---|---|
| `2.x` | DadosAbertosBrasil < 3.0 | Congelado com os arquivos antigos |
| `3.0.0` | DadosAbertosBrasil 3.0 (em desenvolvimento) | Formato descrito acima |

Mudanças incompatíveis no formato incrementam `schema_version`.

## Desenvolvimento

Requer [uv](https://docs.astral.sh/uv/).

```bash
uv sync
uv run dab-wiki atualizar              # raspa todos os datasets e grava data/ se mudou
uv run dab-wiki atualizar governadores # só um dataset
uv run dab-wiki validar                # valida data/ contra os modelos
uv run dab-wiki schemas                # regenera schemas/
uv run pytest                          # testes offline, com o HTML salvo em tests/fixtures/
uv run pytest -m online                # raspa a revisão atual da Wikipedia
```

### Adicionar um dataset

1. Crie `src/dab_wiki/datasets/<nome>.py` com um modelo `Registro` e uma subclasse de `Dataset`
   (veja [`governadores.py`](src/dab_wiki/datasets/governadores.py)).
2. Registre a classe em [`datasets/__init__.py`](src/dab_wiki/datasets/__init__.py).
3. Rode `uv run dab-wiki fixture <nome>`, escreva os testes em `tests/` e gere os arquivos com
   `uv run dab-wiki atualizar <nome>` e `uv run dab-wiki schemas`.

As tabelas são localizadas pelo cabeçalho, não pela posição na página. Se uma coluna esperada
sumir, o scraper falha com `EstruturaAlterada` em vez de publicar dados errados.

### Automação

- [`ci.yml`](.github/workflows/ci.yml): ruff, testes, validação de `data/` e de `schemas/` a cada push e PR.
- [`atualizar.yml`](.github/workflows/atualizar.yml): toda segunda-feira, um job por dataset. Abre
  PR (`bot/<dataset>-<branch>`) quando os dados mudam e issue (`scraper-quebrado`) quando falha. A
  issue é fechada sozinha quando o scraper volta a funcionar. Também pode ser disparado à mão pela aba
  *Actions*.

Agendamentos do GitHub Actions só rodam no branch padrão.

## Licença

Código sob a [licença MIT](LICENSE). Os dados são derivados da Wikipedia e seguem a licença
[CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/deed.pt-br).
