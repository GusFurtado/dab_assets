from datetime import date
from pathlib import Path
import json

from bs4 import BeautifulSoup
import pandas as pd
import requests


class ScrapeGovernadores:
    URL = r"https://pt.wikipedia.org/wiki/Lista_de_governadores_das_unidades_federativas_do_Brasil"

    COLS = {
        "Unidade federativa": "uf",
        "Governador.1": "nome",
        "No cargo": "periodo",
        "Partido": "partido",
        "Mandato (ano da eleição)": "ano_eleicao",
        "Vice-governador": "vice_governador",
    }

    MESES = {
        "janeiro": 1,
        "fevereiro": 2,
        "março": 3,
        "abril": 4,
        "maio": 5,
        "junho": 6,
        "julho": 7,
        "agosto": 8,
        "setembro": 9,
        "outubro": 10,
        "novembro": 11,
        "dezembro": 12,
    }

    def apply_periodo(self, texto: str) -> str:
        texto = texto.replace("º", "").replace("ª", "")
        texto = texto.split(" de ")
        dia = int(texto[0])
        mes = self.MESES[texto[1].lower()]
        ano = int(texto[2][:4])
        return str(date(ano, mes, dia))

    def apply_vice(self, x: str) -> str:
        if x.startswith("—"):
            return None
        else:
            return x.split("1")[0].split("[")[0].strip()

    def get(self) -> pd.DataFrame:
        r = requests.get(self.URL)
        soup = BeautifulSoup(r.text, features="lxml")
        table = soup.find_all("table")[0]
        return pd.read_html(str(table))[0]

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df["Unidade federativa"] = df["Unidade federativa"].apply(
            lambda x: x.split("(")[0].strip()
        )
        df = df[list(self.COLS.keys())].copy()
        df.rename(columns=self.COLS, inplace=True)
        df = df.groupby("uf").last()
        df["nome_completo"] = df.nome.apply(lambda x: x.split("(")[-1].split(")")[0])
        df.nome = df.nome.apply(lambda x: x.split("(")[0].strip())
        df["mandato_inicio"] = df.periodo.apply(
            lambda x: self.apply_periodo(x.split("–")[0])
        )
        df["mandato_fim"] = df.periodo.apply(
            lambda x: self.apply_periodo(x.split("–")[1])
        )
        df["partido_sigla"] = df.partido.apply(lambda x: x.split("(")[-1].split(")")[0])
        df.partido = df.partido.apply(lambda x: x.split("(")[0].strip())
        df.ano_eleicao = df.ano_eleicao.apply(lambda x: x.split("(")[-1].split(")")[0])
        df.ano_eleicao = df.ano_eleicao.apply(lambda x: None if x == "1" else int(x))
        df.vice_governador = df.vice_governador.apply(self.apply_vice)

        return df[
            [
                "nome",
                "nome_completo",
                "ano_eleicao",
                "mandato_inicio",
                "mandato_fim",
                "partido",
                "partido_sigla",
                "vice_governador",
            ]
        ]

    def save(self, df: pd.DataFrame) -> None:
        data = df.to_json(orient="index")
        with open("data/governadores.json", "w") as f:
            json.dump(data, f)

    def exec(self) -> None:
        df = self.get()
        df = self.transform(df)
        self.save(df)
        return df


def validar():
    file = Path("data/governadores.json")
    assert file.is_file()

    with open(file, "r") as opened_file:
        return json.load(opened_file)


if __name__ == "__main__":
    obj = ScrapeGovernadores()
    obj.exec()

    data = validar()
    print(data)
