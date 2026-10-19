from dab_wiki.base import Dataset
from dab_wiki.datasets.governadores import Governadores
from dab_wiki.datasets.ministros import Ministros
from dab_wiki.datasets.municipios import Municipios
from dab_wiki.datasets.partidos import Partidos
from dab_wiki.datasets.prefeitos_capitais import PrefeitosCapitais
from dab_wiki.datasets.tribunais import Tribunais
from dab_wiki.datasets.ufs import UFs

DATASETS: dict[str, Dataset] = {
    d.nome: d for d in [Governadores(), PrefeitosCapitais(), Ministros(), Tribunais(), Partidos(), Municipios(), UFs()]
}
