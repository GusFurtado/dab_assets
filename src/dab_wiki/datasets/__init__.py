from dab_wiki.base import Dataset
from dab_wiki.datasets.governadores import Governadores

DATASETS: dict[str, Dataset] = {d.nome: d for d in [Governadores()]}
