import json

from typing import Dict, List, Set, Union

from redis import Redis
from datasets import Dataset, DownloadMode, load_dataset

from constants import SUBSETS_FEATURES

from utils.common_utils import identity, multi_map
from utils.embeddings_utils import get_adawat_embeddings
from utils.clusters_utils import get_adawat_clusters


def refresh_adawat_and_tags(db: Redis) -> None:
    adawat = load_dataset(
        'arbml/adawat',
        download_mode=DownloadMode.FORCE_REDOWNLOAD,
        ignore_verifications=True,
    )['train']

    tags = get_features_tags(adawat)
    adawat = list(adawat)

    embeddings = get_adawat_embeddings(adawat, db)
    clusters, reduced_embeddings = get_adawat_clusters(embeddings)

    for index, (dataset, dataset_cluster, dataset_reduced_embeddings) in enumerate(
        zip(
            adawat,
            clusters,
            reduced_embeddings,
        )
    ):
        dataset['Id'] = index + 1
        dataset['Cluster'] = dataset_cluster
        dataset['Embeddings'] = dataset_reduced_embeddings

    db.set('adawat', json.dumps(adawat))
    db.set('tags', json.dumps(tags))


def get_features_tags(adawat: Dataset) -> Dict[str, List[Union[str, int]]]:
    tags: Dict[str, Union[Set[str], List[Union[str, int]]]] = dict()

    for feature in adawat.features:
        if feature == 'Tasks':
            tags = process_tasks_feature(tags, adawat['Tasks'])
        else:
            tags[feature] = sorted(set(adawat[feature]))

    for feature in tags:
        try:
            tags[feature].remove('nan')
        except ValueError:
            pass

    return tags


def extract_country_from_dialect_feature(dialect_feature: str) -> str:
    country = dialect_feature.split('(')[-1].split(')')[0]

    if country == 'Modern Standard Arabic':
        return 'MSA'

    return country

def process_dialect_feature(
    tags: Dict[str, Union[Set[str], List[Union[str, int]]]],
    dialect_feature: List[str],
) -> Dict[str, Union[Set[str], List[Union[str, int]]]]:
    tags['Dialect'] = set()

    for dialects in dialect_feature:
        tags['Dialect'].update(
            list(
                multi_map(
                    dialects.split(','),
                    extract_country_from_dialect_feature,
                    str.strip,
                ),
            ),
        )

    tags['Dialect'] = sorted(tags['Dialect'])

    return tags


def process_tasks_feature(
    tags: Dict[str, Union[Set[str], List[Union[str, int]]]],
    tasks_feature: List[str],
) -> Dict[str, Union[Set[str], List[Union[str, int]]]]:
    tags['Tasks'] = set()

    for tasks in tasks_feature:
        tags['Tasks'].update(
            list(
                filter(
                    identity,
                    map(str.strip, tasks.split(',')),
                ),
            ),
        )

    tags['Tasks'] = sorted(tags['Tasks'])

    return tags
