from typing import List, Tuple

from sklearn.cluster import KMeans
from sklearn.manifold import TSNE
import numpy as np

def get_adawat_clusters(embeddings: List[List[float]]) -> Tuple[List[int], List[List[float]]]:
    reduced_embeddings = compute_reduced_embeddings(embeddings)

    return compute_clusters(reduced_embeddings), reduced_embeddings


def compute_clusters(embeddings: List[List[float]]) -> List[int]:
    clustering = KMeans(n_clusters=15, random_state=42).fit(embeddings)

    return clustering.labels_.tolist()


def compute_reduced_embeddings(embeddings: List[List[float]]) -> List[List[float]]:
    tsne_model = TSNE(n_components=2, random_state=42)
    embeddings = np.asarray(embeddings)
    tsne_data = tsne_model.fit_transform(embeddings)

    return (tsne_data - tsne_data.min()).tolist()
