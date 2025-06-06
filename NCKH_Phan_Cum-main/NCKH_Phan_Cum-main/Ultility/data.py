import os
import numpy as np
import pandas as pd


TEST_CASES = {
    53: {
        'name': 'Iris',
        'n_cluster': 3,
        'test_points': [5.1, 3.5, 1.4, 0.2]
    },
    109: {
        'name': 'Wine',
        'n_cluster': 4,
        'test_points': [14.23, 1.71, 2.43, 15.6,
                        127, 2.80, 3.06, 0.28,
                        2.29, 5.64, 1.04, 3.92,
                        1065]
    },
    602: {
        'name': 'DryBean',
        'n_cluster': 7,
        'test_points': [
            28395, 610.291, 208.178117, 173.888747,
            1.197191, 0.549812, 28715, 190.141097,
            0.763923, 0.988856, 0.958027, 0.913358,
            0.007332, 0.003147, 0.834222, 0.998724]
    }
}


def round_float(number: float) -> float:
    return round(number, 3)


def euclidean_distances(A: np.ndarray, B: np.ndarray, axis: int = None):
    return np.linalg.norm(A - B, axis=axis)


LOCAL_DATASETS = {
    53: "/NCKH/data/UCI/Iris.csv",
    109: "/NCKH/data/UCI/Wine.csv",
    602: "/NCKH/data/UCI/Dry_Bean.csv"
}

def fetch_data_from_uci(dataset_id: int) -> dict:
    """Lấy dữ liệu từ file cục bộ dựa trên dataset_id"""
    if dataset_id not in LOCAL_DATASETS:
        raise ValueError(f"Dataset ID {dataset_id} không tồn tại trong danh sách.")
    file_path = LOCAL_DATASETS[dataset_id]
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Không tìm thấy file dữ liệu: {file_path}")
    df = pd.read_csv(file_path)
    features = df.iloc[:, :-1].values  #
    labels = df.iloc[:, -1].values  
    return {'X': features, 'y': labels}

def fetch_data_from_uci1(dataset_id: int) -> dict:
    """Lấy dữ liệu từ file cục bộ dựa trên dataset_id"""
    if dataset_id not in LOCAL_DATASETS:
        raise ValueError(f"Dataset ID {dataset_id} không tồn tại trong danh sách.")
    file_path = LOCAL_DATASETS[dataset_id]
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Không tìm thấy file dữ liệu: {file_path}")
    df = pd.read_csv(file_path)
    features = df.iloc[:, 1:].values  #
    labels = df.iloc[:, 0].values  
    return {'X': features, 'y': labels}