import numpy as np
def divide_data_for_collaborativ(data, labels, n_data_site):
    #Xáo trộn bọ dữ liệu rồi đem chia đều cho n_data_site
    np.random.seed(42)

    indices = np.arange(len(data))
    np.random.shuffle(indices)

    X_shuffled = data[indices]
    y_shuffled = labels[indices]

    sub_data = np.array_split(X_shuffled, n_data_site, axis=0)
    sub_data_labels = np.array_split(y_shuffled, n_data_site, axis=0)

    return sub_data, sub_data_labels