import feather
import numpy as np
import pandas as pd

from scipy import sparse
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split

path = 'data/data_sparse.npz'
X = sparse.load_npz(path)

path = 'data/info_df.feather'
info = feather.read_dataframe(path)

y = info.crsp_obj_cd

part = 100000

# y = y
# X = X[:, 1:110000]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=42)


print('Start logistic regression...')
clf = LogisticRegression(random_state=0, solver='lbfgs',
                         multi_class='multinomial').fit(X_train, y_train)

print('Predict...')
# clf.predict(X[:2, :])
#
# clf.predict_proba(X[:2, :])


print(clf.score(X_test, y_test))

# from sklearn.metrics import accuracy_score

# accuracy_score(y_true, y_pred)
