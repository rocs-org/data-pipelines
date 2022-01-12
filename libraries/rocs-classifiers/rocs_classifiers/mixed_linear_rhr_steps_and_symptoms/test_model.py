import numpy as np
from numpy.random import uniform, choice
from sklearn.base import ClassifierMixin, BaseEstimator
from .model import Model


def test_model_implements_classifier():
    assert issubclass(Model, ClassifierMixin)


def test_model_implements_base_estimator():
    assert issubclass(Model, BaseEstimator)


def test_default_parameter_values_for_symptom_model():
    model = Model()
    assert model.symptoms_bias == -1.32
    assert model.symptoms_age == -0.01
    assert model.symptoms_sex == 0.44
    assert model.symptoms_smell == 1.75
    assert model.symptoms_cough == 0.31
    assert model.symptoms_fatigue == 0.49


def test_model_predict_returns_probabilities():
    N = 10
    model = Model()
    vital_feature_test_data = uniform(low=0, high=100, size=(6, N))
    age_test_data = choice(range(10, 100), size=(1, N))
    symptom_feature_test_data = choice([0, 1], size=(4, N))

    feature_test_data = np.concatenate(
        (vital_feature_test_data, age_test_data, symptom_feature_test_data), axis=0
    ).T

    assert feature_test_data.shape == (N, 11)

    predictions = model.predict(feature_test_data)

    assert predictions.shape == (N,)
