import numpy as np
from numpy.typing import NDArray
from sklearn.base import ClassifierMixin, BaseEstimator


class Model(BaseEstimator, ClassifierMixin):
    def __init__(
        self,
        symptoms_bias: float = -1.32,
        symptoms_age: float = -0.01,
        symptoms_sex: float = 0.44,
        symptoms_smell: float = 1.75,
        symptoms_cough: float = 0.31,
        symptoms_fatigue: float = 0.49,
        sensor_rhr: float = 40,
        sensor_sleep: float = 56.06,
        sensor_activity: float = 2489.85,
        **kwargs
    ):
        self.symptoms_bias = symptoms_bias
        self.symptoms_age = symptoms_age
        self.symptoms_sex = symptoms_sex
        self.symptoms_smell = symptoms_smell
        self.symptoms_cough = symptoms_cough
        self.symptoms_fatigue = symptoms_fatigue
        self.sensor_rhr = sensor_rhr
        self.sensor_sleep = sensor_sleep
        self.sensor_activity = sensor_activity

        super().__init__(**kwargs)

    def predict(self, x: NDArray) -> NDArray:
        """
        make predictions based on feature vector x
        as described by Quer et al. 2021 (http://dx.doi.org/10.1038/s41591-020-1123-x)

        Due to inferior typing of numpy.arrays description of the feature vector in prose:

        x is assumed to have the type:
        x: ArrayLike[Features] where Features is
        [
            median_daily_rhr_baseline, max_daily_rhr_test,
            median_daily_sleep_baseline, mean_daily_sleep_test,
            median_daily_activity_baseline, mean_daily_activity_test,
            age, sex, loss_of_smell, cough, fatigue
        ]
        """
        print(len(x.T))
        sensor_data = x.T[:6]
        symptom_data = x.T[6:]

        def symptom_metric(
            age: NDArray,
            sex: NDArray,
            smell: NDArray,
            cough: NDArray,
            fatigue: NDArray,
        ) -> NDArray:
            return (
                self.symptoms_bias
                + self.symptoms_age * age
                + self.symptoms_sex * sex
                + self.symptoms_smell * smell
                + self.symptoms_cough * cough
                + self.symptoms_fatigue * fatigue
            )

        def sensor_metric(
            rhr_baseline: NDArray,
            rhr_test: NDArray,
            sleep_baseline: NDArray,
            sleep_test: NDArray,
            activity_baseline: NDArray,
            activity_test: NDArray,
        ) -> NDArray:
            return (
                (rhr_test - rhr_baseline) / self.sensor_rhr
                + (sleep_test - sleep_baseline) / self.sensor_sleep
                + (activity_test - activity_baseline) / self.sensor_activity
            )

        return 1 / (
            1 + np.exp(symptom_metric(*symptom_data) + sensor_metric(*sensor_data))
        )
