from kafka import KafkaConsumer
import json
import yaml
from datetime import datetime
import numpy as np
from kliep import DensityRatioEstimator
import pickle
from sklearn.ensemble import RandomForestRegressor
from sklearn.utils.validation import check_X_y, check_array, check_consistent_length


class RandomForestIW(RandomForestRegressor):
    """
    A random forest regressor that accepts sample weights as importance weighting\
    """
    def fit(self, X, y, sample_weight=None):
        """
        Fit the random forest model to the training data with optional importance weighting
        """
        X, y = check_X_y(X, y, accept_sparse="csc", dtype=["float32", "float64"])
        if sample_weight is not None:
            sample_weight = check_array(sample_weight, ensure_2d=False)
            check_consistent_length(sample_weight, y)
            # normalize the sample weights to sum up to the number of samples
            sample_weight = sample_weight * len(y) / sample_weight.sum()
        super().fit(X, y, sample_weight)


# define the function to process the data and perform prediction
def predict_ESR(X):
        """
        Predict drift using a pre-trained ML Regressor and importance weighting
        :param data: streaming data to predict the minimum pressure value
        """
        # Load the pre-trained model
        with open('model.pkl', 'rb') as file:
            regressor = pickle.load(file)

        # Calculate importance weights using Density Ratio Estimation with Kullback-Leibler Importance Estimation Procedure (KLIEP)
        kliep = DensityRatioEstimator()
        kliep.fit(X, X)
        weight = kliep.predict(X)

        # Predict drift using importance weighting
        drift_pred = regressor.predict(X, sample_weight=weight)

        # Print drift predictions with IW
        print("IW Predictions:")
        print(drift_pred)

        #   Prediction without importance weighting
        pred = regressor.predict(X)

        # Print drift predictions without IW
        print("Without IW Predictions:")
        print(pred)



if __name__ == '__main__':
    with open(r'config.yaml') as file:
        config = yaml.load(file, Loader=yaml.FullLoader)
    consumer = KafkaConsumer(config['topic'], group_id=config['group_id'],
                             bootstrap_servers=config['bootstrap_servers'],
                             auto_offset_reset=config['auto_offset_reset'])
    for message in consumer:
        msg = json.loads(message.value.decode('utf-8'))
        data.append(msg[0])
        print(f"Received data point: {msg[0]}")
        # Check if this is a new pump
        new_pump = True
        if (new_pump == msg[1]):
            data = []
            start_time = datetime.now()
        # Check if 30 seconds have passed since last prediction
        time_diff = (datetime.now() - start_time).total_seconds()
        if time_diff >= 30:
            print("30 seconds passed. Processing data and performing prediction...")
            # Call the prediction function here to perform prediction
            predict_ESR(np.array(data, dtype=float))
            data = []
            start_time = datetime.now()
