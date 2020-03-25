from enum import Enum


class ModelName(Enum):
    linear = 'Linear'
    logistic = 'Logistic'
    dt = 'DecisionTree'
    c45 = 'CART4.5'
    svm = 'SVM'
    xgboost = 'XGBoost'
    randomforest = 'RandomForest'
    gb = 'GradientBoosting'
    mlp = 'MultiLayerPerceptron'

    @classmethod
    def available_modes(self):
        # return [self.linear, self.logistic, self.dt, self.randomforest, self.svm, self.mlp]  #  self.svm, self.gb self.xgboost,
        return [self.randomforest]

    @classmethod
    def get_short_name(self, model_name):
        return \
        dict(zip([self.linear, self.logistic, self.dt, self.c45, self.svm, self.xgboost, self.randomforest, self.mlp],
                 ['Linear', 'LR', 'DecisionTree', 'C45', 'SVM', 'XGB', 'RF', 'MLP']))[model_name]  # , self.gb
