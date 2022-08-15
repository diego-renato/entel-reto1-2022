import pandas as pd

class DataframeColumnDuplicateTransformer():
    def __init__(self, columns=None):
        self.columns = columns

    def transform(self, input_df, **transform_params):
        columns_edited = [column_i +'_count' for column_i in self.columns]
        input_df.loc[:,columns_edited] = input_df[self.columns].copy().values
        return input_df

    def fit(self, X, y=None, **fit_params):
        return self

