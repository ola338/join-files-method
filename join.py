import numpy as np
import pandas as pd
from collections import OrderedDict


class Join:
    def __init__(self, file_path1, file_path2, col_name, join_type='inner', index=0):
        self.file_path1 = file_path1
        self.file_path2 = file_path2
        self.col_name = col_name
        self.join_type = join_type
        self.index = index


    def get_data_and_set_join_type(self):
        data1 = pd.read_csv(self.file_path1)
        data2 = pd.read_csv(self.file_path2)
        if self.join_type == 'right':
            data1 = pd.read_csv(self.file_path2)
            data2 = pd.read_csv(self.file_path1)
            self.join_type = 'left'
        return data1, data2


    def create_df(self, data1, data2):
        all_columns = list(data1.columns) + list(data2.columns)
        new_columns = list(OrderedDict.fromkeys(all_columns))
        joined_columns = [col 
                        for col in data2.columns 
                        if (col not in data1.columns and col != self.col_name)]
        joined_df = pd.DataFrame(np.full((1,len(new_columns)), ''), columns=new_columns)
        return joined_df, joined_columns


    def inner_join(self, data1, data2, joined_df, joined_columns):
        joined_data = joined_df
        for el, el_idx in zip(data1[self.col_name], data1.index):
            iters = data2[data2[self.col_name] == el].shape[0] 
            for i in range(iters):
                data1_records = list(data1.loc[el_idx])
                data2_records = list(data2[data2[self.col_name] == el][joined_columns].iloc[i])
                joined_data.loc[self.index] = data1_records + data2_records
                self.index +=1  
        return joined_data


    def left_join(self, data1, data2, joined_df, joined_columns):
        joined_data = joined_df
        for el, el_idx in zip(data1[self.col_name], data1.index):
            iters = data2[data2[self.col_name] == el].shape[0] 
            data1_records = list(data1.loc[el_idx])
            if iters == 0:
                joined_data.loc[self.index] = data1_records + np.full((1, len(joined_columns)), np.nan).tolist()[0]
                self.index +=1
            for i in range(iters):
                data2_records = list(data2[data2[self.col_name] == el][joined_columns].iloc[i])
                joined_data.loc[self.index] = data1_records + data2_records
                self.index +=1     
        return joined_data


    def full_join(self, data1, data2, joined_df, joined_columns):
        joined_data = self.left_join(data1, data2, joined_df, joined_columns)
        for el in data2[self.col_name]:
            if el not in list(data1[self.col_name]):
                data2_records = list(data2[data2[self.col_name] == el].iloc[0])        
                nan_shape = joined_data.shape[1]-len(joined_columns)-1
                joined_data.loc[self.index] = [data2_records[0]] + np.full((1, nan_shape), np.nan).tolist()[0] + data2_records[1:]
        return joined_data


    def get_action(self, data1, data2, joined_df, joined_columns):
        if self.join_type=='full':
            joined_data = self.full_join(data1, data2, joined_df, joined_columns) 
        elif self.join_type=='left':
            joined_data = self.left_join(data1, data2, joined_df, joined_columns)
        elif self.join_type=='inner':
            joined_data = self.inner_join(data1, data2, joined_df, joined_columns)
        else:
            raise ValueError('Incorrect join type')
        return joined_data


    def save_to_csv(self, joined_data, file_name="joined_file.csv"):
        joined_data.to_csv(file_name, index=False)