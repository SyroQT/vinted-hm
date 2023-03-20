import os
from pathlib import Path
from collections import defaultdict

import pandas as pd

class MapReduce:
    """
    Simple implementation of the Map Reduce framework 
    For usage example see example.py
    """
    def __init__(self, maper, reducer, output):
        # set up the needed state
        self.__state = defaultdict(list)
        self.__df_state = pd.DataFrame()

        self.__map(maper)
        self.__reduce(reducer)
        self.__write_csv(output)

    def __map(self, maper: dict):
        for directory, func in maper.items():
            for file in os.listdir(directory):
                df = self.__read_csv(directory, file)
                # user defined function 
                result = df.apply(func, axis=1).dropna()
                # shuffle step
                for key, value in result:
                    self.__state[key].append(value)
        
                 
    def __reduce(self, reducer: callable):
        reduced_entries = map(reducer, self.__state.items())
        for entry in reduced_entries:
            if entry:
                self.__df_state = pd.concat([self.__df_state, pd.DataFrame(entry)])
        self.__df_state.reset_index(inplace=True, drop=True)

    def __read_csv(self, directory:str, path: str) -> pd.DataFrame:
        return pd.read_csv(directory + "/" + path)
    
    def __write_csv(self, output_dir: str):
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        file_number = len(os.listdir(output_dir)) + 1
        file_name = f"part-{file_number:03}.csv"
        self.__df_state.to_csv(output_dir + "/" + file_name)

