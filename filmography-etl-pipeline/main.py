"""
main.py

This module provides functionality to Execute Extract, Transform and Load Process in Sequence.

Author: Jahid Sheikh
Date: 2025-03-09
"""

from extraction_oop import MovieDetailsFetcher
from transformer_oop import Transformer
from load import DataLoading
import pandas as pd
import logging


if __name__ == '__main__':
  base_url = 'https://en.wikipedia.org/'
  filmography_url = 'https://en.wikipedia.org/wiki/Aamir_Khan_filmography'

  # Extract
  print('Data Extraction Started')
  fetcher = MovieDetailsFetcher(base_url, filmography_url)
  movies_details = fetcher.fetch_movie_details()
 
  # Transform
  print('Data Transformation Started')
  trans = Transformer(movies_details)
  movies_df = trans.cleaned_dataframe()
  
  # Load
  # print('Data Loading Started')
  # loader = DataLoading(movies_df)
  # loader.loading()






  




