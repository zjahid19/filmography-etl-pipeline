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
from detail_log import logger


if __name__ == '__main__':
  logger.info('ETL Pipleine Stats from here')
  base_url = 'https://en.wikipedia.org/'
  logger.info(f'base_url parameter value is {base_url}')
  filmography_url = 'https://en.wikipedia.org/wiki/Aamir_Khan_filmography'
  logger.info(f'filmography_url parameter value is {filmography_url}')
  
  # Extract
  logger.info("=========================  EXTRACTION-started  ============================")
  fetcher = MovieDetailsFetcher(base_url, filmography_url)
  movies_details = fetcher.fetch_movie_details()
  # logger.info(f'Total records extracted - {len(movies_details)}')
  logger.info("=========================  EXTRACTION-completed  ===========================")

 
  # Transform
  logger.info("=========================  TRANSFORMATION-started  =========================")
  trans = Transformer(movies_details)
  movies_df = trans.cleaned_dataframe()
  logger.info("=========================  TRANSFORMATION-completed  ========================")
  # print(movies_df.head())
  
  # Load
  # logger.info("=========================  LOADING-started  =========================")
  # loader = DataLoading(movies_df)
  # loader.loading()
  # logger.info("=========================  LOADING-Completed  =========================")





  




