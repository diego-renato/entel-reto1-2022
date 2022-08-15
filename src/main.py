import pandas as pd
from src.data_prep.book import _get_abt_book
from src.data_prep.utils import create_pyspark_session, load_dict, load_data
from src.ml.preditor import _predict
from pyspark.sql import functions as F

def excute_get_abt_save(spark):
    table_dict = load_dict()
    suscriptora_df = load_data(table_dict['suscriptora_df'], spark)
    target_df = load_data(table_dict['target_df'], spark)
    adenda = load_data(table_dict['adenda'], spark)
    perfil_digital = load_data(table_dict['perfil_digital'], spark)
    roaming = load_data(table_dict['roaming'], spark)
    terminales = load_data(table_dict['terminales'], spark)
    trafico = load_data(table_dict['trafico'], spark)
    convergente = load_data(table_dict['convergente'], spark)
    (_get_abt_book(suscriptora_df, target_df, adenda,
              perfil_digital, roaming, terminales,
              trafico, convergente, spark)
              .drop(*['FECINGRESOCLIENTE', 'FECACTIVACIONCONTRATO', 'NUMPERIODO_data'])
              .coalesce(1)
              .write
              .csv("data/transformed_data/entel_book_features_reto1.csv", sep=',',header=True)
    )

def execute_get_prediction_save(spark, ano_mes: int):
    book_last_event =( load_data('data/transformed_data/entel_book_features_reto1.csv', spark)
                            .filter(F.col('NUMPERIODO')==ano_mes)
                            .drop(*['TARGET'])
                            .toPandas()
                     )
    book_last_event['TARGET'] = _predict(book_last_event, model_path='models/pipeline_model.pkl')    
    (book_last_event[['nro_telefono_hash', 'TARGET']].set_index('nro_telefono_hash')
                    .to_csv(f"data/scores_data/prediction_mes_{ano_mes}.csv")
    )       
    print('Transformación hecha')
    