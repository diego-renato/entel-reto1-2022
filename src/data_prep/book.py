from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def _get_abt_book(suscriptora_hist_df: DataFrame, target_df: DataFrame,
                 adenda: DataFrame, perfil_digital: DataFrame, roaming: DataFrame, 
                 terminales: DataFrame, trafico: DataFrame, convergente: DataFrame,
                 spark) -> DataFrame:
    '''
    Crea un conjunto de variables que pasaran en el modelo final
    '''
    trafico = ( trafico
                      .withColumn(
                        'trafico_app_total', F.col('trafico_app_1')+
                                             F.col('trafico_app_2')+
                                             F.col('trafico_app_3')+
                                             F.col('trafico_app_4')+
                                             F.col('trafico_app_5')+
                                             F.col('trafico_app_6')+
                                             F.col('trafico_app_7')+
                                             F.col('trafico_app_8')+
                                             F.col('trafico_app_9')
                                    )
                     .withColumn(
                         'mins_flujo_total', F.col('mins_flujo_1')+
                                             F.col('mins_flujo_1')
                                    )
                     .withColumn(
                         'trafico_app_tasa', F.col('trafico_app_total')/
                                             F.col('trafico_total')
                                    )
                     .withColumn(
                         'nro_traficos_ceros',F.when(F.col('trafico_app_1')==0,1).otherwise(0)+
                                              F.when(F.col('trafico_app_2')==0,1).otherwise(0)+
                                              F.when(F.col('trafico_app_3')==0,1).otherwise(0)+
                                              F.when(F.col('trafico_app_4')==0,1).otherwise(0)+
                                              F.when(F.col('trafico_app_5')==0,1).otherwise(0)+
                                              F.when(F.col('trafico_app_6')==0,1).otherwise(0)+
                                              F.when(F.col('trafico_app_7')==0,1).otherwise(0)+
                                              F.when(F.col('trafico_app_8')==0,1).otherwise(0)+
                                              F.when(F.col('trafico_app_9')==0,1).otherwise(0)                                                    
                                    )
                       .withColumn(
                         'trafico_app_8_tasa',F.col('trafico_app_8')/
                                             F.col('trafico_total')                                    
                                    )
                   )
    
    perfil_digital = perfil_digital.withColumn('grupo_correto', F.split(F.col('GRUPO'),pattern='\|'))\
                                  .withColumn('grupo_correto', F.explode(F.col('grupo_correto')))\
                                  .withColumnRenamed('PERIODO', 'NUMPERIODO')\
                                  .groupby('NUMPERIODO','nro_telefono_hash').agg(F.count('grupo_correto').alias('nro_grupos'),
                                                                                F.max('SCORECAT').alias('SCORECAT'))
    
    suscriptora_df_fe = (suscriptora_hist_df.withColumn('NUMPERIODO_data', 
                                                      F.last_day(
                                                          F.to_date(F.col('NUMPERIODO').cast('string'),'yyyyMM')
                                                                )
                                                     )
                                          .withColumn('dias_activo_cliente', 
                                                      F.datediff(
                                                              F.col('NUMPERIODO_data'),F.col('FECINGRESOCLIENTE')
                                                                 )
                                                     )
                                          .withColumn('dias_activo_contrato', 
                                                      F.datediff(
                                                              F.col('NUMPERIODO_data'),F.col('FECACTIVACIONCONTRATO')
                                                                 )
                                                     )
                                      .join(
                                            target_df.withColumnRenamed('PERIODO', 'NUMPERIODO')
                                          , on=['nro_telefono_hash','NUMPERIODO'], how='left'
                                          )
                                      .join(
                                            adenda
                                          , on=['nro_telefono_hash','NUMPERIODO'], how='left'
                                          )
                                      .join(
                                            perfil_digital
                                          , on=['nro_telefono_hash','NUMPERIODO'], how='left'
                                          )
                                      .join(
                                            roaming.withColumnRenamed('PERIODO', 'NUMPERIODO').groupBy('nro_telefono_hash', 'NUMPERIODO')
                                                     .agg(F.count('GIGAS').alias('roaming_count_M1'),
                                                          F.sum('GIGAS').alias('gigas_total_M1'),
                                                          F.sum('MINUTOS').alias('minutos_total_M1')
                                                         )
                                          , on=['nro_telefono_hash','NUMPERIODO'], how='left'
                                          )
                                      .join(terminales.withColumnRenamed('PERIODO', 'NUMPERIODO')
                                          , on=['nro_telefono_hash','NUMPERIODO'], how='left'
                                          )
                                      .join(
                                            trafico
                                          , on=['nro_telefono_hash','NUMPERIODO'], how='left'
                                          )
                                      .join(
                                            convergente.withColumnRenamed('PERIODO', 'NUMPERIODO')
                                          , on=['nro_documento_hash','NUMPERIODO'], how='left'
                                          )
                                     .withColumn('dias_lanzamento', 
                                                      F.datediff(
                                                              F.col('NUMPERIODO_data'),F.col('LANZAMIENTO')
                                                                 )
                                                     )
                        )
    # tabelas agrupadas
    documento_gb_df = (suscriptora_df_fe.groupBy('nro_documento_hash', 'NUMPERIODO')
                                        .agg(F.count('nro_documento_hash').alias('nro_contratos_atual'),
                                             F.sum('trafico_app_total').alias('total_trafico_app_total_atual'),
                                             F.sum('trafico_app_tasa').alias('total_trafico_app_tasa_atual'),
                                             F.sum('trafico_app_8').alias('total_trafico_app_8_atual'),
                                             F.sum('nro_grupos').alias('nro_grupos_documento_atual'),
                                             F.sum('VCHPENALIDAD').alias('total_VCHPENALIDAD_atual'),
                                             F.max('dias_lanzamento').alias('max_dias_lanzamento_celular_atual'),
                                             F.min('dias_lanzamento').alias('min_dias_lanzamento_celular_atual'),
                                             F.max('VCHPENALIDAD').alias('max_VCHPENALIDAD_atual'),
                                             F.min('VCHPENALIDAD').alias('min_VCHPENALIDAD_atual')
                                            )
                       )
    documento_gb_df_30d = (suscriptora_df_fe.groupBy('nro_documento_hash', 'NUMPERIODO')
                                        .agg(F.mean('TARGET').alias('cliente_target_mean_30d'),
                                             F.count('nro_documento_hash').alias('nro_contratos_30d'),
                                             F.sum('trafico_app_total').alias('total_trafico_app_total_30d'),
                                             F.sum('trafico_app_tasa').alias('total_trafico_app_tasa_30d'),
                                             F.sum('trafico_app_8').alias('total_trafico_app_8_30d'),
                                             F.sum('VCHPENALIDAD').alias('total_VCHPENALIDAD_30d')
                                            )
                                        .withColumn('NUMPERIODO', 
                                            F.date_format(
                                                F.add_months(
                                                        F.to_timestamp(F.col('NUMPERIODO').cast('string'), "yyyyMM")
                                                                 
                                                    ,1)
                                               , "yyyyMM"))
                       )
    telefono_gb_df_30d = (suscriptora_df_fe.groupBy('nro_telefono_hash', 'NUMPERIODO')
                                       .agg(F.mean('TARGET').alias('telefone_ultimo_evento_30d'),
                                             F.sum('VCHPENALIDAD').alias('VCHPENALIDAD_30d')
                                           )
                                       .withColumn('NUMPERIODO', 
                                            F.date_format(
                                                F.add_months(
                                                        F.to_timestamp(F.col('NUMPERIODO').cast('string'), "yyyyMM")
                                                                        ,1)
                                               , "yyyyMM"))
                       )
    telefono_gb_df_60d = (suscriptora_df_fe.groupBy('nro_telefono_hash', 'NUMPERIODO')
                                       .agg(F.mean('TARGET').alias('telefone_ultimo_evento_60d')
                                           )
                                       .withColumn('NUMPERIODO', 
                                            F.date_format(
                                                F.add_months(
                                                        F.to_timestamp(F.col('NUMPERIODO').cast('string'), "yyyyMM")
                                                                        ,2)
                                               , "yyyyMM"))
                       )
    documento_gb_pivot_df = (suscriptora_df_fe.groupBy('nro_documento_hash', 'NUMPERIODO')
                                              .pivot('TIPO_ADQ', ['tipo1', 'tipo2'])
                                              .agg(F.count('nro_documento_hash').alias('nro_contratos'),
                                                   F.sum('trafico_app_total').alias('trafico_app_total'),
                                                   F.sum('trafico_app_tasa').alias('trafico_app_tasa'),
                                                   F.sum('mins_flujo_total').alias('mins_flujo_total')
                                                  )
                             )
    documento_gb_pivot_df_30d = (suscriptora_df_fe.groupBy('nro_documento_hash', 'NUMPERIODO')
                                              .pivot('TIPO_ADQ', ['tipo1', 'tipo2'])
                                              .agg(F.mean('TARGET').alias('cliente_target_mean_30d'),
                                                   F.count('nro_documento_hash').alias('nro_contratos_30d'),
                                                   F.sum('trafico_app_total').alias('trafico_app_total_30d'),
                                                   F.sum('trafico_app_tasa').alias('trafico_app_tasa_30d'),
                                                   F.sum('mins_flujo_total').alias('mins_flujo_total_30d')
                                                  )
                                              .withColumn('NUMPERIODO', 
                                                    F.date_format(
                                                        F.add_months(
                                                                F.to_timestamp(F.col('NUMPERIODO').cast('string'), "yyyyMM")

                                                            ,1)
                                                       , "yyyyMM"))
                             )
                                      
    return (suscriptora_df_fe.join(
                                  documento_gb_df, on=['nro_documento_hash', 'NUMPERIODO'], how='left'
                                   )
                             .join(
                                  documento_gb_df_30d, on=['nro_documento_hash', 'NUMPERIODO'], how='left'
                                   )
                             .join(
                                  telefono_gb_df_30d, on=['nro_telefono_hash', 'NUMPERIODO'], how='left'
                                   )
                             .join(
                                  telefono_gb_df_60d, on=['nro_telefono_hash', 'NUMPERIODO'], how='left'
                                   )
                             .join(
                                  documento_gb_pivot_df, on=['nro_documento_hash', 'NUMPERIODO'], how='left'
                                   )
                             .join(
                                  documento_gb_pivot_df_30d, on=['nro_documento_hash', 'NUMPERIODO'], how='left'
                                   )
                             .withColumn('diff_nro_contratos_atual_30d', F.col('nro_contratos_atual')-
                                                                         F.col('nro_contratos_30d')
                                        )
                             .withColumn('diff_total_trafico_app_8_atual_30d', F.col('total_trafico_app_8_atual')-
                                                                         F.col('total_trafico_app_8_30d')
                                        )
                             .withColumn('diff_total_VCHPENALIDAD_atual_30d', F.col('total_VCHPENALIDAD_atual')-
                                                                         F.col('total_VCHPENALIDAD_30d')
                                        )
                             .withColumn('diff_VCHPENALIDAD_atual_30d', F.col('VCHPENALIDAD')-
                                                                         F.col('VCHPENALIDAD_30d')
                                        )
                             
           )