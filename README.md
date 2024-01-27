# entel-reto1-2022

Reto 1 del equipo smart predict conformado por:

- Diego Paolo Lozano Godos
- Martin Elias Yupanqui Andrade
- Diego Renato Risco Cosavalente

Para ejecutar el siguiente proyecto se requiere de algunas librerias extra que el python 3 require, encontrandose en el archivo `requirements.txt`.
A continuación la carpeta `src` contiene el código principal para obtener el book de variables así como la ejecución del modelo

En caso de querer ejecutar, 
colocar los nuevos datos en la carpeta `data\raw_data` con los mismos nombres anteriores e ejecutar en el archivo .ipynb en la carpeta `notebooks\01-abt-book`. Podemos observar el entrenamiento del modelo en el archivo (.ipynb) `notebooks\02-entrenamiento-modelo`, podiendo omitir esa parte y por ultimo el resultado del modelo `notebooks\03-prediction-save`. Cabe recalcar que para seguir el padrón de lo esperado por kaggle fue omitido por el momento la táctica para resolver el problema de negócios que sería apresentado al momento de sustentar.


Project Organization
------------
    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├── scores_data    <- Results from scoring model.
    │   ├── transformed_data <- The final, canonical data sets for modeling.
    │   └── raw_data       <- The original, immutable data dump.
    │
    ├── models             <- Trained and serialized models, model predictions, or model summaries
    │
    ├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering)
    │
    ├── pyproject.toml   <- dependences using poetry
    │
    ├── src                <- Source code for use in this project.
    │   ├── data_prep   <- Paste with the Scripts to prepare data
    │   │
    │   ├── ml          <- Pate with the principal function to load model 
    │   │                    
    │   └── main.py     <- Main Script to run the ETL
    │
    └──


--------
